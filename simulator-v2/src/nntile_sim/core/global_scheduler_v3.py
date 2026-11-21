"""V3: independent event-based GPU memory scheduler with early D2H."""

from collections import defaultdict, deque
from math import inf
from typing import Sequence

from .engine import (
    Config,
    Event,
    SimulationError,
    SimulationResult,
    order_tasks,
)
from ..chunks import split_into_chunks
from ..model import Task


def schedule(tasks: Sequence[Task], config: Config) -> SimulationResult:
    if config.chunk_allocation != "fixed-slot":
        raise SimulationError("V3 requires fixed-slot allocation")

    ordered = order_tasks(
        tasks, config.compute_order, config.custom_order
    )

    sizes = {}
    first_read = {}

    for task in ordered:
        for access in task.accesses:
            if access.size_bytes <= 0:
                raise SimulationError("Invalid chunk size")

            if (
                access.handle in sizes
                and sizes[access.handle] != access.size_bytes
            ):
                raise SimulationError("Inconsistent handle size")

            sizes[access.handle] = access.size_bytes
            first_read.setdefault(
                access.handle, access.reads_previous_value
            )

    overrides = dict(config.chunk_sizes_by_handle or ())
    unknown = set(overrides) - set(sizes)
    if unknown:
        raise SimulationError(
            f"Unknown chunk handles: {sorted(unknown)}"
        )

    chunks_by_handle = {
        handle: split_into_chunks(
            handle,
            size,
            overrides.get(
                handle, config.chunk_size_bytes or size
            ),
        )
        for handle, size in sizes.items()
    }

    chunks = {
        chunk.key: chunk
        for group in chunks_by_handle.values()
        for chunk in group
    }

    required = [
        {
            chunk.key
            for access in task.accesses
            for chunk in chunks_by_handle[access.handle]
        }
        for task in ordered
    ]

    for task, keys in zip(ordered, required):
        memory = sum(chunks[k].reserved_bytes for k in keys)
        if memory > config.capacity_bytes:
            raise SimulationError(
                f"Task {task.job_id} exceeds GPU capacity"
            )

    future = defaultdict(deque)

    for i, keys in enumerate(required):
        for key in keys:
            future[key].append(i)

    cpu = {
        chunk.key
        for handle, group in chunks_by_handle.items()
        if first_read[handle]
        for chunk in group
    }

    gpu = set()
    dirty = set()
    ready_at = {}

    events = []

    now = 0.0
    h2d_free = 0.0
    d2h_free = 0.0

    used = 0
    peak = 0
    h2d_bytes = 0
    d2h_bytes = 0

    def label(key):
        return f"{key[0]}:{key[1]}"

    def next_use(key):
        return future[key][0] if future[key] else inf

    def reserve(key):
        nonlocal used, peak

        if key in gpu:
            raise SimulationError("Double GPU reservation")

        gpu.add(key)
        used += chunks[key].reserved_bytes
        peak = max(peak, used)

        if used > config.capacity_bytes:
            raise SimulationError("GPU capacity exceeded")

    def drop(key, when):
        nonlocal used

        if key not in gpu:
            raise SimulationError("DROP of nonresident chunk")

        if key in dirty and key not in cpu:
            if next_use(key) != inf:
                raise SimulationError(
                    f"Unsafe DROP of dirty chunk {label(key)}"
                )

        gpu.remove(key)
        dirty.discard(key)
        ready_at.pop(key, None)

        used -= chunks[key].reserved_bytes

        events.append(
            Event(
                "Memory", "DROP", label(key), None,
                when, when, used
            )
        )

    def load(key, earliest):
        nonlocal h2d_free, h2d_bytes

        if key not in cpu:
            raise SimulationError(
                f"No valid CPU version: {label(key)}"
            )

        reserve(key)

        start = max(earliest, h2d_free)
        end = (
            start
            + chunks[key].size_bytes
            / config.h2d_bytes_per_us
        )

        h2d_free = end
        ready_at[key] = end
        h2d_bytes += chunks[key].size_bytes

        events.append(
            Event(
                "H2D", "LOAD", label(key), None,
                start, end, used
            )
        )

    def writeback(key, earliest):
        nonlocal d2h_free, d2h_bytes

        start = max(
            earliest,
            d2h_free,
            ready_at.get(key, 0.0),
        )

        end = (
            start
            + chunks[key].size_bytes
            / config.d2h_bytes_per_us
        )

        d2h_free = end
        d2h_bytes += chunks[key].size_bytes

        events.append(
            Event(
                "D2H", "UNLOAD", label(key), None,
                start, end, used
            )
        )

        cpu.add(key)
        dirty.discard(key)

        return end

    def select_victim(needed, current):
        candidates = [
            key for key in gpu - needed
            if ready_at.get(key, 0.0) <= current
        ]

        if not candidates:
            return None

        return max(
            candidates,
            key=lambda key: (
                next_use(key),
                key not in dirty,
                key,
            ),
        )

    for i, task in enumerate(ordered):
        needed = required[i]

        for key in needed:
            assert future[key][0] == i
            future[key].popleft()

        if needed & gpu:
            now = max(
                now,
                *(ready_at[key] for key in needed & gpu),
            )

        for key in sorted(gpu - needed):
            if (
                next_use(key) == inf
                and ready_at.get(key, 0.0) <= now
            ):
                drop(key, now)

        accesses = {
            access.handle: access
            for access in task.accesses
        }

        for key in sorted(needed - gpu):
            while (
                used + chunks[key].reserved_bytes
                > config.capacity_bytes
            ):
                victim = select_victim(needed, now)

                if victim is None:
                    pending = [
                        ready_at[k]
                        for k in gpu - needed
                        if ready_at.get(k, 0.0) > now
                    ]

                    if pending:
                        now = min(pending)
                        continue

                    raise SimulationError(
                        f"Cannot allocate {label(key)}"
                    )

                if (
                    victim in dirty
                    and next_use(victim) != inf
                ):
                    now = writeback(victim, now)

                drop(victim, now)

            if accesses[key[0]].reads_previous_value:
                load(key, now)
            else:
                reserve(key)
                ready_at[key] = now

                events.append(
                    Event(
                        "Memory", "ALLOC", label(key),
                        None, now, now, used
                    )
                )

        compute_start = max(
            [now] + [ready_at[key] for key in needed]
        )

        compute_end = compute_start + task.duration_us

        events.append(
            Event(
                "Compute",
                task.name,
                None,
                task.job_id,
                compute_start,
                compute_end,
                used,
            )
        )

        # Prefetch for the next Compute.
        if i + 1 < len(ordered):
            next_accesses = {
                access.handle: access
                for access in ordered[i + 1].accesses
            }

            for key in sorted(required[i + 1] - gpu):
                access = next_accesses[key[0]]

                if not access.reads_previous_value:
                    continue

                if key not in cpu:
                    continue

                if (
                    used + chunks[key].reserved_bytes
                    > config.capacity_bytes
                ):
                    continue

                if max(compute_start, h2d_free) >= compute_end:
                    break

                load(key, compute_start)

        # Early D2H: anticipate memory pressure
        # before the next Compute.
        if i + 1 < len(ordered):
            next_needed = required[i + 1]

            dead = {
                key for key in gpu - next_needed
                if next_use(key) == inf
            }

            missing = next_needed - gpu

            required_bytes = sum(
                chunks[key].reserved_bytes
                for key in missing
            )

            available_bytes = (
                config.capacity_bytes
                - used
                + sum(chunks[key].reserved_bytes for key in dead)
            )

            deficit = max(0, required_bytes - available_bytes)

            victims = sorted(
                (
                    key for key in gpu - next_needed - dead
                ),
                key=lambda key: (
                    next_use(key),
                    key not in dirty,
                    key,
                ),
                reverse=True,
            )

            for victim in victims:
                if deficit <= 0:
                    break

                deficit -= chunks[victim].reserved_bytes

                if victim not in dirty:
                    continue

                if victim in needed:
                    continue

                start = max(
                    compute_start,
                    d2h_free,
                    ready_at.get(victim, 0.0),
                )

                duration = (
                    chunks[victim].size_bytes
                    / config.d2h_bytes_per_us
                )

                if start + duration > compute_end + 1e-9:
                    continue

                writeback(victim, start)

        now = compute_end

        # New versions appear after Compute.
        for access in task.accesses:
            if access.writes_value:
                for chunk in chunks_by_handle[access.handle]:
                    dirty.add(chunk.key)
                    cpu.discard(chunk.key)
                    ready_at[chunk.key] = compute_end

    makespan = max(
        [now, h2d_free, d2h_free]
        + [event.end_us for event in events]
    )

    return SimulationResult(
        events=events,
        makespan_us=makespan,
        peak_bytes=peak,
        h2d_bytes=h2d_bytes,
        d2h_bytes=d2h_bytes,
        completed_jobs=len(ordered),
    )
