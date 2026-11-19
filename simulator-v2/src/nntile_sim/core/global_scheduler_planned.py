"""Offline chunk scheduler with safe one-task lookahead H2D prefetch.

Compute is sequential in the selected order. H2D may overlap Compute when
there is spare GPU capacity; eviction/writeback remain demand-driven.
"""
from __future__ import annotations

from collections import defaultdict, deque
from bisect import bisect_right
from math import inf
from typing import Sequence

from .engine import Config, Event, SimulationError, SimulationResult, order_tasks
from .global_scheduler import schedule as reference_schedule
from ..chunks import Chunk, split_into_chunks
from ..model import Task


def schedule(tasks: Sequence[Task], config: Config) -> SimulationResult:
    ordered = order_tasks(tasks, config.compute_order, config.custom_order)
    if config.chunk_allocation != 'fixed-slot':
        raise SimulationError('Global scheduler v2 supports fixed-slot allocation only')

    sizes: dict[str, int] = {}
    first_read: dict[str, bool] = {}
    for task in ordered:
        for access in task.accesses:
            if access.size_bytes <= 0:
                raise SimulationError(f'Invalid size for {access.handle}')
            if access.handle in sizes and sizes[access.handle] != access.size_bytes:
                raise SimulationError(f'Inconsistent size for {access.handle}')
            sizes[access.handle] = access.size_bytes
            first_read.setdefault(access.handle, access.reads_previous_value)

    overrides = dict(config.chunk_sizes_by_handle or ())
    unknown = set(overrides) - set(sizes)
    if unknown:
        raise SimulationError(f'Unknown chunk handles: {sorted(unknown)}')
    chunks_by_handle: dict[str, tuple[Chunk, ...]] = {
        handle: split_into_chunks(handle, size, overrides.get(handle, config.chunk_size_bytes or size))
        for handle, size in sizes.items()
    }
    chunks = {chunk.key: chunk for group in chunks_by_handle.values() for chunk in group}
    task_keys = [
        {chunk.key for access in task.accesses for chunk in chunks_by_handle[access.handle]}
        for task in ordered
    ]
    for task, keys in zip(ordered, task_keys):
        if sum(chunks[key].reserved_bytes for key in keys) > config.capacity_bytes:
            raise SimulationError(f'Task {task.job_id} requires more GPU memory than capacity')

    # Pass 1: obtain future eviction decisions from the reference.
    reference = reference_schedule(tasks, config)
    reference_computes = sorted(
        (e for e in reference.events if e.lane == 'Compute'),
        key=lambda e: e.start_us,
    )
    reference_starts = [e.start_us for e in reference_computes]

    # For each chunk, record the Compute index preceding its D2H.
    planned_evictions = defaultdict(list)
    for e in reference.events:
        if e.lane != 'D2H':
            continue
        handle, chunk_index = e.handle.rsplit(':', 1)
        key = (handle, int(chunk_index))
        position = bisect_right(reference_starts, e.start_us) - 1
        planned_evictions[key].append(position)

    for positions in planned_evictions.values():
        positions.sort()

    # Track writes so that an early D2H never saves a version
    # which the reference plan would overwrite before eviction.
    future_writes = defaultdict(list)
    for index, task in enumerate(ordered):
        for access in task.accesses:
            if access.writes_value:
                for chunk in chunks_by_handle[access.handle]:
                    future_writes[chunk.key].append(index)

    future_uses: dict[tuple[str, int], deque[int]] = defaultdict(deque)
    for index, keys in enumerate(task_keys):
        for key in keys:
            future_uses[key].append(index)

    cpu = {chunk.key for handle, group in chunks_by_handle.items()
           if first_read[handle] for chunk in group}
    gpu: set[tuple[str, int]] = set()
    dirty: set[tuple[str, int]] = set()
    # Chunks being prefetched reserve GPU capacity immediately; they become
    # readable only after their transfer completion.
    ready_at: dict[tuple[str, int], float] = {}
    now = 0.0
    h2d_free = 0.0
    d2h_free = 0.0
    used = peak = h2d_bytes = d2h_bytes = completed = 0
    events: list[Event] = []

    def name(key: tuple[str, int]) -> str:
        return f'{key[0]}:{key[1]}'

    def next_use(key: tuple[str, int]) -> float:
        uses = future_uses[key]
        return uses[0] if uses else inf

    def reserve(key: tuple[str, int]) -> None:
        nonlocal used, peak
        gpu.add(key)
        used += chunks[key].reserved_bytes
        peak = max(peak, used)

    def drop(key: tuple[str, int], when: float) -> None:
        nonlocal used
        gpu.remove(key)
        ready_at.pop(key, None)
        dirty.discard(key)
        used -= chunks[key].reserved_bytes
        events.append(Event('Memory', 'DROP', name(key), None, when, when, used))

    def load(key: tuple[str, int], earliest: float) -> float:
        nonlocal h2d_free, h2d_bytes
        if key not in cpu:
            raise SimulationError(f'No valid CPU version for {name(key)}')
        reserve(key)
        start = max(earliest, h2d_free)
        end = start + chunks[key].size_bytes / config.h2d_bytes_per_us
        h2d_free = end
        ready_at[key] = end
        h2d_bytes += chunks[key].size_bytes
        events.append(Event('H2D', 'LOAD', name(key), None, start, end, used))
        return end

    for i, task in enumerate(ordered):
        needed = task_keys[i]
        for key in needed:
            assert future_uses[key][0] == i
            future_uses[key].popleft()

        # Never evict a chunk whose H2D has not completed.
        now = max(now, *(ready_at[key] for key in needed & gpu)) if needed & gpu else now

        for key in sorted(gpu - needed):
            if next_use(key) == inf and ready_at.get(key, 0.0) <= now:
                drop(key, now)

        access_by_handle = {access.handle: access for access in task.accesses}
        for key in sorted(needed - gpu):
            chunk = chunks[key]
            while used + chunk.reserved_bytes > config.capacity_bytes:
                candidates = {k for k in gpu - needed if ready_at.get(k, 0.0) <= now}
                if not candidates:
                    # Wait for an in-flight prefetch before reclaiming memory.
                    pending = [ready_at[k] for k in gpu - needed if ready_at.get(k, 0.0) > now]
                    if pending:
                        now = min(pending)
                        continue
                    raise SimulationError(f'Cannot reserve {name(key)} for task {task.job_id}')
                victim = max(candidates, key=lambda k: (next_use(k), k not in dirty, k))
                if victim in dirty and next_use(victim) != inf:
                    start = max(now, d2h_free)
                    now = start + chunks[victim].size_bytes / config.d2h_bytes_per_us
                    d2h_free = now
                    d2h_bytes += chunks[victim].size_bytes
                    cpu.add(victim)
                    events.append(Event('D2H', 'UNLOAD', name(victim), None, start, now, used))
                drop(victim, now)

            if access_by_handle[key[0]].reads_previous_value:
                load(key, now)
            else:
                reserve(key)
                ready_at[key] = now
                events.append(Event('Memory', 'ALLOC', name(key), None, now, now, used))

        # Compute starts only when all its chunks are resident and ready.
        compute_start = max(now, *(ready_at[key] for key in needed)) if needed else now
        compute_end = compute_start + task.duration_us
        events.append(Event('Compute', task.name, None, task.job_id,
                            compute_start, compute_end, used))
        completed += 1

        # Experimental early D2H: save dirty resident chunks while
        # Compute runs, without evicting them.
        # Only transfers that finish inside this Compute are scheduled.
        # Current-task chunks are excluded to avoid Compute/D2H races.
        cursor = max(compute_start, d2h_free)
        def eviction_deadline(key):
            positions = planned_evictions[key]
            pos = bisect_right(positions, i)
            if pos >= len(positions):
                return inf
            deadline = positions[pos]

            # Do not save a version that will be overwritten
            # before its planned eviction.
            writes = future_writes[key]
            wpos = bisect_right(writes, i)
            if wpos < len(writes) and writes[wpos] <= deadline:
                return inf
            return deadline

        candidates = sorted(
            (
                key for key in gpu - needed
                if key in dirty
                and ready_at.get(key, 0.0) <= cursor
                and eviction_deadline(key) != inf
            ),
            key=lambda key: (eviction_deadline(key), key),
        )

        for key in candidates:
            start = max(cursor, ready_at.get(key, 0.0))
            end = start + chunks[key].size_bytes / config.d2h_bytes_per_us
            if end > compute_end:
                continue

            events.append(Event(
                'D2H', 'UNLOAD', name(key), None,
                start, end, used,
            ))
            d2h_bytes += chunks[key].size_bytes
            d2h_free = end
            cursor = end

            # The saved CPU version is valid after this D2H.
            # Since end <= compute_end and the current Compute does not
            # access this chunk, it is safe to update planner state here.
            cpu.add(key)
            dirty.remove(key)

        # Next-task lookahead: prefetch only CPU-valid inputs, only into free
        # capacity, and never evict any chunk while Compute is running.
        if i + 1 < len(ordered):
            next_task = ordered[i + 1]
            next_access = {a.handle: a for a in next_task.accesses}
            for key in sorted(task_keys[i + 1] - gpu):
                if not next_access[key[0]].reads_previous_value or key not in cpu:
                    continue
                if used + chunks[key].reserved_bytes > config.capacity_bytes:
                    continue
                # Prefetch is useful only if it can start before Compute ends.
                if max(compute_start, h2d_free) >= compute_end:
                    break
                load(key, compute_start)

        now = compute_end
        for access in task.accesses:
            if access.writes_value:
                for chunk in chunks_by_handle[access.handle]:
                    dirty.add(chunk.key)
                    cpu.discard(chunk.key)
                    ready_at[chunk.key] = compute_end

    makespan = max([now, h2d_free] + [event.end_us for event in events])
    return SimulationResult(events=events, makespan_us=makespan,
                            peak_bytes=peak, h2d_bytes=h2d_bytes,
                            d2h_bytes=d2h_bytes, completed_jobs=completed)
