"""Plan residency at transfer deadlines on the ideal fixed-order Compute timeline."""
from bisect import bisect_right
from collections import defaultdict
from math import inf

from ..chunks import split_into_chunks
from .engine import Event, SimulationError, order_tasks
from .global_scheduler_v4 import pack


def schedule(tasks, config):
    return plan(tasks, config)


def plan(tasks, config, *, prefer_clean=False, reverse_loads=False, skip_overwritten=False,
         demand_order=False, release_dead=False, spill_idle=False,
         large_first=False, split_after=None, prefetch_next=False, fastest_release=False):
    if config.chunk_allocation != "fixed-slot":
        raise SimulationError("V6 requires fixed-slot allocation")
    tasks = order_tasks(tasks, config.compute_order, config.custom_order)
    chunks = {}
    overrides = dict(config.chunk_sizes_by_handle or ())
    handle_sizes = {}
    occurrences = defaultdict(list)
    accesses = []
    starts = []
    clock = 0.0
    for i, task in enumerate(tasks):
        modes = {}
        for access in task.accesses:
            h = access.handle
            if handle_sizes.setdefault(h, access.size_bytes) != access.size_bytes:
                raise SimulationError(f"Inconsistent size for {h}")
            if h not in chunks:
                width = overrides.get(h, config.chunk_size_bytes or access.size_bytes)
                chunks[h] = split_into_chunks(h, access.size_bytes, width)
            for chunk in chunks[h]:
                key = chunk.key
                if key not in modes:
                    occurrences[key].append(i)
                reads, writes = modes.get(key, (False, False))
                modes[key] = reads or access.reads_previous_value, writes or access.writes_value
        starts.append(clock)
        clock += task.duration_us
        accesses.append(modes)

    if overrides.keys() - chunks.keys():
        raise SimulationError("Chunk size override for unknown handle")
    info = {c.key: c for group in chunks.values() for c in group}
    # Budget the serial H2D lane backwards from each consumer. A request
    # for a cached chunk will be cancelled during the residency pass.
    requests = []
    lane_start = inf
    for i in reversed(range(len(tasks))):
        requests.append((starts[i], 1, i, None))
        if demand_order:
            continue
        inputs = list(accesses[i].items())
        if large_first:
            inputs.sort(key=lambda item: -info[item[0]].size_bytes)
        if not reverse_loads:
            inputs.reverse()
        for key, (reads, _) in inputs:
            if reads:
                lane_start = min(lane_start, starts[i]) - info[key].size_bytes / config.h2d_bytes_per_us
                requests.append((lane_start, 0, i, key))

    gpu = set()
    dirty = set()
    last_end = defaultdict(float)
    next_index = {k: uses[0] for k, uses in occurrences.items()}
    operations = []
    used = 0

    def emit(key, lane, action, duration=0.0):
        operations.append(Event(lane, action, f'{key[0]}:{key[1]}', None, 0.0, duration, 0))

    def needs_writeback(key):
        nxt = next_index[key]
        return key in dirty and nxt != inf and (not skip_overwritten or accesses[nxt][key][0])

    def evict(key):
        nonlocal used
        if needs_writeback(key):
            emit(key, 'D2H', 'UNLOAD', info[key].size_bytes / config.d2h_bytes_per_us)
        emit(key, 'Memory', 'DROP')
        gpu.remove(key)
        dirty.discard(key)
        used -= info[key].reserved_bytes

    def acquire(key, reads, deadline, protected):
        nonlocal used
        if key in gpu:
            return
        while used + info[key].reserved_bytes > config.capacity_bytes:
            def rank(k):
                writeback = needs_writeback(k)
                ready = last_end[k] + (info[k].size_bytes / config.d2h_bytes_per_us if writeback else 0)
                if fastest_release:
                    deficit = used + info[key].reserved_bytes - config.capacity_bytes
                    nxt = next_index[k]
                    return_cost = (info[k].size_bytes / config.h2d_bytes_per_us
                                   if nxt != inf and accesses[nxt][k][0] else 0.0)
                    slack = starts[nxt] - deadline - return_cost if nxt != inf else inf
                    return info[k].reserved_bytes >= deficit, -ready, slack, nxt, k
                if prefer_clean:
                    return ready <= deadline, not writeback, next_index[k], k
                return ready <= deadline, next_index[k], not writeback, k

            victim = max((k for k in gpu if k not in protected), key=rank, default=None)
            if victim is None:
                raise SimulationError("Task working set exceeds capacity")
            evict(victim)
        if reads:
            emit(key, 'H2D', 'LOAD', info[key].size_bytes / config.h2d_bytes_per_us)
        else:
            emit(key, 'Memory', 'ALLOC')
        gpu.add(key)
        used += info[key].reserved_bytes

    for deadline, _, i, requested in sorted(requests):
        if requested is not None:
            # An intervening writer must produce the version first.
            if next_index[requested] == i:
                acquire(requested, True, deadline, ())
            continue
        modes = accesses[i]
        protected = modes
        if prefetch_next and i + 1 < len(tasks):
            # Give a future input the first available bytes if it can
            # coexist with the entire current task, including its outputs.
            protected = set(modes)
            free = config.capacity_bytes - sum(info[k].reserved_bytes for k in modes)
            for key, (reads, _) in accesses[i+1].items():
                if reads and key not in protected and info[key].reserved_bytes <= free:
                    acquire(key, True, deadline, protected)
                    protected.add(key)
                    free -= info[key].reserved_bytes
        admissions = modes.items()
        if demand_order:
            admissions = sorted(admissions, key=lambda item: (not item[1][0],
                                -info[item[0]].size_bytes if large_first else 0))
        for key, (reads, _) in admissions:
            acquire(key, reads, deadline, protected)
        task = tasks[i]
        operations.append(Event('Compute', task.name, None, task.job_id, 0, task.duration_us, 0))
        pressure = False
        if fastest_release and spill_idle and i + 1 < len(tasks):
            missing = accesses[i+1].keys() - gpu
            pressure = used + sum(info[k].reserved_bytes for k in missing) > config.capacity_bytes
        finished = modes.items()
        if fastest_release and spill_idle:
            finished = sorted(finished, key=lambda pair: info[pair[0]].size_bytes)
        for key, (_, writes) in finished:
            if writes:
                dirty.add(key)
            last_end[key] = deadline + task.duration_us
            uses = occurrences[key]
            pos = bisect_right(uses, i)
            next_index[key] = uses[pos] if pos < len(uses) else inf
            if release_dead:
                nxt = next_index[key]
                dead = nxt == inf or not accesses[nxt][key][0]
                idle = spill_idle and nxt > i + 1 and (not fastest_release or pressure)
                if dead or idle or split_after == (task.job_id, key[0]):
                    evict(key)
    return pack(tasks, config, operations)
