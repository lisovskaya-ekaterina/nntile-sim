"""Independent exhaustive event-state search for tiny fixed-order schedules."""
from fractions import Fraction
from heapq import heappop, heappush
from itertools import count
from .engine import order_tasks, SimulationError
from ..chunks import split_into_chunks


def exact_us(tasks, config, *, max_states=200000):
    """Return optimum as Fraction, or raise on budget exhaustion (no estimate)."""
    if config.chunk_allocation != 'fixed-slot':
        raise ValueError('Expected fixed-slot allocation')
    tasks = order_tasks(tasks, config.compute_order, config.custom_order)
    keys = {}
    chunks = []
    need, read, write = [], [], []
    overrides = dict(config.chunk_sizes_by_handle or ())
    for t in tasks:
        accesses = reads = writes = 0
        for a in t.accesses:
            if a.handle not in keys:
                width = overrides.get(a.handle, config.chunk_size_bytes or a.size_bytes)
                group = split_into_chunks(a.handle, a.size_bytes, width)
                keys[a.handle] = ((1 << len(group)) - 1) << len(chunks)
                chunks.extend(group)
            mask = keys[a.handle]
            accesses |= mask
            if a.reads_previous_value:
                reads |= mask
            if a.writes_value:
                writes |= mask
        need.append(accesses)
        read.append(reads)
        write.append(writes)
    if len(chunks) > 10 or len(tasks) > 12:
        raise ValueError('Exact search supports up to 10 chunks and 12 tasks')
    bits = [1 << k for k in range(len(chunks))]
    capacity = config.capacity_bytes
    sizes = [sum(c.reserved_bytes for b, c in zip(bits, chunks) if mask & b)
             for mask in range(1 << len(bits))]
    live = [0] * (len(tasks) + 1)
    for i in reversed(range(len(tasks))):
        live[i] = read[i] | (live[i+1] & ~write[i])
    duration = [Fraction(str(t.duration_us)) for t in tasks]
    loads = [Fraction(c.size_bytes) / Fraction(str(config.h2d_bytes_per_us)) for c in chunks]
    saves = [Fraction(c.size_bytes) / Fraction(str(config.d2h_bytes_per_us)) for c in chunks]
    # State at a decision point: next task, resident/dirty masks, remaining
    # Compute time, active H2D bit/time and active D2H bit/time.
    start = (0, 0, 0, 0, 0, 0, 0, 0)
    queue = [(0, 0, start)]
    distance = {start: 0}
    serial = count(1)

    def offer(state, time):
        if state not in distance or time < distance[state]:
            distance[state] = time
            heappush(queue, (time, next(serial), state))

    while queue:
        time, _, state = heappop(queue)
        if time != distance[state]:
            continue
        i, gpu, dirty, ct, h, ht, d, dt = state
        if i == len(tasks) and not (ct or ht or dt):
            return Fraction(time)
        if len(distance) > max_states:
            raise RuntimeError('Exact search state budget exhausted')
        pinned = need[i-1] if ct else 0
        locked = pinned | h | d
        if i < len(tasks) and not ct and not (read[i] & ~gpu or need[i] & locked):
            allocated = gpu | need[i]
            if sizes[allocated] <= capacity:
                after = dirty if duration[i] else dirty | write[i]
                offer((i+1, allocated, after, duration[i], h, ht, d, dt), time)
        for k, bit in enumerate(bits):
            if locked & bit:
                continue
            if gpu & bit:
                if not (dirty & live[i] & bit):
                    offer((i, gpu ^ bit, dirty & ~bit, ct, h, ht, d, dt), time)
                elif not d:
                    offer((i, gpu, dirty, ct, h, ht, bit, saves[k]), time)
            elif not h and live[i] & bit and sizes[gpu | bit] <= capacity:
                offer((i, gpu | bit, dirty, ct, bit, loads[k], d, dt), time)
        # Waiting for any active operation is always an option, even when
        # another lane is idle. Between completions releases cannot change.
        active = [v for v in (ct, ht, dt) if v]
        if active:
            step = min(active)
            if ct == step:
                dirty |= write[i-1]
            if dt == step:
                dirty &= ~d
            offer((i, gpu, dirty, max(0, ct-step), h if ht > step else 0,
                   max(0, ht-step), d if dt > step else 0, max(0, dt-step)), time+step)
    raise SimulationError('No feasible schedule')
