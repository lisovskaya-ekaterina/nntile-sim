"""Offline earliest-start scheduling for the residency decisions selected by V3."""
from collections import defaultdict
from dataclasses import replace
from heapq import heappop, heappush

from ...chunks import split_into_chunks
from ..engine import SimulationResult
from .baseline import schedule as schedule_v3


def schedule(tasks, config):
    return retime(tasks, config, schedule_v3(tasks, config))


def retime(tasks, config, baseline):
    """Pack an existing residency plan into the actual GPU capacity."""
    return pack(tasks, config, sorted(baseline.events, key=event_order))


def event_order(event):
    priority = {"DROP": 0, "LOAD": 1, "ALLOC": 1}
    return event.start_us, priority.get(event.action, 2)


def pack(tasks, config, operations):
    """Place a dependency-ordered stream on lanes and account for reserved bytes."""
    overrides = dict(config.chunk_sizes_by_handle or ())
    sizes = {a.handle: a.size_bytes for t in tasks for a in t.accesses}
    keys = defaultdict(list)
    reserved = {}
    payload = {}
    for handle, size in sizes.items():
        width = overrides.get(handle, config.chunk_size_bytes or size)
        for chunk in split_into_chunks(handle, size, width):
            key = f'{handle}:{chunk.index}'
            keys[handle].append(key)
            reserved[key] = chunk.reserved_bytes
            payload[key] = chunk.size_bytes

    by_job = {t.job_id: t for t in tasks}
    chunk_free = defaultdict(float)
    lane_free = defaultdict(float)
    # Every byte can be reassigned only after its previous residency ends.
    memory = [(0.0, config.capacity_bytes)]
    shifted = []
    for event in operations:
        if event.lane == 'Compute':
            touched = [key for a in by_job[event.job_id].accesses for key in keys[a.handle]]
        else:
            touched = [event.handle]
        start = max((chunk_free[key] for key in touched), default=0.0)
        if event.lane != 'Memory':
            start = max(start, lane_free[event.lane])
        if event.action in ('LOAD', 'ALLOC'):
            needed = reserved[event.handle]
            while needed:
                available_at, amount = heappop(memory)
                start = max(start, available_at)
                take = min(amount, needed)
                needed -= take
                if amount > take:
                    heappush(memory, (available_at, amount - take))
        elif event.action == 'DROP':
            heappush(memory, (start, reserved[event.handle]))
        end = start + (event.end_us - event.start_us)
        for key in touched:
            chunk_free[key] = end
        if event.lane != 'Memory':
            lane_free[event.lane] = end
        shifted.append(replace(event, start_us=start, end_us=end))

    used = peak = 0
    events = []
    for event in sorted(shifted, key=event_order):
        if event.action in ('ALLOC', 'LOAD'):
            used += reserved[event.handle]
        elif event.action == 'DROP':
            used -= reserved[event.handle]
        peak = max(peak, used)
        events.append(replace(event, used_bytes_after=used))
    h2d = sum(payload[e.handle] for e in events if e.lane == "H2D")
    d2h = sum(payload[e.handle] for e in events if e.lane == "D2H")
    return SimulationResult(events, max(lane_free.values(), default=0.0),
                            peak, h2d, d2h, len(tasks))
