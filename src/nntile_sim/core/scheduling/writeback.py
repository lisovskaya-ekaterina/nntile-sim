"""Advance scheduled writebacks within the lifetime of the version they save."""
from bisect import bisect_right
from collections import defaultdict
from dataclasses import replace
from operator import itemgetter

from .packing import retime
from .residency import schedule as schedule_v6


def reserve_window(holes, blocked, release, deadline, duration):
    """Reserve a contiguous D2H lane interval disjoint from Compute on this handle."""
    first = bisect_right(holes, release, key=itemgetter(1))
    for j in range(first, len(holes)):
        left, right = holes[j]
        if left >= deadline:
            break
        start = max(release, left)
        limit = min(right, deadline)
        k = bisect_right(blocked, start, key=itemgetter(1))
        while start + duration <= limit:
            finish = start + duration
            if k == len(blocked) or finish <= blocked[k][0]:
                holes[j:j + 1] = [(a, b) for a, b in ((left, start), (finish, right)) if a < b]
                return start
            start = blocked[k][1]
            k += 1
    return None


def advance_writebacks(tasks, config, baseline):
    """Reserve idle D2H intervals; DROP remains ordered after intervening reads."""
    by_job = {task.job_id: task for task in tasks}
    accesses = defaultdict(list)
    holes = []
    previous_end = 0.0
    for event in baseline.events:
        if event.lane == 'D2H':
            if previous_end < event.start_us:
                holes.append((previous_end, event.start_us))
            previous_end = event.end_us
        elif event.lane == 'Compute':
            for handle in {a.handle for a in by_job[event.job_id].accesses}:
                accesses[handle].append((event.start_us, event.end_us))

    write_end = defaultdict(float)
    resident_since = {}
    events = []
    moved = False
    for event in baseline.events:
        if event.lane == 'Compute':
            for access in by_job[event.job_id].accesses:
                if access.writes_value:
                    write_end[access.handle] = event.end_us
        elif event.action in ('ALLOC', 'LOAD'):
            resident_since[event.handle] = event.end_us
        elif event.action == 'DROP':
            del resident_since[event.handle]
        elif event.lane == 'D2H':
            handle = event.handle.rsplit(':', 1)[0]
            release = max(resident_since[event.handle], write_end[handle])
            duration = event.end_us - event.start_us
            blocked = accesses[handle]
            start = reserve_window(holes, blocked, release, event.start_us, duration)
            if start is not None:
                event = replace(event, start_us=start, end_us=start + duration)
                moved = True
        events.append(event)
    if not moved:
        return baseline
    candidate = retime(tasks, config, replace(baseline, events=events))
    return min((baseline, candidate), key=lambda result: result.makespan_us)


def schedule(tasks, config):
    baseline = schedule_v6(tasks, config)
    if baseline.makespan_us <= sum(task.duration_us for task in tasks) + 1e-6:
        return baseline
    return advance_writebacks(tasks, config, baseline)
