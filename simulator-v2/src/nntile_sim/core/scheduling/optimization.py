"""Reschedule critical transfers, including writebacks that can delay a reader."""
from collections import defaultdict
from itertools import product

from .packing import pack
from .residency import plan
from .bounds import lower_bound_us
from .residency_search import schedule as schedule_v10


def early_writes(tasks, cfg, result):
    """Reorder D2H after its last writer/reservation, then rebuild all lane times."""
    jobs = {t.job_id: t for t in tasks}
    writer = {}
    reservation = {}
    moved = set()
    insert = defaultdict(list)
    for i, event in enumerate(result.events):
        if event.lane == 'Compute':
            for access in jobs[event.job_id].accesses:
                if access.writes_value:
                    writer[access.handle] = i
        elif event.action in ('LOAD', 'ALLOC'):
            reservation[event.handle] = i
        elif event.action == 'UNLOAD':
            h = event.handle.rsplit(':', 1)[0]
            anchor = max(writer.get(h, -1), reservation[event.handle])
            insert[anchor].append(event)
            moved.add(i)
    if not moved:
        return result
    stream = []
    for i, event in enumerate(result.events):
        if i not in moved:
            stream.append(event)
        stream.extend(insert.get(i, ()))
    return pack(tasks, cfg, stream)


def schedule(tasks, config):
    best = schedule_v10(tasks, config)
    bound = lower_bound_us(tasks, config)
    if best.makespan_us <= bound + 1e-6:
        return best

    def consider(candidate):
        nonlocal best
        for result in (candidate, early_writes(tasks, config, candidate)):
            if result.makespan_us < best.makespan_us:
                best = result

    consider(best)
    consider(plan(tasks, config, skip_overwritten=True, demand_order=True,
                  release_dead=True, prefetch_next=True))
    for demand, spill, large in product((True, False), (False, True), (True, False)):
        if best.makespan_us <= bound + 1e-6:
            return best
        consider(plan(tasks, config, skip_overwritten=True, demand_order=demand,
                      release_dead=True, spill_idle=spill, large_first=large))

    # Try individual residency splits near the longest waits. Bounds limit
    # the number of full replans independently of the number of graph nodes.
    jobs = {t.job_id: t for t in tasks}
    compute = [e for e in best.events if e.lane == 'Compute']
    gaps = [(compute[i].start_us - compute[i-1].end_us, i)
            for i in range(1, len(compute))]
    splits = []
    for gap, i in sorted(gaps, reverse=True):
        if gap <= 1e-6 or len(splits) >= 24:
            break
        for event in reversed(compute[max(0, i-3):i]):
            for access in jobs[event.job_id].accesses:
                split = (event.job_id, access.handle)
                if split not in splits and len(splits) < 24:
                    splits.append(split)
    for split in splits:
        if best.makespan_us <= bound + 1e-6:
            break
        consider(plan(tasks, config, skip_overwritten=True, demand_order=True,
                      release_dead=True, large_first=True, split_after=split))
    return best
