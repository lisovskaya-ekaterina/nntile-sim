"""Bound, compare feasible plans, and explain immediate blockers at Compute starts."""
from collections import defaultdict
from itertools import product

from .engine import order_tasks
from .global_scheduler_v6 import plan
from .global_scheduler_v7 import advance_writebacks, schedule as schedule_v7


def lower_bound_us(tasks, config):
    ordered = order_tasks(tasks, config.compute_order, config.custom_order)
    seen = set()
    suffix = 0.0
    suffixes = []
    for task in reversed(ordered):
        suffix += task.duration_us
        suffixes.append(suffix)
    bound = suffix
    volume = 0
    for task, remaining in zip(ordered, reversed(suffixes)):
        incoming = {a.handle: a.size_bytes for a in task.accesses
                    if a.reads_previous_value and a.handle not in seen}
        volume += sum(incoming.values())
        seen.update(a.handle for a in task.accesses)
        bound = max(bound, remaining + volume / config.h2d_bytes_per_us)
    return bound


def schedule(tasks, config):
    bound = lower_bound_us(tasks, config)
    best = schedule_v7(tasks, config)
    for clean, reverse in product((False, True), repeat=2):
        if best.makespan_us <= bound + 1e-6:
            break
        candidate = plan(tasks, config, prefer_clean=clean, reverse_loads=reverse,
                     skip_overwritten=True)
        candidate = advance_writebacks(tasks, config, candidate)
        if candidate.makespan_us < best.makespan_us:
            best = candidate
    return best


def compute_gaps(tasks, result):
    by_job = {t.job_id: t for t in tasks}
    # Index actual completion times. ALLOC is instantaneous, LOAD and UNLOAD
    # become usable at completion. Look up only chunks of the waiting job.
    completions = defaultdict(list)
    for e in result.events:
        if e.action in ('ALLOC', 'LOAD', 'UNLOAD'):
            completions[e.handle.rsplit(':', 1)[0], e.end_us].append((e.action, e.handle))
    previous = 0.0
    for e in result.events:
        if e.lane == 'Compute':
            if e.start_us > previous + 1e-6:
                handles = {a.handle for a in by_job[e.job_id].accesses}
                blockers = [item for h in sorted(handles)
                            for item in completions.get((h, e.start_us), ())]
                yield e.job_id, e.start_us - previous, blockers
            previous = e.end_us
