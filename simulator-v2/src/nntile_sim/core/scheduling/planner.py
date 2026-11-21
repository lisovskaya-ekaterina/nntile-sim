"""Compare full schedules when choosing which reservation to release first."""
from itertools import product

from .residency import plan
from .bounds import lower_bound_us
from .optimization import early_writes, schedule as schedule_v11


def schedule(tasks, config):
    best = schedule_v11(tasks, config)
    bound = lower_bound_us(tasks, config)
    for prefetch, spill in product((True, False), repeat=2):
        if best.makespan_us <= bound + 1e-6:
            break
        candidate = plan(tasks, config, demand_order=True, release_dead=True,
                         skip_overwritten=True, fastest_release=True,
                         prefetch_next=prefetch, spill_idle=spill)
        for result in (candidate, early_writes(tasks, config, candidate)):
            if result.makespan_us < best.makespan_us:
                best = result
    return best
