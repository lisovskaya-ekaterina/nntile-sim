"""Compare residence across uses with earlier release and overlapped transfers."""
from itertools import product

from .global_scheduler_v6 import plan
from .global_scheduler_v7 import advance_writebacks
from .global_scheduler_v8 import lower_bound_us
from .global_scheduler_v9 import schedule as schedule_v9


def schedule(tasks, config):
    best = schedule_v9(tasks, config)
    bound = lower_bound_us(tasks, config)
    for demand, spill in product((True, False), (False, True)):
        if best.makespan_us <= bound + 1e-6:
            break
        # With demand ordering, establish the residency decisions at each
        # consumer. The packer still overlaps loads with earlier Compute.
        candidate = plan(tasks, config, skip_overwritten=True, demand_order=demand,
                         release_dead=True, spill_idle=spill)
        candidate = advance_writebacks(tasks, config, candidate)
        if candidate.makespan_us < best.makespan_us:
            best = candidate
    return best
