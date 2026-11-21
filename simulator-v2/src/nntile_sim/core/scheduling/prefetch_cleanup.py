"""Cancel unused prefetches before compacting the schedule."""
from collections import defaultdict
from dataclasses import replace

from .packing import retime
from .bounds import lower_bound_us, schedule as schedule_v8


def schedule(tasks, config):
    result = schedule_v8(tasks, config)
    if result.makespan_us <= lower_bound_us(tasks, config) + 1e-6:
        return result

    # Track LOADs in the current residency that no operation has consumed.
    by_job = {t.job_id: t for t in tasks}
    pending = defaultdict(dict)
    cancelled = set()
    for i, event in enumerate(result.events):
        if event.lane == 'Compute':
            for access in by_job[event.job_id].accesses:
                pending.pop(access.handle, None)
        elif event.handle is not None:
            handle = event.handle.rsplit(':', 1)[0]
            if event.action == 'LOAD':
                pending[handle][event.handle] = i
            else:
                load = pending[handle].pop(event.handle, None)
                if load is not None and event.action == 'DROP':
                    cancelled.update((load, i))

    if cancelled:
        remaining = [e for i, e in enumerate(result.events) if i not in cancelled]
        candidate = retime(tasks, config, replace(result, events=remaining))
        if candidate.makespan_us <= result.makespan_us:
            return candidate
    return result
