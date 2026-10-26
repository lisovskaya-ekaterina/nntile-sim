"""Conservative, correctness-first three-lane simulator (times in microseconds).

Baseline policy: prepare the next compute task, transfer one missing input at a
 time, evict clean/dirty unpinned objects on demand. H2D, D2H and Compute have
 independent busy intervals. No speculative prefetch yet.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from collections import defaultdict
from heapq import heappush, heappop
from typing import Sequence
from ..model import Task

class SimulationError(RuntimeError):
    pass

@dataclass(frozen=True)
class Config:
    capacity_bytes: int
    h2d_bytes_per_us: float
    d2h_bytes_per_us: float
    compute_order: str = 'topological'
    custom_order: tuple[str, ...] | None = None

    def __post_init__(self):
        if self.capacity_bytes <= 0 or self.h2d_bytes_per_us <= 0 or self.d2h_bytes_per_us <= 0:
            raise ValueError('Capacity and bandwidths must be positive')

@dataclass(frozen=True)
class Event:
    lane: str
    action: str
    handle: str | None
    job_id: str | None
    start_us: float
    end_us: float
    used_bytes_after: int

@dataclass
class SimulationResult:
    events: list[Event]
    makespan_us: float
    peak_bytes: int
    h2d_bytes: int
    d2h_bytes: int
    completed_jobs: int


def order_tasks(tasks: Sequence[Task], mode: str = 'topological', custom: Sequence[str] | None = None) -> list[Task]:
    by_id = {t.job_id: t for t in tasks}
    if len(by_id) != len(tasks):
        raise SimulationError('Duplicate JobId')
    if mode == 'trace':
        result = sorted(tasks, key=lambda t: (t.start_time, t.job_id))
    elif mode == 'custom':
        if custom is None or len(custom) != len(tasks) or set(custom) != set(by_id) or len(set(custom)) != len(custom):
            raise SimulationError('Custom order must contain each JobId exactly once')
        result = [by_id[j] for j in custom]
    elif mode == 'topological':
        # Dependencies not represented in the completed task set are external.
        indeg = {t.job_id: 0 for t in tasks}
        children = defaultdict(list)
        for t in tasks:
            for dep in set(t.depends_on):
                if dep in by_id:
                    indeg[t.job_id] += 1
                    children[dep].append(t.job_id)
        ready = []
        for t in tasks:
            if indeg[t.job_id] == 0:
                heappush(ready, (t.start_time, t.job_id))
        result = []
        while ready:
            _, jid = heappop(ready)
            result.append(by_id[jid])
            for child in children[jid]:
                indeg[child] -= 1
                if indeg[child] == 0:
                    heappush(ready, (by_id[child].start_time, child))
        if len(result) != len(tasks):
            raise SimulationError('Dependency cycle detected')
    else:
        raise ValueError(f'Unknown compute order: {mode}')
    seen = set()
    for t in result:
        if any(dep in by_id and dep not in seen for dep in t.depends_on):
            raise SimulationError(f'Order violates dependency of {t.job_id}')
        seen.add(t.job_id)
    return result

class Simulator:
    def __init__(self, tasks: Sequence[Task], config: Config):
        self.tasks = order_tasks(tasks, config.compute_order, config.custom_order)
        self.config = config

    def run(self) -> SimulationResult:
        tasks, cfg = self.tasks, self.config
        sizes: dict[str, int] = {}
        first_read: dict[str, bool] = {}
        remaining = defaultdict(int)
        for task in tasks:
            for a in task.accesses:
                if a.handle in sizes and sizes[a.handle] != a.size_bytes:
                    raise SimulationError(f'Inconsistent size for {a.handle}')
                sizes[a.handle] = a.size_bytes
                first_read.setdefault(a.handle, a.reads_previous_value)
                remaining[a.handle] += 1
        # First read implies initial CPU data; first W implies uninitialized output.
        cpu = {h for h, reads in first_read.items() if reads}
        gpu: set[str] = set()
        dirty: set[str] = set()
        reserved: set[str] = set()  # in-flight H2D allocations
        unloading: set[str] = set()
        used = peak = h2d_bytes = d2h_bytes = 0
        now = 0.0
        seq = 0
        pending = []  # (finish, sequence, lane, handle, job)
        busy: set[str] = set()
        events: list[Event] = []
        running: Task | None = None
        next_idx = 0
        finished = 0
        pinned: set[str] = set()

        def schedule(lane: str, action: str, h: str | None, job: str | None, duration: float):
            nonlocal seq
            if lane in busy:
                raise SimulationError(f'{lane} double booked')
            busy.add(lane)
            seq += 1
            heappush(pending, (now + duration, seq, lane, action, h, job, now))

        while finished < len(tasks):
            progressed = False
            if running is None and next_idx < len(tasks):
                task = tasks[next_idx]
                required = {a.handle for a in task.accesses}
                if sum(sizes[h] for h in required) > cfg.capacity_bytes:
                    raise SimulationError(f'Task {task.job_id} requires more GPU memory than capacity')
                # A write-only output requires allocation but no CPU transfer.
                missing = [a for a in task.accesses if a.handle not in gpu and a.handle not in reserved]
                for a in missing:
                    if a.handle in unloading:
                        continue
                    size = sizes[a.handle]
                    if used + size > cfg.capacity_bytes:
                        continue
                    if a.reads_previous_value or a.handle in cpu:
                        if 'H2D' not in busy and a.handle in cpu:
                            used += size
                            peak = max(peak, used)
                            reserved.add(a.handle)
                            h2d_bytes += size
                            schedule('H2D', 'LOAD', a.handle, None, size / cfg.h2d_bytes_per_us)
                            progressed = True
                    else:
                        gpu.add(a.handle)
                        used += size
                        peak = max(peak, used)
                        progressed = True
                # One eviction at a time; avoid evicting inputs for current task.
                if any(a.handle not in gpu and a.handle not in reserved for a in task.accesses):
                    if 'D2H' not in busy:
                        candidates = sorted((h for h in gpu if h not in required and h not in pinned and h not in unloading),
                                            key=lambda h: (h in dirty, -sizes[h]))
                        if candidates:
                            h = candidates[0]
                            if h in dirty and remaining[h] > 0:
                                unloading.add(h)
                                d2h_bytes += sizes[h]
                                schedule('D2H', 'UNLOAD', h, None, sizes[h] / cfg.d2h_bytes_per_us)
                            else:
                                gpu.remove(h)
                                dirty.discard(h)
                                used -= sizes[h]
                                events.append(Event('Memory', 'DROP', h, None, now, now, used))
                            progressed = True
                if all(h in gpu for h in required) and 'Compute' not in busy:
                    pinned = required
                    running = task
                    schedule('Compute', task.name, None, task.job_id, task.duration_us)
                    next_idx += 1
                    progressed = True
            if pending:
                end, _, lane, action, h, job, start = heappop(pending)
                now = end
                busy.remove(lane)
                if lane == 'H2D':
                    reserved.remove(h)
                    gpu.add(h)
                elif lane == 'D2H':
                    unloading.remove(h)
                    dirty.remove(h)
                    cpu.add(h)
                    gpu.remove(h)
                    used -= sizes[h]
                elif lane == 'Compute':
                    assert running is not None and running.job_id == job
                    for a in running.accesses:
                        remaining[a.handle] -= 1
                        if a.writes_value:
                            dirty.add(a.handle)
                            cpu.discard(a.handle)  # CPU version is now stale
                    running = None
                    pinned = set()
                    finished += 1
                events.append(Event(lane, action, h, job, start, end, used))
            elif not progressed:
                raise SimulationError(f'Deadlock before task {tasks[next_idx].job_id if next_idx < len(tasks) else "end"}: '
                                      f'used={used}, capacity={cfg.capacity_bytes}')
        # Drain transfers started concurrently with final compute.
        while pending:
            end, _, lane, action, h, job, start = heappop(pending)
            now = end
            if lane == 'H2D':
                reserved.remove(h)
                gpu.add(h)
            elif lane == 'D2H':
                unloading.remove(h)
                dirty.remove(h)
                cpu.add(h)
                gpu.remove(h)
                used -= sizes[h]
            events.append(Event(lane, action, h, job, start, end, used))
        return SimulationResult(events, now, peak, h2d_bytes, d2h_bytes, finished)
