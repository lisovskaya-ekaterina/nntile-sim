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
    memory_policy: str = 'lru-demand'

    def __post_init__(self):
        if self.memory_policy not in ('lru-demand', 'legacy-baseline'):
            raise ValueError('Unknown memory policy')
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
        # Stable lexicographical topological ordering using networkx.
        import networkx as nx
        graph = nx.DiGraph()
        graph.add_nodes_from(by_id)
        graph.add_edges_from((dep, t.job_id) for t in tasks
                             for dep in set(t.depends_on) if dep in by_id)
        try:
            ids = nx.lexicographical_topological_sort(
                graph, key=lambda jid: (by_id[jid].start_time, jid))
            result = [by_id[jid] for jid in ids]
        except nx.NetworkXUnfeasible as exc:
            raise SimulationError('Dependency cycle detected') from exc
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
        if self.config.memory_policy == 'lru-demand':
            return self._run_lru_demand()
        return self._run_legacy()

    def _run_lru_demand(self) -> SimulationResult:
        """Sequential demand-only execution with LRU eviction; no future-task transfers.

        A write-only access can allocate/overwrite without fetching an old version.
        Clean copies can be dropped, dirty live copies require writeback.
        """
        tasks, cfg = self.tasks, self.config
        sizes = {}
        remaining = defaultdict(int)
        for task in tasks:
            for a in task.accesses:
                if a.size_bytes <= 0:
                    raise SimulationError(f'Invalid size for {a.handle}')
                if a.handle in sizes and sizes[a.handle] != a.size_bytes:
                    raise SimulationError(f'Inconsistent size for {a.handle}')
                sizes[a.handle] = a.size_bytes
                remaining[a.handle] += 1
        # Inputs whose first use reads are initially CPU-resident.
        first = {}
        for task in tasks:
            for a in task.accesses:
                first.setdefault(a.handle, a.reads_previous_value)
        cpu = {h for h, reads in first.items() if reads}
        gpu, dirty = set(), set()
        last_touch = {}  # logical LRU recency, updated on Compute access
        clock = 0
        now = 0.0
        used = peak = h2d_bytes = d2h_bytes = 0
        events = []

        def emit(lane, action, handle, job, duration):
            nonlocal now
            start = now
            now += duration
            events.append(Event(lane, action, handle, job, start, now, used))

        for task in tasks:
            accesses = {a.handle: a for a in task.accesses}
            required = set(accesses)
            required_size = sum(sizes[h] for h in required)
            if required_size > cfg.capacity_bytes:
                raise SimulationError(f'Task {task.job_id} requires more GPU memory than capacity')
            # All handles of this task are protected during preparation.
            for h, a in accesses.items():
                if h in gpu:
                    continue
                size = sizes[h]
                while used + size > cfg.capacity_bytes:
                    candidates = gpu - required
                    if not candidates:
                        raise SimulationError(f'No evictable data before task {task.job_id}')
                    victim = min(candidates, key=lambda x: (last_touch.get(x, -1), x))
                    if victim in dirty and remaining[victim] > 0:
                        d2h_bytes += sizes[victim]
                        # D2H completion releases GPU allocation.
                        # Transfer completes before the allocation is released.
                        start = now
                        now += sizes[victim] / cfg.d2h_bytes_per_us
                        cpu.add(victim)
                        gpu.remove(victim)
                        dirty.discard(victim)
                        used -= sizes[victim]
                        events.append(Event('D2H', 'UNLOAD', victim, None, start, now, used))
                    else:
                        gpu.remove(victim)
                        dirty.discard(victim)
                        used -= sizes[victim]
                    if victim not in cpu or remaining[victim] == 0:
                        # A dead dirty value may be dropped without writeback.
                        pass
                    events.append(Event('Memory', 'DROP', victim, None, now, now, used))
                used += size
                peak = max(peak, used)
                gpu.add(h)
                if a.reads_previous_value:
                    if h not in cpu:
                        raise SimulationError(f'No valid version of {h} before task {task.job_id}')
                    h2d_bytes += size
                    emit('H2D', 'LOAD', h, None, size / cfg.h2d_bytes_per_us)
                else:
                    # W-only allocation; no H2D.
                    events.append(Event('Memory', 'ALLOC', h, None, now, now, used))
            emit('Compute', task.name, None, task.job_id, task.duration_us)
            for h, a in accesses.items():
                clock += 1
                last_touch[h] = clock
                remaining[h] -= 1
                if a.writes_value:
                    dirty.add(h)
                    cpu.discard(h)
        return SimulationResult(events, now, peak, h2d_bytes, d2h_bytes, len(tasks))

    def _run_legacy(self) -> SimulationResult:
        tasks, cfg = self.tasks, self.config
        sizes = {}
        first_read = {}
        remaining = defaultdict(int)
        for task in tasks:
            for a in task.accesses:
                if a.size_bytes <= 0:
                    raise SimulationError(f'Invalid size for {a.handle}')
                if a.handle in sizes and sizes[a.handle] != a.size_bytes:
                    raise SimulationError(f'Inconsistent size for {a.handle}')
                sizes[a.handle] = a.size_bytes
                first_read.setdefault(a.handle, a.reads_previous_value)
                remaining[a.handle] += 1
        cpu = {h for h, reads in first_read.items() if reads}
        gpu, dirty, reserved, unloading = set(), set(), set(), set()
        used = peak = h2d_bytes = d2h_bytes = 0
        now = 0.0
        seq = 0
        pending = []
        busy = set()
        events = []
        running = None
        next_idx = finished = 0
        pinned = set()

        def schedule(lane, action, h, job, duration):
            nonlocal seq
            if lane in busy:
                raise SimulationError(f'{lane} double booked')
            if duration < 0:
                raise SimulationError('Negative duration')
            busy.add(lane)
            seq += 1
            heappush(pending, (now + duration, seq, lane, action, h, job, now))

        def prepare(task):
            nonlocal used, peak, h2d_bytes, d2h_bytes
            required = {a.handle for a in task.accesses}
            if sum(sizes[h] for h in required) > cfg.capacity_bytes:
                raise SimulationError(f'Task {task.job_id} requires more GPU memory than capacity')
            # Reserve all data for the current compute and its immediate successor.
            protected = required | pinned | reserved | unloading
            progress = False
            for a in task.accesses:
                h = a.handle
                if h in gpu or h in reserved or h in unloading:
                    continue
                size = sizes[h]
                if used + size > cfg.capacity_bytes:
                    continue
                if h in cpu:
                    if 'H2D' in busy:
                        continue
                    used += size
                    peak = max(peak, used)
                    reserved.add(h)
                    h2d_bytes += size
                    schedule('H2D', 'LOAD', h, None, size / cfg.h2d_bytes_per_us)
                    progress = True
                elif first_read[h] and h not in gpu:
                    raise SimulationError(f'No valid version of {h}')
                else:
                    # An output-only handle can be allocated without H2D.
                    used += size
                    peak = max(peak, used)
                    gpu.add(h)
                    progress = True
            missing = any(h not in gpu and h not in reserved for h in required)
            if missing and 'D2H' not in busy:
                candidates = sorted((h for h in gpu if h not in protected),
                                    key=lambda h: (h in dirty, -sizes[h], h))
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
                    progress = True
            return progress

        while finished < len(tasks) or pending:
            # Dispatch at the current timestamp before advancing virtual time.
            while True:
                progressed = False
                if next_idx < len(tasks):
                    task = tasks[next_idx]
                    progressed |= prepare(task)
                    required = {a.handle for a in task.accesses}
                    if running is None and not (required & (reserved | unloading)) and required <= gpu:
                        pinned = required
                        running = task
                        schedule('Compute', task.name, None, task.job_id, task.duration_us)
                        next_idx += 1
                        progressed = True
                if not progressed:
                    break
            if not pending:
                if finished < len(tasks):
                    raise SimulationError(f'Deadlock before task {tasks[next_idx].job_id if next_idx < len(tasks) else "end"}: '
                                          f'used={used}, capacity={cfg.capacity_bytes}')
                break
            end = pending[0][0]
            now = end
            # Process all completions at the same timestamp before redispatching.
            while pending and pending[0][0] == end:
                _, _, lane, action, h, job, start = heappop(pending)
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
                            cpu.discard(a.handle)
                    running = None
                    pinned = set()
                    finished += 1
                events.append(Event(lane, action, h, job, start, end, used))
        return SimulationResult(events, now, peak, h2d_bytes, d2h_bytes, finished)
