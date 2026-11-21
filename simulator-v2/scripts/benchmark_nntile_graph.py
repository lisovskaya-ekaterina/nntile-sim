"""Compare demand-only LRU, demand-only Belady, and chunked Global Planner.

Example:
 python simulator-v2/scripts/benchmark_nntile_graph.py examples/tasks_mbs1.rec \
   --capacities-mib 768 1024 1536 --h2d-gbps 12 --d2h-gbps 12 \
   --chunk-mib 1 --output simulator-v2/results/global_comparison/benchmark.csv

Times are microseconds; GB/s is decimal GB/s (1000 bytes/microsecond).
"""
from __future__ import annotations

import argparse
import csv
import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))

from nntile_sim.parser import parse_tasks
from nntile_sim.core.engine import Config, Simulator, SimulationError
from nntile_sim.core.global_planner import schedule as global_schedule

MIB = 1024 ** 2
POLICIES = ('lru-demand', 'belady-demand', 'global-planner')
FIELDS = (
    'trace', 'tasks', 'capacity_mib', 'policy', 'chunk_mib', 'status',
    'makespan_us', 'speedup_vs_lru', 'compute_idle_us', 'compute_utilization_pct',
    'h2d_busy_us', 'h2d_utilization_pct', 'd2h_busy_us', 'd2h_utilization_pct',
    'h2d_bytes', 'd2h_bytes', 'peak_bytes', 'completed_jobs', 'error',
)
EVENT_FIELDS = (
    'trace', 'capacity_mib', 'policy', 'lane', 'action', 'handle', 'job_id',
    'start_us', 'end_us', 'used_bytes_after',
)


def minimum_task_footprint(tasks):
    return max((sum({a.handle: a.size_bytes for a in task.accesses}.values())
                for task in tasks), default=0)


def interval_union_us(events, lane):
    intervals = sorted((e.start_us, e.end_us) for e in events
                       if e.lane.lower() == lane.lower() and e.end_us > e.start_us)
    total = 0.0
    start = end = None
    for left, right in intervals:
        if start is None:
            start, end = left, right
        elif left <= end:
            end = max(end, right)
        else:
            total += end - start
            start, end = left, right
    if start is not None:
        total += end - start
    return total


def benchmark(tasks, trace_name, capacities_mib, h2d_gbps, d2h_gbps,
              chunk_mib, compute_order):
    rows, event_rows = [], []
    minimum = minimum_task_footprint(tasks)
    chunk_bytes = round(chunk_mib * MIB)
    for capacity_mib in capacities_mib:
        capacity_bytes = round(capacity_mib * MIB)
        lru_makespan = None
        for policy in POLICIES:
            row = dict.fromkeys(FIELDS, '')
            row.update(trace=trace_name, tasks=len(tasks), capacity_mib=capacity_mib,
                       policy=policy, chunk_mib=chunk_mib if policy == 'global-planner' else '')
            if capacity_bytes < minimum:
                row.update(status='skipped', error='Capacity below maximum task footprint')
                rows.append(row)
                continue
            try:
                cfg = Config(
                    capacity_bytes=capacity_bytes,
                    h2d_bytes_per_us=h2d_gbps * 1000.0,
                    d2h_bytes_per_us=d2h_gbps * 1000.0,
                    compute_order=compute_order,
                    memory_policy=policy if policy != 'global-planner' else 'lru-demand',
                    chunk_size_bytes=chunk_bytes if policy == 'global-planner' else None,
                    prefetch_depth=0,
                )
                result = (global_schedule(tasks, cfg) if policy == 'global-planner'
                          else Simulator(tasks, cfg).run())
                if result.completed_jobs != len(tasks):
                    raise SimulationError('Incomplete task execution')
                if result.peak_bytes > capacity_bytes:
                    raise SimulationError('GPU capacity exceeded')
                if not math.isfinite(result.makespan_us) or result.makespan_us <= 0:
                    raise SimulationError('Invalid makespan')
                busy = {lane: interval_union_us(result.events, lane)
                        for lane in ('H2D', 'D2H', 'Compute')}
                if busy['Compute'] > result.makespan_us + 1e-5:
                    raise SimulationError('Compute busy time exceeds makespan')
                row.update(
                    status='ok', makespan_us=result.makespan_us,
                    compute_idle_us=max(0.0, result.makespan_us - busy['Compute']),
                    compute_utilization_pct=100 * busy['Compute'] / result.makespan_us,
                    h2d_busy_us=busy['H2D'],
                    h2d_utilization_pct=100 * busy['H2D'] / result.makespan_us,
                    d2h_busy_us=busy['D2H'],
                    d2h_utilization_pct=100 * busy['D2H'] / result.makespan_us,
                    h2d_bytes=result.h2d_bytes, d2h_bytes=result.d2h_bytes,
                    peak_bytes=result.peak_bytes, completed_jobs=result.completed_jobs,
                )
                if policy == 'lru-demand':
                    lru_makespan = result.makespan_us
                if lru_makespan is not None:
                    row['speedup_vs_lru'] = lru_makespan / result.makespan_us
                for event in result.events:
                    event_rows.append(dict(
                        trace=trace_name, capacity_mib=capacity_mib, policy=policy,
                        lane=event.lane, action=event.action,
                        handle=event.handle or '', job_id=event.job_id or '',
                        start_us=event.start_us, end_us=event.end_us,
                        used_bytes_after=event.used_bytes_after,
                    ))
            except (SimulationError, ValueError, AssertionError, RuntimeError) as exc:
                row.update(status='error', error=f'{type(exc).__name__}: {exc}')
            rows.append(row)
    return rows, event_rows


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('trace', type=Path)
    parser.add_argument('--capacities-mib', nargs='+', type=float, required=True)
    parser.add_argument('--h2d-gbps', type=float, required=True)
    parser.add_argument('--d2h-gbps', type=float, required=True)
    parser.add_argument('--chunk-mib', type=float, default=1.0,
                        help='Global Planner chunk size (MiB); default 1')
    parser.add_argument('--compute-order', choices=('topological', 'trace'),
                        default='topological')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    values = (*args.capacities_mib, args.h2d_gbps, args.d2h_gbps, args.chunk_mib)
    if any(not math.isfinite(v) or v <= 0 for v in values):
        parser.error('Capacities, bandwidths, and chunk size must be positive finite numbers')
    if round(args.chunk_mib * MIB) < 1:
        parser.error('Chunk size must be at least one byte')
    try:
        tasks = parse_tasks(args.trace)
    except (OSError, ValueError) as exc:
        parser.error(str(exc))
    if not tasks:
        parser.error('Trace contains no completed compute tasks')
    rows, event_rows = benchmark(tasks, args.trace.name, args.capacities_mib,
                                 args.h2d_gbps, args.d2h_gbps,
                                 args.chunk_mib, args.compute_order)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    events_path = args.output.with_name(args.output.stem + '_events.csv')
    for path, fields, data in ((args.output, FIELDS, rows),
                               (events_path, EVENT_FIELDS, event_rows)):
        with path.open('w', newline='', encoding='utf-8') as stream:
            writer = csv.DictWriter(stream, fieldnames=fields)
            writer.writeheader()
            writer.writerows(data)
    print(f'Trace: {args.trace} | tasks: {len(tasks)} | '
          f'max task footprint: {minimum_task_footprint(tasks) / MIB:.3f} MiB')
    for row in rows:
        value = (f"{float(row['makespan_us']) / 1e6:.6f} s"
                 if row['status'] == 'ok' else '-')
        print(f"{row['capacity_mib']:g} MiB | {row['policy']:14s} | "
              f"{row['status']:7s} | makespan={value} | {row['error']}")
    print(f'Metrics: {args.output}\nEvents:  {events_path}')
    return 1 if any(row['status'] == 'error' for row in rows) else 0


if __name__ == '__main__':
    raise SystemExit(main())
