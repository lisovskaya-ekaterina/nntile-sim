"""Benchmark LRU/Belady demand and one-task prefetch on NNTile .rec traces.

Run: python simulator-v2/scripts/benchmark_nntile_graph.py TRACE.rec \
  --capacities-mib 1024 1536 2048 --h2d-gbps 12 --d2h-gbps 12 \
  --output simulator-v2/results/milestone6_3_1/benchmark.csv
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

MIB = 1024 ** 2
FIELDS = (
    'trace', 'tasks', 'capacity_mib', 'policy', 'prefetch_depth', 'status',
    'makespan_us', 'speedup_vs_demand', 'h2d_bytes', 'd2h_bytes',
    'peak_bytes', 'error',
)


def minimum_task_footprint(tasks):
    return max((sum({a.handle: a.size_bytes for a in task.accesses}.values())
                for task in tasks), default=0)


def benchmark(tasks, trace_name, capacities_mib, h2d_gbps, d2h_gbps):
    rows = []
    minimum = minimum_task_footprint(tasks)
    for capacity_mib in capacities_mib:
        capacity_bytes = int(capacity_mib * MIB)
        for policy in ('lru-demand', 'belady-demand'):
            demand = None
            for depth in (0, 1):
                row = dict.fromkeys(FIELDS, '')
                row.update(trace=trace_name, tasks=len(tasks), capacity_mib=capacity_mib,
                           policy=policy, prefetch_depth=depth)
                if capacity_bytes < minimum:
                    row.update(status='skipped', error='Capacity below maximum task footprint')
                else:
                    try:
                        config = Config(capacity_bytes=capacity_bytes,
                                        h2d_bytes_per_us=h2d_gbps * 1000.0,
                                        d2h_bytes_per_us=d2h_gbps * 1000.0,
                                        memory_policy=policy, prefetch_depth=depth)
                        result = Simulator(tasks, config).run()
                        if result.completed_jobs != len(tasks):
                            raise SimulationError('Incomplete task execution')
                        if result.peak_bytes > capacity_bytes:
                            raise SimulationError('GPU capacity exceeded')
                        row.update(status='ok', makespan_us=result.makespan_us,
                                   h2d_bytes=result.h2d_bytes,
                                   d2h_bytes=result.d2h_bytes, peak_bytes=result.peak_bytes)
                        if depth == 0:
                            demand = result
                            row['speedup_vs_demand'] = 1.0
                        elif demand is not None:
                            row['speedup_vs_demand'] = (demand.makespan_us / result.makespan_us
                                                         if result.makespan_us > 0 else '')
                    except (SimulationError, ValueError, AssertionError) as exc:
                        row.update(status='error', error=f'{type(exc).__name__}: {exc}')
                rows.append(row)
    return rows


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('trace', type=Path, help='NNTile/StarPU .rec trace')
    parser.add_argument('--capacities-mib', nargs='+', type=float, required=True)
    parser.add_argument('--h2d-gbps', type=float, required=True)
    parser.add_argument('--d2h-gbps', type=float, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    if (any(not math.isfinite(v) or v <= 0 for v in args.capacities_mib)
            or any(not math.isfinite(v) or v <= 0
                   for v in (args.h2d_gbps, args.d2h_gbps))):
        parser.error('Capacities and bandwidths must be positive finite numbers')
    try:
        tasks = parse_tasks(args.trace)
    except (OSError, ValueError) as exc:
        parser.error(str(exc))
    if not tasks:
        parser.error('Trace contains no completed compute tasks')
    rows = benchmark(tasks, args.trace.name, args.capacities_mib,
                     args.h2d_gbps, args.d2h_gbps)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('w', newline='', encoding='utf-8') as stream:
        writer = csv.DictWriter(stream, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    print(f'Trace: {args.trace} | tasks: {len(tasks)} | '
          f'max task footprint: {minimum_task_footprint(tasks) / MIB:.3f} MiB')
    for row in rows:
        print(f"{row['capacity_mib']:g} MiB | {row['policy']:13s} | "
              f"depth={row['prefetch_depth']} | {row['status']:7s} | "
              f"makespan={row['makespan_us']} us | speedup={row['speedup_vs_demand']} "
              f"{row['error']}")
    print(f'CSV: {args.output}')
    return 1 if any(row['status'] == 'error' for row in rows) else 0


if __name__ == '__main__':
    raise SystemExit(main())
