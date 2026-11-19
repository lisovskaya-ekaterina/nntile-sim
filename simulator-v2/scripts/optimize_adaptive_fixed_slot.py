"""Simulation-guided fixed-slot chunk search grouped by actual handle size."""
import argparse
import csv
import json
from collections import Counter, defaultdict
import time
from analyze_repeated_subgraphs import analyze, shared_handles
from math import ceil
from pathlib import Path
from nntile_sim.parser import parse_tasks
from nntile_sim.core.engine import Config, Simulator, SimulationError

COUNTS = (1, 2, 3, 4, 6, 8, 12, 16)


def handle_sizes(tasks):
    sizes = {}
    for task in tasks:
        for a in task.accesses:
            if a.size_bytes <= 0 or (a.handle in sizes and sizes[a.handle] != a.size_bytes):
                raise ValueError(f'Invalid size for {a.handle}')
            sizes[a.handle] = a.size_bytes
    if not sizes:
        raise ValueError('Empty trace')
    return sizes


def candidates(size, counts=COUNTS):
    if size <= 0 or not counts or any(type(n) is not int or n < 1 for n in counts):
        raise ValueError('Invalid candidates')
    return tuple(sorted({ceil(size / n) for n in (*counts, 1)}, reverse=True))


def evaluate(tasks, capacity, policy, bandwidth, plan):
    cfg = Config(capacity_bytes=capacity, h2d_bytes_per_us=bandwidth,
                 d2h_bytes_per_us=bandwidth, memory_policy=policy,
                 chunk_allocation='fixed-slot', prefetch_depth=0,
                 chunk_sizes_by_handle=tuple(sorted(plan.items())) if plan else None)
    try:
        r = Simulator(tasks, cfg).run()
    except SimulationError as exc:
        return {'status': 'infeasible', 'error': str(exc)}
    return {'status': 'ok', 'makespan_us': r.makespan_us, 'peak_bytes': r.peak_bytes,
            'h2d_bytes': r.h2d_bytes, 'd2h_bytes': r.d2h_bytes}


def score(r):
    if r['status'] != 'ok':
        return (float('inf'),) * 3
    return (r['makespan_us'], r['h2d_bytes'] + r['d2h_bytes'], r['peak_bytes'])


def optimize(tasks, capacity, policy='belady-demand', bandwidth=12000.,
             counts=COUNTS, max_evaluations=60, max_passes=2, on_evaluation=None, focus_handles=None):
    if capacity <= 0 or bandwidth <= 0 or max_evaluations < 1 or max_passes < 1:
        raise ValueError('Invalid search configuration')
    sizes = handle_sizes(tasks)
    groups = defaultdict(list)
    for h, s in sizes.items():
        if focus_handles is None or h in focus_handles:
            groups[s].append(h)
    frequency = Counter(a.size_bytes for task in tasks for a in task.accesses if a.handle in sizes and (focus_handles is None or a.handle in focus_handles))
    order = sorted(groups, key=lambda s: (-frequency[s], -s))
    layout = {s: s for s in groups}
    history, cache = [], {}

    def trial(config, change):
        key = tuple(sorted(config.items()))
        if key not in cache:
            if len(cache) >= max_evaluations:
                return None
            plan = {h: config[s] for s, hs in groups.items() for h in hs if config[s] != s}
            started = time.perf_counter()
            cache[key] = evaluate(tasks, capacity, policy, bandwidth, plan)
            row = {'change': change, 'runtime_s': round(time.perf_counter()-started, 4), **cache[key]}
            history.append(row)
            if on_evaluation is not None:
                on_evaluation(row, plan, len(cache))
        return cache[key]

    baseline = trial(layout, 'baseline')
    best = baseline
    for _ in range(max_passes):
        improved = False
        for size in order:
            chosen, local_best = None, best
            for chunk in candidates(size, counts):
                if chunk == layout[size]:
                    continue
                candidate_layout = dict(layout)
                candidate_layout[size] = chunk
                result = trial(candidate_layout, f'size={size};chunk={chunk}')
                if result is None:
                    break
                if score(result) < score(local_best):
                    chosen, local_best = candidate_layout, result
            if chosen is not None:
                layout, best, improved = chosen, local_best, True
            if len(cache) >= max_evaluations:
                break
        if not improved or len(cache) >= max_evaluations:
            break
    plan = {h: layout[s] for s, hs in groups.items() for h in hs if layout[s] != s}
    return {'baseline': baseline, 'best': best, 'plan': plan, 'history': history,
            'evaluations': len(cache), 'budget_exhausted': len(cache) >= max_evaluations,
            'changed_groups': sum(s != c for s, c in layout.items())}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('trace', type=Path)
    p.add_argument('--capacity-mib', type=int, nargs='+', required=True)
    p.add_argument('--policy', choices=('belady-demand', 'lru-demand'), default='belady-demand')
    p.add_argument('--bandwidth-gbps', type=float, default=12.)
    p.add_argument('--counts', type=int, nargs='+', default=list(COUNTS))
    p.add_argument('--max-evaluations', type=int, default=60)
    p.add_argument('--max-passes', type=int, default=2)
    p.add_argument('--output-dir', type=Path, required=True)
    p.add_argument('--focus', choices=('shared', 'all'), default='shared', help='Search only handles shared by multiple iteration groups by default')
    args = p.parse_args()
    if args.trace.name != 'tasks_mbs1.rec':
        p.error('This research package is scoped to tasks_mbs1.rec')
    if args.policy != 'belady-demand':
        p.error('This research package is scoped to Belady')
    tasks = parse_tasks(args.trace)
    analysis = analyze(tasks)
    if args.focus == 'shared' and len(analysis['groups']) < 2:
        p.error('Need at least two iteration groups for shared focus')
    focus = shared_handles(tasks) if args.focus == 'shared' else None
    analysis_dir = args.output_dir / args.trace.stem
    analysis_dir.mkdir(parents=True, exist_ok=True)
    (analysis_dir / 'subgraph_analysis.json').write_text(json.dumps(analysis, indent=2) + '\n')
    print(f'Iteration groups: {len(analysis["groups"])}; shared handles: {len(shared_handles(tasks))}', flush=True)
    for mib in args.capacity_mib:
        out = args.output_dir / args.trace.stem / f'{args.policy}_{mib}MiB'
        out.mkdir(parents=True, exist_ok=True)
        progress = out / 'search_history.csv'
        fields = ['change', 'status', 'runtime_s', 'makespan_us', 'peak_bytes', 'h2d_bytes', 'd2h_bytes', 'error']
        with progress.open('w', newline='') as f:
            csv.DictWriter(f, fieldnames=fields).writeheader()
        def checkpoint(row, plan, number):
            with progress.open('a', newline='') as f:
                w = csv.DictWriter(f, fieldnames=fields)
                w.writerow({k: row.get(k, '') for k in fields})
            (out / 'latest_plan.json').write_text(json.dumps(plan, indent=2) + '\n')
            duration = row['runtime_s']
            makespan = row.get('makespan_us')
            print(f'[{mib} MiB {number}/{args.max_evaluations}] {row["change"]} '
                  f'{row["status"]} runtime={duration:.2f}s makespan_us={makespan}', flush=True)
        result = optimize(tasks, mib * 1024**2, args.policy, args.bandwidth_gbps * 1000,
                          tuple(args.counts), args.max_evaluations, args.max_passes,
                          on_evaluation=checkpoint, focus_handles=focus)
        summary = {k: v for k, v in result.items() if k not in ('history', 'plan')}
        summary.update({'trace': str(args.trace), 'capacity_mib': mib,
                        'allocation': 'fixed-slot', 'policy': args.policy,
                        'changed_handles': len(result['plan']), 'focus': args.focus})
        (out / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
        (out / 'chunk_plan.json').write_text(json.dumps(result['plan'], indent=2) + '\n')
        print(f'{args.trace.name} {mib}MiB: baseline={result["baseline"]["status"]}, '
              f'best={result["best"]["status"]}, evaluations={result["evaluations"]}, '
              f'changed_handles={len(result["plan"])} -> {out}')


if __name__ == '__main__':
    main()
