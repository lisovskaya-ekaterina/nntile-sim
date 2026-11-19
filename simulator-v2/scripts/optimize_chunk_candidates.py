"""Graph-agnostic, analytical candidate generator for per-handle chunk layouts.

This is NOT a makespan optimizer: event-driven validation is a later milestone.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
from collections import defaultdict
from pathlib import Path

DEFAULT_COUNTS = (1, 2, 3, 4, 6, 8, 12, 16, 24, 32)


def candidate(size: int, target_count: int, allocation: str) -> dict:
    if size <= 0 or target_count <= 0:
        raise ValueError('Sizes and target chunk counts must be positive')
    if allocation not in ('fixed-slot', 'exact-size'):
        raise ValueError('Unknown allocation mode')
    chunk_size = (size + target_count - 1) // target_count
    count = (size + chunk_size - 1) // chunk_size
    reserved = count * chunk_size if allocation == 'fixed-slot' else size
    return dict(chunk_size_bytes=chunk_size, chunks=count,
                reserved_bytes=reserved, waste_bytes=reserved-size)


def load_handles(path: Path) -> list[tuple[str, int, int]]:
    with path.open(newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        if not {'handle', 'size_bytes', 'accesses'} <= set(reader.fieldnames or []):
            raise ValueError(f'{path}: expected handle,size_bytes,accesses')
        rows = [(r['handle'], int(r['size_bytes']), int(r['accesses'])) for r in reader]
    if not rows or any(not h or s <= 0 or a <= 0 for h, s, a in rows):
        raise ValueError(f'{path}: empty or invalid handle records')
    if len(set(h for h, _, _ in rows)) != len(rows):
        raise ValueError(f'{path}: duplicate handles')
    return rows


def options_for_size(size: int, allocation: str, counts: tuple[int, ...]):
    by_layout = {}
    for n in counts:
        c = candidate(size, n, allocation)
        # Deduplicate equivalent layouts, including tiny handles.
        by_layout[(c['chunk_size_bytes'], c['chunks'])] = c
    return sorted(by_layout.values(), key=lambda c: (c['chunks'], c['waste_bytes']))


def optimize(rows, allocation='fixed-slot', counts=DEFAULT_COUNTS,
             max_waste_fraction=0.05, max_chunks_per_handle=32):
    """Choose fewest chunks subject to per-handle fixed-slot waste budget.

    The budget is waste / reserved, not waste / payload. One-chunk layouts
    are always feasible in exact-size. No arbitrary KiB size boundaries.
    """
    if not 0 <= max_waste_fraction < 1 or max_chunks_per_handle < 1:
        raise ValueError('Invalid waste budget or chunk cap')
    if not counts or any(n <= 0 for n in counts):
        raise ValueError('Chunk-count candidates must be positive')
    sizes = sorted({s for _, s, _ in rows})
    chosen = {}
    all_options = {}
    for size in sizes:
        opts = options_for_size(size, allocation, tuple(sorted(set(counts) | {1})))
        all_options[size] = opts
        feasible = [c for c in opts if c['chunks'] <= max_chunks_per_handle
                    and c['waste_bytes'] / c['reserved_bytes'] <= max_waste_fraction]
        # If no feasible candidate exists, keep best achievable waste and
        # report that the requested constraint was not met.
        pool = feasible or [c for c in opts if c['chunks'] <= max_chunks_per_handle]
        if not pool:
            raise ValueError('No layouts under chunk cap')
        chosen[size] = min(pool, key=lambda c: (c['chunks'] if feasible else
                        c['waste_bytes'] / c['reserved_bytes'],
                        c['waste_bytes'] if feasible else c['chunks']))
    results = []
    for handle, size, accesses in rows:
        c = chosen[size]
        results.append(dict(handle=handle, size_bytes=size, accesses=accesses,
                            **c, meets_waste_budget=(c['waste_bytes'] / c['reserved_bytes'] <= max_waste_fraction)))
    return results, all_options


def summary(results):
    payload = sum(r['size_bytes'] for r in results)
    reserved = sum(r['reserved_bytes'] for r in results)
    return dict(handles=len(results), unique_sizes=len({r['size_bytes'] for r in results}),
                payload_bytes=payload, reserved_bytes=reserved,
                waste_bytes=reserved-payload,
                waste_fraction=(reserved-payload)/reserved,
                total_chunks=sum(r['chunks'] for r in results),
                access_weighted_chunks=sum(r['chunks']*r['accesses'] for r in results),
                budget_violations=sum(not r['meets_waste_budget'] for r in results))


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('handles_csv', nargs='+', type=Path)
    p.add_argument('--output-dir', type=Path, required=True)
    p.add_argument('--allocation', choices=['fixed-slot','exact-size'], default='fixed-slot')
    p.add_argument('--max-waste-fraction', type=float, default=0.05)
    p.add_argument('--max-chunks-per-handle', type=int, default=32)
    p.add_argument('--counts', nargs='+', type=int, default=list(DEFAULT_COUNTS))
    args = p.parse_args()
    for path in args.handles_csv:
        rows = load_handles(path)
        results, options = optimize(rows, args.allocation, tuple(args.counts),
                                    args.max_waste_fraction, args.max_chunks_per_handle)
        out = args.output_dir / path.parent.name
        out.mkdir(parents=True, exist_ok=True)
        with (out/'chunk_plan.csv').open('w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=list(results[0]))
            writer.writeheader(); writer.writerows(results)
        with (out/'candidates.csv').open('w', newline='', encoding='utf-8') as f:
            fields = ['size_bytes','chunk_size_bytes','chunks','reserved_bytes','waste_bytes']
            writer = csv.DictWriter(f, fieldnames=fields); writer.writeheader()
            for size, opts in options.items():
                for c in opts: writer.writerow(dict(size_bytes=size, **c))
        metrics = dict(trace=str(path), allocation=args.allocation,
                       max_waste_fraction=args.max_waste_fraction,
                       max_chunks_per_handle=args.max_chunks_per_handle,
                       **summary(results))
        (out/'summary.json').write_text(json.dumps(metrics, indent=2)+'\n', encoding='utf-8')
        print(f'{path}: {metrics["handles"]} handles, {metrics["total_chunks"]} chunks, '
              f'{metrics["waste_fraction"]:.2%} waste, {metrics["budget_violations"]} budget violations -> {out}')


if __name__ == '__main__':
    main()
