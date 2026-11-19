"""Analyze handle sizes and fixed-slot chunk fragmentation in NNTile .rec traces."""
from __future__ import annotations

import argparse
import csv
import math
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from nntile_sim.parser import parse_tasks

MIB = 1024 ** 2


def collect_handles(tasks):
    sizes = {}
    accesses = Counter()
    for task in tasks:
        for access in task.accesses:
            if access.size_bytes <= 0:
                raise ValueError(f'Non-positive size for handle {access.handle!r}')
            previous = sizes.setdefault(access.handle, access.size_bytes)
            if previous != access.size_bytes:
                raise ValueError(f'Inconsistent size for handle {access.handle!r}: {previous} vs {access.size_bytes}')
            accesses[access.handle] += 1
    return sizes, accesses


def evaluate(sizes, accesses, chunk_bytes):
    if chunk_bytes <= 0:
        raise ValueError('Chunk size must be positive')
    payload = sum(sizes.values())
    count = sum((size + chunk_bytes - 1) // chunk_bytes for size in sizes.values())
    reserved = count * chunk_bytes
    waste = reserved - payload
    weighted_waste = sum(((size + chunk_bytes - 1) // chunk_bytes * chunk_bytes - size) * accesses[h] for h, size in sizes.items())
    return dict(chunk_bytes=chunk_bytes, handles=len(sizes), chunks=count,
                payload_bytes=payload, fixed_slot_reserved_bytes=reserved,
                fixed_slot_waste_bytes=waste,
                fixed_slot_waste_pct=100 * waste / reserved if reserved else 0,
                exact_size_reserved_bytes=payload,
                access_weighted_waste_bytes=weighted_waste)


def write_csv(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('traces', nargs='+', type=Path)
    parser.add_argument('--chunk-sizes-mib', type=float, nargs='+', default=[1, 2, 4, 8, 16, 32])
    parser.add_argument('--output-dir', type=Path, required=True)
    args = parser.parse_args(argv)
    if any(not math.isfinite(x) or x <= 0 or int(x * MIB) < 1 for x in args.chunk_sizes_mib):
        parser.error('All chunk sizes must be positive finite values representing at least one byte')
    for trace in args.traces:
        tasks = parse_tasks(trace)
        sizes, accesses = collect_handles(tasks)
        if not sizes:
            parser.error(f'No handles found in {trace}')
        handle_rows = [dict(handle=h, size_bytes=size, accesses=accesses[h]) for h, size in sorted(sizes.items())]
        chunk_rows = [evaluate(sizes, accesses, int(mib * MIB)) for mib in args.chunk_sizes_mib]
        output = args.output_dir / trace.stem
        write_csv(output / 'handles.csv', handle_rows)
        write_csv(output / 'chunk_candidates.csv', chunk_rows)
        values = sorted(sizes.values())
        def percentile(p):
            return values[math.ceil(p * len(values)) - 1]
        lines = [f'# Handle analysis: {trace.name}', '', f'- Tasks: {len(tasks)}',
                 f'- Unique handles: {len(sizes)}', f'- Access records: {sum(accesses.values())}',
                 f'- Min size: {values[0]} bytes', f'- Median size: {percentile(.5)} bytes',
                 f'- P90 size: {percentile(.9)} bytes', f'- Max size: {values[-1]} bytes', '',
                 'The totals assume all unique handles are allocated simultaneously; they are NOT GPU peak usage.',
                 'Exact-size reserved bytes are a theoretical allocation comparison, not measured transfer traffic.',
                 'Access-weighted waste is a diagnostic score, not physical memory occupancy.', '',
                 '| Chunk (MiB) | Chunks | Fixed-slot waste (%) |', '|---:|---:|---:|']
        for mib, row in zip(args.chunk_sizes_mib, chunk_rows):
            lines.append(f'| {mib:g} | {row["chunks"]} | {row["fixed_slot_waste_pct"]:.2f} |')
        lines += ['', 'No optimal chunk size is selected at this stage. Compare candidate sizes using full simulations in Milestone 7.2.', '']
        (output / 'summary.md').write_text('\n'.join(lines), encoding='utf-8')
        print(f'{trace}: {len(tasks)} tasks, {len(sizes)} handles -> {output}')


if __name__ == '__main__':
    main()
