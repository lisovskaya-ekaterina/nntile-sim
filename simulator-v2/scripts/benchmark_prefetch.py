"""Compare demand-only and one-task prefetch on deterministic micrographs.

Run from repository root: python simulator-v2/scripts/benchmark_prefetch.py
Optional: --output results.csv
"""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

# Support running directly from the repository root without installing the package.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from nntile_sim.model import Task, Access
from nntile_sim.core.engine import Config, Simulator
from validate_prefetch_events import validate_events


def make_task(jid, accesses, dep=(), duration=10):
    return Task(
        str(jid),
        str(jid),
        None,
        tuple(Access(*a) for a in accesses),
        0,
        duration / 1_000_000,
        None,
        tuple(str(d) for d in dep),
    )


def cases():
    return {
        "h2d_overlap": (
            [make_task(1, [("a", 40, "R")], duration=40),
             make_task(2, [("b", 40, "R")], dep=(1,))], 100),
        "capacity_limited": (
            [make_task(1, [("a", 40, "R")], duration=40),
             make_task(2, [("b", 60, "R")], dep=(1,))], 80),
        "dirty_reuse": (
            [make_task(1, [("a", 40, "W")]),
             make_task(2, [("b", 40, "R")], dep=(1,), duration=40),
             make_task(3, [("c", 40, "R")], dep=(2,)),
             make_task(4, [("a", 40, "R")], dep=(3,))], 80),
    }


def run_benchmark():
    rows = []
    for name, (tasks, capacity) in cases().items():
        for policy in ("lru-demand", "belady-demand"):
            results = {}
            for depth in (0, 1):
                cfg = Config(capacity_bytes=capacity, h2d_bytes_per_us=10,
                             d2h_bytes_per_us=5, memory_policy=policy,
                             prefetch_depth=depth)
                result = Simulator(tasks, cfg).run()
                errors = validate_events(tasks, cfg, result)
                if errors:
                    raise RuntimeError(f"{name} / {policy} / depth={depth}: " + "; ".join(errors))
                results[depth] = result
            base, prefetch = results[0], results[1]
            rows.append({
                "case": name, "policy": policy,
                "demand_makespan_us": base.makespan_us,
                "prefetch_makespan_us": prefetch.makespan_us,
                "speedup": base.makespan_us / prefetch.makespan_us,
                "demand_h2d_bytes": base.h2d_bytes,
                "prefetch_h2d_bytes": prefetch.h2d_bytes,
                "demand_d2h_bytes": base.d2h_bytes,
                "prefetch_d2h_bytes": prefetch.d2h_bytes,
                "demand_peak_bytes": base.peak_bytes,
                "prefetch_peak_bytes": prefetch.peak_bytes,
            })
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, help="Optional CSV output path")
    args = parser.parse_args()
    rows = run_benchmark()
    writer = csv.DictWriter(sys.stdout, fieldnames=list(rows[0]))
    writer.writeheader()
    writer.writerows(rows)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open("w", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)


if __name__ == "__main__":
    main()
