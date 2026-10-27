"""Command-line entry point for running a trace through the baseline simulator."""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

from .parser import parse_tasks, TraceFormatError
from .core.engine import Config, SimulationError, Simulator

MIB = 1024 ** 2
GBPS_TO_BYTES_PER_US = 1000.0  # decimal GB/s -> bytes/microsecond


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Simulate NNTile GPU-memory scheduling (baseline policy)")
    parser.add_argument("trace", type=Path, help="NNTile .rec trace file")
    parser.add_argument("--gpu-memory-mib", type=float, required=True, help="GPU capacity in MiB")
    parser.add_argument("--h2d-gbps", type=float, required=True, help="CPU-to-GPU bandwidth in decimal GB/s")
    parser.add_argument("--d2h-gbps", type=float, required=True, help="GPU-to-CPU bandwidth in decimal GB/s")
    parser.add_argument("--memory-policy", choices=("lru-demand", "legacy-baseline"), default="lru-demand")
    parser.add_argument("--compute-order", choices=("topological", "trace", "custom"), default="topological")
    parser.add_argument("--custom-order-file", type=Path, help="Text file with one JobId per line (required for custom order)")
    parser.add_argument("--events-csv", type=Path, help="Save simulated events as CSV")
    parser.add_argument("--plot-html", type=Path, help="Save interactive Plotly timeline HTML")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.gpu_memory_mib <= 0 or args.h2d_gbps <= 0 or args.d2h_gbps <= 0:
        parser.error("GPU memory and both bandwidths must be positive")
    if (args.compute_order == "custom") != (args.custom_order_file is not None):
        parser.error("--custom-order-file is required exactly when --compute-order custom is used")
    try:
        custom = None
        if args.custom_order_file is not None:
            custom = tuple(line.strip() for line in args.custom_order_file.read_text(encoding="utf-8").splitlines() if line.strip())
        tasks = parse_tasks(args.trace)
        if not tasks:
            raise SimulationError("Trace contains no completed compute tasks")
        config = Config(
            capacity_bytes=int(args.gpu_memory_mib * MIB),
            h2d_bytes_per_us=args.h2d_gbps * GBPS_TO_BYTES_PER_US,
            d2h_bytes_per_us=args.d2h_gbps * GBPS_TO_BYTES_PER_US,
            compute_order=args.compute_order,
            custom_order=custom,
            memory_policy=args.memory_policy,
        )
        result = Simulator(tasks, config).run()
        if args.events_csv is not None:
            args.events_csv.parent.mkdir(parents=True, exist_ok=True)
            with args.events_csv.open("w", encoding="utf-8", newline="") as file:
                writer = csv.writer(file)
                writer.writerow(("lane", "action", "handle", "job_id", "start_us", "end_us", "used_bytes_after"))
                for event in result.events:
                    writer.writerow((event.lane, event.action, event.handle or "", event.job_id or "",
                                     event.start_us, event.end_us, event.used_bytes_after))
    except (OSError, ValueError, TraceFormatError, SimulationError) as exc:
        print(f"Simulation failed: {exc}", file=sys.stderr)
        return 1

    print(f"NNTile simulation ({args.memory_policy})")
    print(f"Trace: {args.trace}")
    print(f"Compute order: {args.compute_order}")
    print(f"Completed tasks: {result.completed_jobs:,} / {len(tasks):,}")
    print(f"Makespan: {result.makespan_us:,.3f} us ({result.makespan_us / 1e6:,.6f} s)")
    print(f"Peak GPU memory: {result.peak_bytes / MIB:,.3f} MiB")
    print(f"H2D transferred: {result.h2d_bytes / MIB:,.3f} MiB")
    print(f"D2H transferred: {result.d2h_bytes / MIB:,.3f} MiB")
    if args.events_csv is not None:
        print(f"Events CSV: {args.events_csv}")
    if args.plot_html is not None:
        from .plotting import create_timeline
        try:
            create_timeline([dict(lane=e.lane, action=e.action, handle=e.handle or "",
                                  job_id=e.job_id or "", start_us=e.start_us, end_us=e.end_us)
                             for e in result.events], args.plot_html, title=f"NNTile {args.memory_policy}: {args.trace.name}")
        except (OSError, RuntimeError, ValueError) as exc:
            print(f"Plot failed: {exc}", file=sys.stderr)
            return 1
        print(f"Plotly timeline: {args.plot_html}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
