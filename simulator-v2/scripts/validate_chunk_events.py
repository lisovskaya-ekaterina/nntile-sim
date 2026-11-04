
"""Validate sequential chunked simulator event CSV files."""

import argparse
import csv
from collections import Counter
from pathlib import Path

from nntile_sim.chunks import split_into_chunks
from nntile_sim.parser import parse_tasks

MIB = 1024 ** 2


def validate(
    path: Path,
    capacity_mib: int,
    chunk_mib: int,
    chunk_allocation: str = "fixed-slot",
    trace: Path | None = None,
) -> None:
    capacity = capacity_mib * MIB
    chunk_size = chunk_mib * MIB

    if capacity <= 0:
        raise ValueError("GPU capacity must be positive")
    if chunk_size <= 0:
        raise ValueError("Chunk size must be positive")
    if chunk_allocation not in ("fixed-slot", "exact-size"):
        raise ValueError("Unknown chunk allocation mode")
    if chunk_allocation == "exact-size" and trace is None:
        raise ValueError("--trace is required for exact-size")

    chunk_sizes = {}
    expected_compute_count = 5484  # Legacy default for tasks_mbs1.rec

    if trace is not None:
        tasks = parse_tasks(trace)
        expected_compute_count = len(tasks)

        sizes = {}
        for task in tasks:
            for access in task.accesses:
                previous = sizes.setdefault(
                    access.handle,
                    access.size_bytes,
                )
                if previous != access.size_bytes:
                    raise ValueError(
                        f"Inconsistent size for {access.handle}"
                    )

        chunk_sizes = {
            f"{chunk.handle}:{chunk.index}": chunk.size_bytes
            for handle, size in sizes.items()
            for chunk in split_into_chunks(
                handle, size, chunk_size
            )
        }

    with path.open(newline="", encoding="utf-8") as file:
        events = list(csv.DictReader(file))

    resident = set()
    pending_drop = None
    counts = Counter()
    previous_end = 0.0
    peak = 0

    for i, event in enumerate(events, start=2):
        lane = event["lane"]
        action = event["action"]
        handle = event["handle"]
        start = float(event["start_us"])
        end = float(event["end_us"])
        used = int(event["used_bytes_after"])

        counts[(lane, action)] += 1

        # Sequential simulation: events must not overlap.
        assert start <= end + 1e-5, (
            i, "Negative event duration"
        )
        assert start >= previous_end - 1e-5, (
            i, "Overlapping events"
        )
        previous_end = max(previous_end, end)

        # A dirty eviction emits D2H/UNLOAD followed by Memory/DROP.
        if pending_drop is not None:
            assert (lane, action) == ("Memory", "DROP"), (
                i, "Expected DROP after UNLOAD", pending_drop
            )
            assert handle == pending_drop, (
                i,
                "DROP handle differs from UNLOAD",
                handle,
                pending_drop,
            )

        if (lane, action) in (
            ("Memory", "ALLOC"),
            ("H2D", "LOAD"),
        ):
            assert handle not in resident, (
                i, "Chunk already resident", handle
            )

            if chunk_sizes:
                assert handle in chunk_sizes, (
                    i, "Unknown chunk", handle
                )

            resident.add(handle)

        elif (lane, action) == ("D2H", "UNLOAD"):
            assert handle in resident, (
                i, "Unloading nonresident chunk", handle
            )
            resident.remove(handle)
            pending_drop = handle

        elif (lane, action) == ("Memory", "DROP"):
            if pending_drop is not None:
                assert handle == pending_drop
                pending_drop = None
            else:
                assert handle in resident, (
                    i, "Dropping nonresident chunk", handle
                )
                resident.remove(handle)

        elif lane != "Compute":
            raise AssertionError(
                (i, "Unexpected event", lane, action)
            )

        # Reconstruct GPU occupancy independently of the simulator.
        if chunk_allocation == "exact-size":
            expected_used = sum(
                chunk_sizes[key]
                for key in resident
            )
        else:
            expected_used = len(resident) * chunk_size

        assert used == expected_used, (
            i,
            "GPU occupancy mismatch",
            used,
            expected_used,
        )

        assert 0 <= used <= capacity, (
            i, "GPU capacity exceeded", used
        )

        peak = max(peak, used)

    assert pending_drop is None, (
        "Missing DROP after final UNLOAD", pending_drop
    )

    compute_count = sum(
        count
        for (lane, _), count in counts.items()
        if lane == "Compute"
    )

    assert compute_count == expected_compute_count, (
        "Unexpected Compute count",
        compute_count,
        expected_compute_count,
    )

    print(f"File: {path}")
    print(f"Allocation mode: {chunk_allocation}")
    print(f"Events: {len(events)}")
    print(f"Compute: {compute_count}")
    print(f"Peak GPU: {peak / MIB:.3f} MiB")
    print(f"Resident chunks at end: {len(resident)}")
    print("Chunk residency checks passed.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Validate chunked simulator event CSV files"
    )
    parser.add_argument("csv_file", type=Path)
    parser.add_argument(
        "--capacity-mib",
        type=int,
        required=True,
    )
    parser.add_argument(
        "--chunk-mib",
        type=int,
        required=True,
    )
    parser.add_argument(
        "--chunk-allocation",
        choices=("fixed-slot", "exact-size"),
        default="fixed-slot",
    )
    parser.add_argument(
        "--trace",
        type=Path,
        help="Source .rec trace (required for exact-size)",
    )

    args = parser.parse_args()

    validate(
        path=args.csv_file,
        capacity_mib=args.capacity_mib,
        chunk_mib=args.chunk_mib,
        chunk_allocation=args.chunk_allocation,
        trace=args.trace,
    )


if __name__ == "__main__":
    main()
