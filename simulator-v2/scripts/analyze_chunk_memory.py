"""Reconstruct chunk residency and GPU slot waste from an events CSV."""

import argparse
import csv
from pathlib import Path

from nntile_sim.chunks import split_into_chunks
from nntile_sim.parser import parse_tasks

MIB = 1024 ** 2


def analyze(trace: Path, events_csv: Path, chunk_bytes: int, output: Path):
    tasks = parse_tasks(trace)

    sizes = {}
    for task in tasks:
        for access in task.accesses:
            old = sizes.setdefault(access.handle, access.size_bytes)
            if old != access.size_bytes:
                raise ValueError(f"Inconsistent size for {access.handle}")

    chunks = {
        f"{chunk.handle}:{chunk.index}": chunk
        for handle, size in sizes.items()
        for chunk in split_into_chunks(handle, size, chunk_bytes)
    }

    resident = set()
    reserved = 0
    payload = 0
    rows = []
    peak_waste = 0

    with events_csv.open(newline="", encoding="utf-8") as file:
        for line_number, event in enumerate(csv.DictReader(file), start=2):
            lane = event["lane"]
            action = event["action"]
            key = event["handle"]

            if (lane, action) in {
                ("H2D", "LOAD"),
                ("Memory", "ALLOC"),
            }:
                if key not in chunks:
                    raise ValueError(f"Line {line_number}: unknown chunk {key}")
                if key in resident:
                    raise ValueError(f"Line {line_number}: duplicate load {key}")

                resident.add(key)
                reserved += chunks[key].reserved_bytes
                payload += chunks[key].size_bytes

            elif (lane, action) == ("D2H", "UNLOAD"):
                if key not in resident:
                    raise ValueError(f"Line {line_number}: nonresident unload {key}")

                resident.remove(key)
                reserved -= chunks[key].reserved_bytes
                payload -= chunks[key].size_bytes

            elif (lane, action) == ("Memory", "DROP"):
                # After D2H/UNLOAD, DROP is only a marker.
                if key in resident:
                    resident.remove(key)
                    reserved -= chunks[key].reserved_bytes
                    payload -= chunks[key].size_bytes

            recorded = int(event["used_bytes_after"])
            if reserved != recorded:
                raise ValueError(
                    f"Line {line_number}: reserved={reserved}, "
                    f"used_bytes_after={recorded}"
                )

            waste = reserved - payload
            peak_waste = max(peak_waste, waste)

            rows.append({
                "time_us": event["end_us"],
                "reserved_bytes": reserved,
                "payload_bytes": payload,
                "wasted_bytes": waste,
            })

    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "time_us",
                "reserved_bytes",
                "payload_bytes",
                "wasted_bytes",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"Validated events: {len(rows)}")
    print(f"Peak slot waste: {peak_waste / MIB:.3f} MiB")
    print(f"Memory CSV: {output}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("trace", type=Path)
    parser.add_argument("events_csv", type=Path)
    parser.add_argument("--chunk-mib", type=float, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    if not (0 < args.chunk_mib < float("inf")):
        parser.error("--chunk-mib must be positive and finite")

    chunk_bytes = int(args.chunk_mib * MIB)
    if chunk_bytes < 1:
        parser.error("--chunk-mib must represent at least one byte")

    analyze(args.trace, args.events_csv, chunk_bytes, args.output)


if __name__ == "__main__":
    main()
