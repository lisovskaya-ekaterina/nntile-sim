#!/usr/bin/env python3
"""Validate a draft GPU schedule, including chunk versions and lifetimes."""

import json
import sys
from collections import defaultdict
from pathlib import Path


LANES = {
    "H2D": "H2D",
    "D2H": "D2H",
    "Compute": "Compute",
    "ALLOC": "Memory",
    "DROP": "Memory",
}


def validate(plan, schedule):
    errors = []

    plan_ops = plan["operations"]
    schedule_ops = schedule["operations"]
    ops = {op["id"]: op for op in plan_ops}
    times = {op["id"]: op for op in schedule_ops}

    if len(ops) != len(plan_ops) or len(times) != len(schedule_ops):
        return ["Duplicate operation IDs"]

    if set(ops) != set(times):
        return ["Plan and schedule operation IDs differ"]

    chunks = {c["id"]: c for c in plan["chunks"]}
    if len(chunks) != len(plan["chunks"]):
        return ["Duplicate chunk IDs"]

    # Structural checks.
    for op_id, op in ops.items():
        kind = op["type"]
        event = times[op_id]
        start = event["start_us"]
        end = event["end_us"]

        if kind not in LANES:
            errors.append(f"{op_id}: unknown operation type")
            continue

        if event["lane"] != LANES[kind]:
            errors.append(f"{op_id}: incorrect lane")

        if start < 0 or end < start:
            errors.append(f"{op_id}: invalid time interval")

        if kind in ("ALLOC", "DROP") and start != end:
            errors.append(f"{op_id}: memory operation must be instantaneous")

        if kind in ("H2D", "D2H", "Compute") and start == end:
            errors.append(f"{op_id}: operation must have positive duration")

        if kind in ("ALLOC", "DROP", "H2D", "D2H"):
            if op["chunk"] not in chunks:
                errors.append(f"{op_id}: unknown chunk")

        for access in op.get("reads", []) + op.get("writes", []):
            if access["chunk"] not in chunks:
                errors.append(f"{op_id}: unknown accessed chunk")

        for dep in op.get("depends_on", []):
            if dep not in times:
                errors.append(f"{op_id}: unknown dependency {dep}")
            elif times[dep]["end_us"] > start:
                errors.append(f"{op_id}: dependency {dep} not completed")

    # Continue checking validly structured operations even if
    # dependencies or timing constraints have been violated.
    # This allows reporting both dependency and memory-lifetime errors.
    if any(
        "unknown operation type" in error
        or "unknown chunk" in error
        or "unknown accessed chunk" in error
        for error in errors
    ):
        return errors

    # Each execution lane is serial.
    for lane in ("H2D", "D2H", "Compute"):
        events = sorted(
            (e for e in times.values() if e["lane"] == lane),
            key=lambda e: (e["start_us"], e["end_us"]),
        )
        for previous, current in zip(events, events[1:]):
            if previous["end_us"] > current["start_us"]:
                errors.append(
                    f"{lane}: {previous['id']} overlaps {current['id']}"
                )

    # Event-driven memory and version simulation.
    # Phase 0: completions.
    # Phase 1: instantaneous DROP (releases).
    # Phase 2: instantaneous ALLOC (reserves).
    # Phase 3: transfer starts.
    # Phase 4: Compute starts.
    #
    # At equal timestamps, completed operations are visible before
    # new operations start. DROP precedes new reservations.
    timeline = []
    for op_id, op in ops.items():
        kind = op["type"]
        start = times[op_id]["start_us"]
        end = times[op_id]["end_us"]

        if kind == "DROP":
            timeline.append((start, 1, op_id, "instant"))
        elif kind == "ALLOC":
            timeline.append((start, 2, op_id, "instant"))
        else:
            phase = 4 if kind == "Compute" else 3
            timeline.append((start, phase, op_id, "start"))
            timeline.append((end, 0, op_id, "end"))

    timeline.sort()

    cpu = dict(plan.get("initial_cpu_versions", {}))
    gpu = {}  # chunk -> version, or None while uninitialized/loading
    busy = defaultdict(set)  # chunk -> active operation IDs
    used = 0
    peak = 0

    def reserve(chunk, op_id):
        nonlocal used, peak
        if chunk in gpu:
            errors.append(f"{op_id}: chunk already allocated")
            return False
        gpu[chunk] = None
        used += chunks[chunk]["reserved_bytes"]
        peak = max(peak, used)
        if used > plan["capacity_bytes"]:
            errors.append(f"{op_id}: GPU capacity exceeded")
        return True

    for timestamp, _, op_id, phase in timeline:
        op = ops[op_id]
        kind = op["type"]

        if phase == "instant":
            chunk = op["chunk"]

            if kind == "ALLOC":
                reserve(chunk, op_id)

            elif kind == "DROP":
                if chunk not in gpu:
                    errors.append(f"{op_id}: dropping nonresident chunk")
                    continue

                if busy[chunk]:
                    errors.append(
                        f"{op_id}: DROP during active operation "
                        f"{sorted(busy[chunk])}"
                    )

                version = gpu[chunk]
                if version is not None and cpu.get(chunk) != version:
                    # A version is live if a later operation reads it.
                    future_reads = any(
                        access["chunk"] == chunk
                        and access["version"] == version
                        and times[other_id]["start_us"] > timestamp
                        for other_id, other in ops.items()
                        for access in other.get("reads", [])
                    )
                    if future_reads:
                        errors.append(
                            f"{op_id}: losing dirty version "
                            f"{chunk}@{version}"
                        )

                used -= chunks[chunk]["reserved_bytes"]
                del gpu[chunk]

            continue

        if phase == "start":
            if kind == "H2D":
                chunk = op["chunk"]
                version = op["version"]
                if cpu.get(chunk) != version:
                    errors.append(f"{op_id}: CPU version mismatch")
                if reserve(chunk, op_id):
                    busy[chunk].add(op_id)

            elif kind == "D2H":
                chunk = op["chunk"]
                if gpu.get(chunk) != op["version"]:
                    errors.append(f"{op_id}: invalid D2H source version")
                busy[chunk].add(op_id)

            elif kind == "Compute":
                for access in op.get("writes", []):
                    chunk = access["chunk"]
                    active_d2h = [
                        active_id
                        for active_id in busy[chunk]
                        if ops[active_id]["type"] == "D2H"
                    ]
                    if active_d2h:
                        errors.append(
                            f"{op_id}: write conflict with active D2H "
                            f"on {chunk}"
                        )

                for access in op.get("reads", []):
                    chunk = access["chunk"]
                    if gpu.get(chunk) != access["version"]:
                        errors.append(
                            f"{op_id}: missing or stale GPU version "
                            f"{chunk}@{access['version']}"
                        )

                for access in op.get("writes", []):
                    if access["chunk"] not in gpu:
                        errors.append(
                            f"{op_id}: write target not allocated "
                            f"{access['chunk']}"
                        )

                accessed = {
                    a["chunk"]
                    for a in op.get("reads", []) + op.get("writes", [])
                }
                for chunk in accessed:
                    busy[chunk].add(op_id)

        else:  # Completion
            if kind == "H2D":
                chunk = op["chunk"]
                gpu[chunk] = op["version"]
                busy[chunk].discard(op_id)

            elif kind == "D2H":
                chunk = op["chunk"]
                if gpu.get(chunk) != op["version"]:
                    errors.append(f"{op_id}: D2H source changed")
                else:
                    cpu[chunk] = op["version"]
                active_writers = [
                    active_id
                    for active_id in busy[chunk]
                    if ops[active_id]["type"] == "Compute"
                    and any(
                        access["chunk"] == chunk
                        for access in ops[active_id].get("writes", [])
                    )
                ]

                if active_writers:
                    errors.append(
                        f"{op_id}: D2H conflict with active Compute write "
                        f"on {chunk}"
                    )
                busy[chunk].discard(op_id)

            elif kind == "Compute":
                for access in op.get("writes", []):
                    chunk = access["chunk"]
                    gpu[chunk] = access["version"]

                accessed = {
                    a["chunk"]
                    for a in op.get("reads", []) + op.get("writes", [])
                }
                for chunk in accessed:
                    busy[chunk].discard(op_id)

    metrics = schedule.get("metrics", {})
    makespan = max(e["end_us"] for e in times.values())

    if "makespan_us" in metrics and metrics["makespan_us"] != makespan:
        errors.append("Incorrect makespan_us")

    if "peak_gpu_bytes" in metrics and metrics["peak_gpu_bytes"] != peak:
        errors.append("Incorrect peak_gpu_bytes")

    return errors


def main():
    directory = Path(__file__).resolve().parents[1] / "docs" / "scheduling"

    plan = json.loads((directory / "example-plan.json").read_text())
    schedule = json.loads((directory / "example-schedule.json").read_text())

    errors = validate(plan, schedule)

    if errors:
        for error in errors:
            print("ERROR:", error)
        sys.exit(1)

    print("Schedule validation OK")


if __name__ == "__main__":
    main()
