"""Validate observable invariants of an NNTile simulator event trace.

This is a trace-level checker, not a full GPU residency/version proof: allocations
without Memory events cannot be reconstructed from the current event schema.
"""
from __future__ import annotations

import math
from collections import defaultdict


def validate_events(tasks, config, result):
    """Return a list of problems; an empty list means the checked invariants hold."""
    errors = []
    eps = 1e-9
    by_id = {str(t.job_id): t for t in tasks}
    computes = defaultdict(list)
    lanes = defaultdict(list)
    transfers = defaultdict(list)
    sizes = {}
    for task in tasks:
        for access in task.accesses:
            sizes[access.handle] = access.size_bytes

    for i, event in enumerate(result.events):
        if not all(math.isfinite(x) for x in (event.start_us, event.end_us)):
            errors.append(f"event {i}: non-finite time")
            continue
        if event.start_us < -eps or event.end_us + eps < event.start_us:
            errors.append(f"event {i}: invalid time interval")
        if event.end_us > result.makespan_us + eps:
            errors.append(f"event {i}: ends after makespan")
        if not 0 <= event.used_bytes_after <= config.capacity_bytes:
            errors.append(f"event {i}: memory outside capacity")
        if event.used_bytes_after > result.peak_bytes:
            errors.append(f"event {i}: event memory exceeds reported peak")
        if event.lane in ("H2D", "D2H", "Compute"):
            lanes[event.lane].append(event)
        if event.lane == "Compute":
            computes[str(event.job_id)].append(event)
        if event.lane in ("H2D", "D2H"):
            transfers[event.handle].append(event)
            if event.handle not in sizes:
                errors.append(f"event {i}: unknown transfer handle {event.handle}")

    for lane, events in lanes.items():
        ordered = sorted(events, key=lambda e: (e.start_us, e.end_us))
        for prev, curr in zip(ordered, ordered[1:]):
            if curr.start_us < prev.end_us - eps:
                errors.append(f"{lane}: overlapping lane events")

    for job_id, task in by_id.items():
        entries = computes[job_id]
        if len(entries) != 1:
            errors.append(f"job {job_id}: expected one Compute event, got {len(entries)}")
            continue
        compute = entries[0]
        if abs((compute.end_us - compute.start_us) - task.duration_us) > eps:
            errors.append(f"job {job_id}: incorrect compute duration")
        for dep in task.depends_on:
            if str(dep) in by_id and len(computes[str(dep)]) == 1:
                if computes[str(dep)][0].end_us > compute.start_us + eps:
                    errors.append(f"job {job_id}: dependency {dep} violated")
        for access in task.accesses:
            for transfer in transfers[access.handle]:
                if max(compute.start_us, transfer.start_us) < min(compute.end_us, transfer.end_us) - eps:
                    errors.append(f"job {job_id}: {transfer.lane} overlaps access to {access.handle}")

    for job_id in computes:
        if job_id not in by_id:
            errors.append(f"unknown Compute job {job_id}")
    if result.completed_jobs != len(tasks):
        errors.append("completed_jobs mismatch")
    if sum(len(v) for v in computes.values()) != result.completed_jobs:
        errors.append("Compute event count mismatch")
    if not 0 <= result.peak_bytes <= config.capacity_bytes:
        errors.append("peak memory outside capacity")
    for lane, field in (("H2D", "h2d_bytes"), ("D2H", "d2h_bytes")):
        total = sum(sizes[e.handle] for e in lanes[lane] if e.handle in sizes)
        if total != getattr(result, field):
            errors.append(f"{field} mismatch: events={total}, reported={getattr(result, field)}")
    if result.events and abs(max(e.end_us for e in result.events) - result.makespan_us) > eps:
        errors.append("makespan does not match final event")
    if not result.events and result.makespan_us != 0:
        errors.append("nonzero makespan with no events")
    return errors
