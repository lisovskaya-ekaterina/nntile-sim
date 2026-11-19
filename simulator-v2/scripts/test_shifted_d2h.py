"""Safely move existing D2H earlier without changing Compute or DROP."""

from bisect import bisect_right
from collections import defaultdict
from dataclasses import replace

from nntile_sim.parser import parse_tasks
from nntile_sim.chunks import split_into_chunks
from nntile_sim.core.engine import Config
from nntile_sim.core.global_scheduler import schedule

from validate_global_schedule import validate
from check_d2h_versions import check_versions


def main():
    tasks = parse_tasks("examples/tasks_mbs1.rec")
    cfg = Config(
        capacity_bytes=768 * 1024 * 1024,
        h2d_bytes_per_us=12000,
        d2h_bytes_per_us=12000,
        chunk_size_bytes=768 * 1024,
        memory_policy="belady-demand",
    )

    print("Building reference schedule...", flush=True)
    original = schedule(tasks, cfg)

    by_id = {str(t.job_id): t for t in tasks}
    sizes = {
        a.handle: a.size_bytes
        for t in tasks for a in t.accesses
    }
    keys = {
        h: [f"{c.handle}:{c.index}"
            for c in split_into_chunks(h, size, cfg.chunk_size_bytes)]
        for h, size in sizes.items()
    }

    # Compute uses of each chunk, including writes.
    uses = defaultdict(list)
    computes = sorted(
        (e for e in original.events if e.lane == "Compute"),
        key=lambda e: e.start_us,
    )

    for e in computes:
        task = by_id[str(e.job_id)]
        for access in task.accesses:
            for key in keys[access.handle]:
                uses[key].append(e.end_us)

    for times in uses.values():
        times.sort()

    # Track the beginning of each GPU residency interval.
    # A D2H cannot start before the chunk is resident and ready.
    residency = defaultdict(list)
    for e in original.events:
        if e.lane == "H2D":
            residency[e.handle].append((e.end_us, e.start_us))
        elif e.action == "ALLOC":
            residency[e.handle].append((e.end_us, e.start_us))

    for entries in residency.values():
        entries.sort()

    transfers = sorted(
        (e for e in original.events if e.lane == "D2H"),
        key=lambda e: e.start_us,
    )

    replacements = {}
    cursor = 0.0
    moved = 0
    moved_us = 0.0
    skipped = 0

    for e in transfers:
        history = uses[e.handle]
        pos = bisect_right(history, e.start_us + 1e-6)

        resident = residency[e.handle]
        resident_times = [entry[1] for entry in resident]
        rpos = bisect_right(resident_times, e.start_us + 1e-6)

        if pos == 0 or rpos == 0:
            release = e.start_us
            skipped += 1
        else:
            last_use = history[pos - 1]
            ready_time = resident[rpos - 1][0]

            # If the last use was in a previous residency
            # interval, do not move this transfer.
            if last_use + 1e-6 < ready_time:
                release = e.start_us
                skipped += 1
            else:
                release = max(last_use, ready_time)

        duration = e.end_us - e.start_us
        start = max(cursor, release)
        end = start + duration

        # Conservative fallback: never make a D2H finish
        # later than in the validated reference schedule.
        if end > e.end_us + 1e-6:
            start, end = e.start_us, e.end_us

        cursor = end

        if start < e.start_us - 1e-6:
            moved += 1
            moved_us += e.start_us - start

        replacements[id(e)] = replace(
            e, start_us=start, end_us=end
        )

    shifted_events = [
        replacements.get(id(e), e) for e in original.events
    ]

    shifted = replace(original, events=shifted_events)

    print("Validating shifted timeline...", flush=True)
    errors, peak = validate(tasks, cfg, shifted)

    if errors:
        print(f"FAIL: {len(errors)} errors")
        for error in errors:
            print(" -", error)
        raise SystemExit(1)

    print("Checking shifted D2H version safety...", flush=True)
    checked, version_errors = check_versions(tasks, cfg, shifted)
    print(f"Checked D2H transfers: {checked}")

    if version_errors:
        print(f"FAIL: {len(version_errors)} version/residency conflicts")
        for error in version_errors:
            print(" -", error)
        raise SystemExit(1)

    print(f"\nPASS: {shifted.completed_jobs} tasks")
    print(f"D2H transfers: {len(transfers)}")
    print(f"Moved earlier: {moved}")
    print(f"Not shifted (conservative): {skipped}")
    print(f"Sum of D2H start advances: {moved_us / 1e6:.3f} s")
    print(f"D2H traffic: {shifted.d2h_bytes / 1024**2:.2f} MiB")
    print(f"Peak GPU memory: {peak / 1024**2:.2f} MiB")
    print(f"Makespan: {shifted.makespan_us / 1e6:.6f} s")
    print("\nCompute times intentionally unchanged.")


if __name__ == "__main__":
    main()
