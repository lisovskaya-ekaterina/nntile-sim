"""Offline D2H planning from a validated demand-eviction schedule."""

import csv
import heapq
from collections import defaultdict
from pathlib import Path

from nntile_sim.parser import parse_tasks
from nntile_sim.chunks import split_into_chunks
from nntile_sim.core.engine import Config
from nntile_sim.core.global_scheduler import schedule


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
    result = schedule(tasks, cfg)

    by_id = {str(t.job_id): t for t in tasks}
    sizes = {
        a.handle: a.size_bytes
        for t in tasks for a in t.accesses
    }

    chunk_sizes = {}
    for handle, size in sizes.items():
        for c in split_into_chunks(handle, size, cfg.chunk_size_bytes):
            chunk_sizes[f"{c.handle}:{c.index}"] = c.size_bytes

    # Timeline of Compute uses for each chunk.
    uses = defaultdict(list)
    for e in result.events:
        if e.lane != "Compute":
            continue
        task = by_id[str(e.job_id)]
        for a in task.accesses:
            for c in split_into_chunks(
                a.handle, sizes[a.handle], cfg.chunk_size_bytes
            ):
                uses[f"{c.handle}:{c.index}"].append(e.end_us)

    for times in uses.values():
        times.sort()

    # Every demand D2H represents a future dirty eviction.
    # Its original start is the memory-pressure deadline.
    transfers = sorted(
        (e for e in result.events if e.lane == "D2H"),
        key=lambda e: e.start_us,
    )

    import bisect

    jobs = []
    for e in transfers:
        times = uses[e.handle]
        pos = bisect.bisect_right(times, e.start_us + 1e-6)
        if pos == 0:
            raise RuntimeError(f"No preceding Compute: {e.handle}")

        release = times[pos - 1]
        deadline = e.start_us
        duration = chunk_sizes[e.handle] / cfg.d2h_bytes_per_us

        jobs.append({
            "chunk": e.handle,
            "release_us": release,
            "deadline_us": deadline,
            "duration_us": duration,
        })

    # Non-preemptive D2H queue.
    # Among transfers whose data are ready, choose earliest deadline.
    arrivals = sorted(jobs, key=lambda j: j["release_us"])
    pending = []
    planned = []
    cursor = 0.0
    index = 0
    serial = 0

    while index < len(arrivals) or pending:
        if not pending and index < len(arrivals):
            cursor = max(cursor, arrivals[index]["release_us"])

        while (
            index < len(arrivals)
            and arrivals[index]["release_us"] <= cursor + 1e-6
        ):
            job = arrivals[index]
            heapq.heappush(
                pending, (job["deadline_us"], serial, job)
            )
            serial += 1
            index += 1

        if not pending:
            continue

        _, _, job = heapq.heappop(pending)
        start = max(cursor, job["release_us"])
        end = start + job["duration_us"]

        planned.append({
            **job,
            "planned_start_us": start,
            "planned_end_us": end,
            "slack_us": job["deadline_us"] - end,
        })
        cursor = end

    output = Path("simulator-v2/early_d2h_plan.csv")
    with output.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=planned[0].keys())
        writer.writeheader()
        writer.writerows(planned)

    on_time = [j for j in planned if j["slack_us"] >= -1e-6]
    late = [j for j in planned if j["slack_us"] < -1e-6]

    print(f"\nD2H planned: {len(planned)}")
    print(f"Meet deadline: {len(on_time)}")
    print(f"Miss deadline: {len(late)}")
    print(
        "Original D2H traffic: "
        f"{result.d2h_bytes / 1024**2:.2f} MiB"
    )

    if late:
        print(
            "Maximum lateness: "
            f"{max(-j['slack_us'] for j in late) / 1000:.3f} ms"
        )

    print(f"Saved: {output}")


if __name__ == "__main__":
    main()
