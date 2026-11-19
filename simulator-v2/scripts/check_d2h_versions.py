"""Check D2H version safety against Compute writes and GPU residency."""

from collections import defaultdict

from nntile_sim.parser import parse_tasks
from nntile_sim.chunks import split_into_chunks
from nntile_sim.core.engine import Config
from nntile_sim.core.global_scheduler import schedule


def check_versions(tasks, cfg, result):
    sizes = {}
    for task in tasks:
        for access in task.accesses:
            sizes[access.handle] = access.size_bytes

    chunks_by_handle = {
        handle: [
            f"{c.handle}:{c.index}"
            for c in split_into_chunks(handle, size, cfg.chunk_size_bytes)
        ]
        for handle, size in sizes.items()
    }

    by_id = {str(t.job_id): t for t in tasks}

    # Timeline events:
    # Compute reads/writes occupy the entire Compute interval.
    # D2H reads the GPU version throughout its transfer interval.
    # H2D and ALLOC begin a new GPU residency interval.
    computes_by_chunk = defaultdict(list)
    d2h_by_chunk = defaultdict(list)
    residency_by_chunk = defaultdict(list)

    for event in result.events:
        if event.lane == "Compute":
            task = by_id[str(event.job_id)]
            for access in task.accesses:
                for key in chunks_by_handle[access.handle]:
                    computes_by_chunk[key].append((
                        event.start_us,
                        event.end_us,
                        access.writes_value,
                        event.job_id,
                    ))

        elif event.lane == "D2H":
            d2h_by_chunk[event.handle].append(event)

        elif event.lane == "H2D":
            residency_by_chunk[event.handle].append(
                (event.start_us, "LOAD")
            )

        elif event.action == "ALLOC":
            residency_by_chunk[event.handle].append(
                (event.start_us, "ALLOC")
            )

        elif event.action == "DROP":
            residency_by_chunk[event.handle].append(
                (event.start_us, "DROP")
            )

    errors = []
    checked = 0
    EPS = 1e-6

    for key, transfers in d2h_by_chunk.items():
        computes = computes_by_chunk[key]
        residency = sorted(residency_by_chunk[key])

        for transfer in transfers:
            checked += 1
            start = transfer.start_us
            end = transfer.end_us

            # D2H cannot overlap any Compute using the same chunk.
            for cstart, cend, writes, job_id in computes:
                if start < cend - EPS and end > cstart + EPS:
                    errors.append(
                        f"{key}: D2H [{start:.3f}, {end:.3f}] "
                        f"overlaps Compute {job_id} "
                        f"[{cstart:.3f}, {cend:.3f}]"
                    )
                    break

            # D2H must stay inside one GPU residency interval.
            resident = False
            for timestamp, action in residency:
                if timestamp > start + EPS:
                    break
                resident = action != "DROP"

            if not resident:
                errors.append(
                    f"{key}: D2H starts without GPU residency "
                    f"at {start:.3f}"
                )

            for timestamp, action in residency:
                if start + EPS < timestamp < end - EPS:
                    errors.append(
                        f"{key}: {action} during D2H "
                        f"at {timestamp:.3f}"
                    )
                    break

            if len(errors) >= 25:
                return checked, errors

    return checked, errors


def main():
    tasks = parse_tasks("examples/tasks_mbs1.rec")
    cfg = Config(
        capacity_bytes=768 * 1024 * 1024,
        h2d_bytes_per_us=12000,
        d2h_bytes_per_us=12000,
        chunk_size_bytes=768 * 1024,
        memory_policy="belady-demand",
    )

    print("Building schedule...", flush=True)
    result = schedule(tasks, cfg)

    checked, errors = check_versions(tasks, cfg, result)

    print(f"Checked D2H transfers: {checked}")
    if errors:
        print(f"FAIL: {len(errors)} issues shown")
        for error in errors:
            print(" -", error)
        raise SystemExit(1)

    print("PASS: no D2H/Compute or residency conflicts")


if __name__ == "__main__":
    main()
