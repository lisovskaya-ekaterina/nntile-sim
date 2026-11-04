
"""Сравнение резидентных диапазонов GPU для двух размеров чанков."""

import csv
from collections import defaultdict
from pathlib import Path

MIB = 2**20


def read_states(path, chunk_mib):
    chunk_bytes = chunk_mib * MIB
    resident = {}
    states = []
    pending_drop = None

    with open(path, newline="") as f:
        for row_number, event in enumerate(csv.DictReader(f), start=2):
            lane = event["lane"]
            action = event["action"]
            handle = event["handle"]
            used = int(event["used_bytes_after"])

            if pending_drop is not None:
                assert (
                    lane == "Memory"
                    and action == "DROP"
                    and handle == pending_drop
                ), (path, row_number, "Ожидался DROP", pending_drop)
                pending_drop = None
                continue

            if (lane, action) in {
                ("Memory", "ALLOC"),
                ("H2D", "LOAD"),
            }:
                assert handle not in resident, (
                    path, row_number, "Повторная загрузка", handle
                )

                current_used = sum(resident.values())
                size = used - current_used

                assert 0 < size <= chunk_bytes, (
                    path, row_number, "Некорректный размер чанка", size
                )
                resident[handle] = size

            elif (lane, action) == ("D2H", "UNLOAD"):
                assert handle in resident, (
                    path, row_number, "UNLOAD отсутствующего чанка", handle
                )
                del resident[handle]
                pending_drop = handle

            elif (lane, action) == ("Memory", "DROP"):
                assert handle in resident, (
                    path, row_number, "DROP отсутствующего чанка", handle
                )
                del resident[handle]

            assert sum(resident.values()) == used, (
                path, row_number, "Несовпадение занятости GPU"
            )

            if lane == "Compute":
                ranges = defaultdict(list)

                for chunk_key, size in resident.items():
                    original_handle, index = chunk_key.rsplit(":", 1)
                    start = int(index) * chunk_bytes
                    ranges[original_handle].append((start, start + size))

                normalized = {}

                for original_handle, intervals in ranges.items():
                    intervals.sort()
                    merged = []

                    for start, end in intervals:
                        if merged and start <= merged[-1][1]:
                            merged[-1] = (
                                merged[-1][0],
                                max(merged[-1][1], end),
                            )
                        else:
                            merged.append((start, end))

                    normalized[original_handle] = tuple(merged)

                states.append((
                    event["job_id"],
                    event["action"],
                    normalized,
                    used,
                ))

    assert pending_drop is None, (path, "Незавершённый UNLOAD")
    return states


def main():
    base = Path("results/milestone5_3")

    a = read_states(
        base / "belady_4mib_exact_events.csv", 4
    )
    b = read_states(
        base / "belady_8mib_exact_events.csv", 8
    )

    assert len(a) == len(b) == 5484

    occupancy_differences = 0
    residency_differences = 0
    first_difference = None

    for i, (left, right) in enumerate(zip(a, b)):
        job_a, action_a, ranges_a, used_a = left
        job_b, action_b, ranges_b, used_b = right

        assert (job_a, action_a) == (job_b, action_b)

        if used_a != used_b:
            occupancy_differences += 1

        if ranges_a != ranges_b:
            residency_differences += 1

            if first_difference is None:
                first_difference = (i, left, right)

    print("Compute-задач:", len(a))
    print("Различий в занятости:", occupancy_differences)
    print("Различий в резидентных диапазонах:", residency_differences)

    if first_difference is None:
        print("Резидентные диапазоны совпадают")
        return

    i, left, right = first_difference
    job_id, action, ranges_a, used_a = left
    _, _, ranges_b, used_b = right

    print("\nПервое различие в резидентных данных:")
    print("Позиция Compute:", i)
    print("job_id:", job_id)
    print("Операция:", action)
    print("GPU 4 MiB:", used_a / MIB, "MiB")
    print("GPU 8 MiB:", used_b / MIB, "MiB")

    different_handles = sorted(
        handle
        for handle in ranges_a.keys() | ranges_b.keys()
        if ranges_a.get(handle) != ranges_b.get(handle)
    )

    print("Хэндлов с различиями:", len(different_handles))

    for handle in different_handles[:10]:
        print("\nХэндл:", handle)
        print("  4 MiB:", ranges_a.get(handle, ()))
        print("  8 MiB:", ranges_b.get(handle, ()))


if __name__ == "__main__":
    main()
