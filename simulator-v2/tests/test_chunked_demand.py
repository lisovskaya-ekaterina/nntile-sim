from nntile_sim.model import Access, Task
from nntile_sim.core.engine import Config, Simulator


def make_task(i, handle, size):
    return Task(
        job_id=str(i),
        name=f"read_{handle}",
        iteration=None,
        accesses=(Access(handle, size, "R"),),
        start_time=float(i),
        end_time=float(i + 1),
        submit_order=i,
        depends_on=(),
    )


def test_partial_chunk_eviction():
    tasks = [
        make_task(0, "A", 10),
        make_task(1, "B", 4),
        make_task(2, "A", 10),
    ]

    config = Config(
        capacity_bytes=12,
        chunk_size_bytes=4,
        h2d_bytes_per_us=1.0,
        d2h_bytes_per_us=1.0,
        compute_order="trace",
        memory_policy="lru-demand",
    )

    result = Simulator(tasks, config).run()

    assert result.completed_jobs == 3
    assert result.peak_bytes <= 12

    # A consists of three chunks (4, 4, 2).
    # B requires one 4-byte slot.
    # Only one chunk of A needs to be evicted to load B.
    # With equal LRU timestamps, the smallest payload chunk A:2
    # is evicted first (2 bytes rather than 4).
    assert result.h2d_bytes == 16

    drops = [
        e for e in result.events
        if e.lane == "Memory" and e.action == "DROP"
    ]
    assert [e.handle for e in drops] == ["A:2", "B:0"]
    assert result.d2h_bytes == 0

def test_chunked_belady_reduces_h2d():
    tasks = [
        make_task(0, "A", 8),
        make_task(1, "B", 4),
        make_task(2, "A", 8),
        make_task(3, "C", 4),
        make_task(4, "B", 4),
        make_task(5, "A", 8),
    ]

    def run(policy):
        return Simulator(
            tasks,
            Config(
                capacity_bytes=12,
                chunk_size_bytes=4,
                h2d_bytes_per_us=1.0,
                d2h_bytes_per_us=1.0,
                compute_order="trace",
                memory_policy=policy,
            ),
        ).run()

    lru = run("lru-demand")
    belady = run("belady-demand")

    assert lru.completed_jobs == 6
    assert belady.completed_jobs == 6

    assert lru.h2d_bytes == 24
    assert belady.h2d_bytes == 20

    assert belady.h2d_bytes < lru.h2d_bytes
    assert belady.peak_bytes <= 12

    belady_drops = [
        e.handle
        for e in belady.events
        if e.lane == "Memory" and e.action == "DROP"
    ]

    assert belady_drops == ["A:1", "C:0"]

def test_partial_dirty_chunk_writeback():
    tasks = [
        Task(
            job_id="0",
            name="write_A",
            iteration=None,
            accesses=(Access("A", 10, "W"),),
            start_time=0.0,
            end_time=1.0,
            submit_order=0,
            depends_on=(),
        ),
        make_task(1, "B", 4),
        make_task(2, "A", 10),
    ]

    result = Simulator(
        tasks,
        Config(
            capacity_bytes=12,
            chunk_size_bytes=4,
            h2d_bytes_per_us=1.0,
            d2h_bytes_per_us=1.0,
            compute_order="trace",
            memory_policy="lru-demand",
        ),
    ).run()

    assert result.completed_jobs == 3
    assert result.peak_bytes == 12

    # A is initially produced on GPU: no H2D.
    # B is read from CPU: 4 bytes.
    # The smallest chunk A:2 is written back and reloaded: 2 bytes.
    assert result.h2d_bytes == 6
    assert result.d2h_bytes == 2

    unloads = [
        e.handle
        for e in result.events
        if e.lane == "D2H" and e.action == "UNLOAD"
    ]
    assert unloads == ["A:2"]

    drops = [
        e.handle
        for e in result.events
        if e.lane == "Memory" and e.action == "DROP"
    ]
    assert drops == ["A:2", "B:0"]

def test_partial_last_dirty_chunk_writeback():
    tasks = [
        Task(
            job_id="0",
            name="write_A",
            iteration=None,
            accesses=(Access("A", 10, "W"),),
            start_time=0.0,
            end_time=1.0,
            submit_order=0,
            depends_on=(),
        ),
        make_task(1, "B", 4),
        make_task(2, "A", 10),
    ]

    result = Simulator(
        tasks,
        Config(
            capacity_bytes=12,
            chunk_size_bytes=4,
            h2d_bytes_per_us=1.0,
            d2h_bytes_per_us=1.0,
            compute_order="trace",
            memory_policy="belady-demand",
        ),
    ).run()

    assert result.completed_jobs == 3
    assert result.peak_bytes == 12

    # Belady sees the same next use for all chunks of A.
    # Its tie-break selects the largest key: A:2.
    unloads = [
        e.handle
        for e in result.events
        if e.lane == "D2H" and e.action == "UNLOAD"
    ]
    assert unloads == ["A:2"]

    # A:2 occupies a 4-byte GPU slot but contains only 2 bytes.
    assert result.d2h_bytes == 2

    # B:0 loads 4 bytes; A:2 later reloads 2 bytes.
    assert result.h2d_bytes == 6

    drops = [
        e.handle
        for e in result.events
        if e.lane == "Memory" and e.action == "DROP"
    ]
    assert drops == ["A:2", "B:0"]

import pytest


@pytest.mark.parametrize("policy", ["lru-demand", "belady-demand"])
def test_chunking_without_memory_pressure_matches_original(policy):
    tasks = [
        make_task(0, "A", 10),
        make_task(1, "B", 6),
        make_task(2, "A", 10),
        make_task(3, "B", 6),
    ]

    def run(chunk_size):
        return Simulator(
            tasks,
            Config(
                capacity_bytes=24,
                chunk_size_bytes=chunk_size,
                h2d_bytes_per_us=2.0,
                d2h_bytes_per_us=2.0,
                compute_order="trace",
                memory_policy=policy,
            ),
        ).run()

    original = run(None)
    chunked = run(4)

    assert original.completed_jobs == chunked.completed_jobs == 4

    assert original.h2d_bytes == chunked.h2d_bytes == 16
    assert original.d2h_bytes == chunked.d2h_bytes == 0

    assert chunked.peak_bytes == 20
    assert original.peak_bytes == 16

    assert chunked.makespan_us == pytest.approx(original.makespan_us)

def test_chunking_rejected_for_legacy_baseline():
    with pytest.raises(
        ValueError,
        match="Chunking is not supported for legacy-baseline",
    ):
        Config(
            capacity_bytes=12,
            chunk_size_bytes=4,
            h2d_bytes_per_us=1.0,
            d2h_bytes_per_us=1.0,
            memory_policy="legacy-baseline",
        )

def test_belady_prefers_smallest_chunk_on_equal_next_use():
    tasks = [
        make_task(0, "A", 10),
        make_task(1, "B", 4),
        make_task(2, "A", 10),
    ]

    result = Simulator(
        tasks,
        Config(
            capacity_bytes=12,
            chunk_size_bytes=4,
            h2d_bytes_per_us=1.0,
            d2h_bytes_per_us=1.0,
            compute_order="trace",
            memory_policy="belady-demand",
        ),
    ).run()

    drops = [
        e.handle
        for e in result.events
        if e.lane == "Memory" and e.action == "DROP"
    ]

    # All chunks of A have the same next use.
    # Belady should evict the smallest payload chunk A:2.
    assert drops == ["A:2", "B:0"]
    assert result.h2d_bytes == 16
    assert result.d2h_bytes == 0
    assert result.peak_bytes == 12