from nntile_sim.model import Task, Access
from nntile_sim.core import Simulator, Config
from nntile_sim.core.engine import Config, Simulator, SimulationError
import pytest


def task(jid, accesses, deps=(), duration=10):
    return Task(
        jid,
        jid,
        None,
        tuple(Access(*a) for a in accesses),
        0,
        duration,
        None,
        tuple(deps),
    )


def test_lru_prefetch_one_overlaps_compute():
    tasks = [
        task("1", [("a", 40, "R")], duration=30),
        task("2", [("b", 40, "R")], deps=("1",)),
    ]

    config = Config(
        capacity_bytes=100,
        h2d_bytes_per_us=10,
        d2h_bytes_per_us=5,
        memory_policy="lru-demand",
        prefetch_depth=1,
    )

    result = Simulator(tasks, config).run()

    compute = next(
        event for event in result.events
        if event.lane == "Compute" and event.job_id == "1"
    )

    load = next(
        event for event in result.events
        if event.lane == "H2D" and event.handle == "b"
    )

    assert max(compute.start_us, load.start_us) < min(
        compute.end_us, load.end_us
    )

    assert result.completed_jobs == 2
    assert result.peak_bytes <= config.capacity_bytes

def test_prefetch_reads_previous_gpu_write():
    tasks = [
        task("1", [("x", 40, "W")], duration=30),
        task("2", [("x", 40, "R")], deps=("1",)),
    ]

    config = Config(
        capacity_bytes=100,
        h2d_bytes_per_us=10,
        d2h_bytes_per_us=5,
        memory_policy="lru-demand",
        prefetch_depth=1,
    )

    result = Simulator(tasks, config).run()

    assert result.completed_jobs == 2
    assert result.h2d_bytes == 0

    computes = [
        event for event in result.events
        if event.lane == "Compute"
    ]

    assert len(computes) == 2
    assert computes[0].end_us <= computes[1].start_us

def test_prefetch_does_not_load_stale_version():
    tasks = [
        task("1", [("x", 40, "R")], duration=10),
        task("2", [("x", 40, "RW")], deps=("1",), duration=30),
        task("3", [("x", 40, "R")], deps=("2",)),
    ]

    config = Config(
        capacity_bytes=100,
        h2d_bytes_per_us=10,
        d2h_bytes_per_us=5,
        memory_policy="lru-demand",
        prefetch_depth=1,
    )

    result = Simulator(tasks, config).run()

    loads = [
        event for event in result.events
        if event.lane == "H2D" and event.handle == "x"
    ]

    assert result.completed_jobs == 3
    assert len(loads) == 1
    assert result.h2d_bytes == 40

    computes = {
        event.job_id: event
        for event in result.events
        if event.lane == "Compute"
    }

    assert computes["1"].end_us <= computes["2"].start_us
    assert computes["2"].end_us <= computes["3"].start_us

def test_prefetch_waits_when_gpu_is_full():
    tasks = [
        task("1", [("a", 40, "R")], duration=30),
        task("2", [("b", 60, "R")], deps=("1",)),
    ]

    config = Config(
        capacity_bytes=80,
        h2d_bytes_per_us=10,
        d2h_bytes_per_us=5,
        memory_policy="lru-demand",
        prefetch_depth=1,
    )

    result = Simulator(tasks, config).run()

    assert result.completed_jobs == 2
    assert result.peak_bytes <= config.capacity_bytes
    assert result.h2d_bytes == 100

    computes = {
        event.job_id: event
        for event in result.events
        if event.lane == "Compute"
    }

    load_b = next(
        event for event in result.events
        if event.lane == "H2D" and event.handle == "b"
    )

    assert load_b.start_us >= computes["1"].end_us

def test_prefetch_lru_evicts_least_recently_used():
    tasks = [
        task("1", [("a", 40, "R")]),
        task("2", [("b", 40, "R")], deps=("1",)),
        task("3", [("a", 40, "R")], deps=("2",)),
        task("4", [("c", 40, "R")], deps=("3",)),
    ]

    config = Config(
        capacity_bytes=80,
        h2d_bytes_per_us=10,
        d2h_bytes_per_us=5,
        memory_policy="lru-demand",
        prefetch_depth=1,
    )

    result = Simulator(tasks, config).run()

    drops = [
        event.handle
        for event in result.events
        if event.lane == "Memory" and event.action == "DROP"
    ]

    assert result.completed_jobs == 4
    assert result.peak_bytes <= config.capacity_bytes
    assert drops == ["b"]

def test_prefetch_belady_evicts_farthest_next_use():
    tasks = [
        task("1", [("a", 40, "R")]),
        task("2", [("b", 40, "R")], deps=("1",)),
        task("3", [("a", 40, "R")], deps=("2",)),
        task("4", [("c", 40, "R")], deps=("3",)),
        task("5", [("b", 40, "R")], deps=("4",)),
        task("6", [("a", 40, "R")], deps=("5",)),
    ]

    config = Config(
        capacity_bytes=80,
        h2d_bytes_per_us=10,
        d2h_bytes_per_us=5,
        memory_policy="belady-demand",
        prefetch_depth=1,
    )

    result = Simulator(tasks, config).run()

    drops = [
        event.handle
        for event in result.events
        if event.lane == "Memory" and event.action == "DROP"
    ]

    assert result.completed_jobs == 6
    assert result.peak_bytes <= config.capacity_bytes

    # Belady should evict a, whose next use is farther away.
    assert drops[0] == "a"

def test_prefetch_writes_back_live_dirty_data():
    tasks = [
        task("1", [("a", 40, "W")]),
        task("2", [("b", 40, "R")], deps=("1",)),
        task("3", [("a", 40, "R")], deps=("2",)),
    ]

    config = Config(
        capacity_bytes=40,
        h2d_bytes_per_us=10,
        d2h_bytes_per_us=5,
        memory_policy="lru-demand",
        prefetch_depth=1,
    )

    result = Simulator(tasks, config).run()

    writebacks = [
        event for event in result.events
        if event.lane == "D2H" and event.handle == "a"
    ]

    assert result.completed_jobs == 3
    assert result.peak_bytes <= config.capacity_bytes
    assert result.d2h_bytes == 40
    assert result.h2d_bytes == 80
    assert len(writebacks) == 1

    loads_a = [
        event for event in result.events
        if event.lane == "H2D" and event.handle == "a"
    ]

    assert len(loads_a) == 1
    assert loads_a[0].start_us >= writebacks[0].end_us

def test_prefetch_d2h_keeps_gpu_memory_until_transfer_finishes():
    tasks = [
        task("1", [("a", 40, "W")]),
        task("2", [("b", 40, "R")], deps=("1",)),
        task("3", [("a", 40, "R")], deps=("2",)),
    ]

    config = Config(
        capacity_bytes=40,
        h2d_bytes_per_us=10,
        d2h_bytes_per_us=5,
        memory_policy="lru-demand",
        prefetch_depth=1,
    )

    result = Simulator(tasks, config).run()

    d2h_a = next(
        e for e in result.events
        if e.lane == "D2H" and e.handle == "a"
    )

    load_b = next(
        e for e in result.events
        if e.lane == "H2D" and e.handle == "b"
    )

    assert load_b.start_us >= d2h_a.end_us
    assert result.peak_bytes <= config.capacity_bytes

def test_prefetch_drops_dead_dirty_data_without_writeback():
    tasks = [
        task("1", [("a", 40, "W")]),
        task("2", [("b", 40, "R")], deps=("1",)),
    ]

    config = Config(
        capacity_bytes=40,
        h2d_bytes_per_us=10,
        d2h_bytes_per_us=5,
        memory_policy="lru-demand",
        prefetch_depth=1,
    )

    result = Simulator(tasks, config).run()

    assert result.completed_jobs == 2
    assert result.peak_bytes <= config.capacity_bytes
    assert result.d2h_bytes == 0

    assert any(
        e.lane == "Memory"
        and e.action == "DROP"
        and e.handle == "a"
        for e in result.events
    )

    assert not any(
        e.lane == "D2H" and e.handle == "a"
        for e in result.events
    )

def test_prefetch_rejects_task_larger_than_gpu_capacity():
    tasks = [
        task("1", [
            ("a", 40, "R"),
            ("b", 40, "R"),
        ]),
    ]

    config = Config(
        capacity_bytes=60,
        h2d_bytes_per_us=10,
        d2h_bytes_per_us=5,
        memory_policy="lru-demand",
        prefetch_depth=1,
    )

    with pytest.raises(
        SimulationError,
        match="No evictable data|exceeds GPU capacity",
    ):
        Simulator(tasks, config).run()