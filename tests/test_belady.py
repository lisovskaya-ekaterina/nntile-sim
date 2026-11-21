
from nntile_sim.model import Access, Task
from nntile_sim.core.engine import Config, Simulator


def make_tasks(handles):
    """Create sequential read-only tasks with 1-byte objects."""
    tasks = []

    for i, handle in enumerate(handles):
        tasks.append(
            Task(
                job_id=str(i),
                name=f"read_{handle}",
                iteration=None,
                accesses=(Access(handle, 1, "R"),),
                start_time=float(i),
                end_time=float(i + 1),
                submit_order=i,
                depends_on=(),
            )
        )

    return tasks


def run_policy(tasks, policy):
    config = Config(
        capacity_bytes=2,
        h2d_bytes_per_us=1.0,
        d2h_bytes_per_us=1.0,
        compute_order="trace",
        memory_policy=policy,
    )
    return Simulator(tasks, config).run()


def test_belady_reduces_h2d():
    tasks = make_tasks(["A", "B", "A", "C", "B", "A"])

    lru = run_policy(tasks, "lru-demand")
    belady = run_policy(tasks, "belady-demand")

    assert lru.completed_jobs == 6
    assert belady.completed_jobs == 6

    assert lru.h2d_bytes == 5
    assert belady.h2d_bytes == 4

    assert belady.h2d_bytes < lru.h2d_bytes


def test_belady_evicts_farthest_next_use():
    tasks = make_tasks(["A", "B", "A", "C", "B", "A"])

    belady = run_policy(tasks, "belady-demand")

    drops = [
        event.handle
        for event in belady.events
        if event.lane == "Memory" and event.action == "DROP"
    ]

    assert drops == ["A", "C"]


def test_belady_writes_back_live_dirty_object():
    tasks = make_tasks(["A", "B", "C", "B", "A"])

    # A is produced on GPU; its CPU copy is invalid.
    tasks[0] = Task(
        job_id="0",
        name="write_A",
        iteration=None,
        accesses=(Access("A", 1, "W"),),
        start_time=0.0,
        end_time=1.0,
        submit_order=0,
        depends_on=(),
    )

    result = run_policy(tasks, "belady-demand")

    # When C is loaded, Belady evicts A because
    # A is needed later than B.
    assert result.h2d_bytes == 3
    assert result.d2h_bytes == 1

    unloads = [
        e.handle for e in result.events
        if e.lane == "D2H" and e.action == "UNLOAD"
    ]
    assert unloads == ["A"]



def test_belady_drops_dead_dirty_without_writeback():
    tasks = make_tasks(["A", "B", "C", "B"])

    tasks[0] = Task(
        job_id="0",
        name="write_A",
        iteration=None,
        accesses=(Access("A", 1, "W"),),
        start_time=0.0,
        end_time=1.0,
        submit_order=0,
        depends_on=(),
    )

    result = run_policy(tasks, "belady-demand")

    # A is dead, while B will be used again.
    # Belady must evict A without D2H.
    assert result.d2h_bytes == 0
    assert result.h2d_bytes == 2
    assert result.completed_jobs == 4

    drops = [
        e.handle
        for e in result.events
        if e.lane == "Memory" and e.action == "DROP"
    ]

    assert drops == ["A"]


def test_belady_multiple_evictions_for_large_object():
    sizes = {"A": 3, "B": 3, "C": 4, "D": 7}
    sequence = ["A", "B", "C", "D", "C", "A", "B"]

    tasks = [
        Task(
            job_id=str(i),
            name=f"read_{handle}",
            iteration=None,
            accesses=(Access(handle, sizes[handle], "R"),),
            start_time=float(i),
            end_time=float(i + 1),
            submit_order=i,
            depends_on=(),
        )
        for i, handle in enumerate(sequence)
    ]

    config = Config(
        capacity_bytes=10,
        h2d_bytes_per_us=1.0,
        d2h_bytes_per_us=1.0,
        compute_order="trace",
        memory_policy="belady-demand",
    )

    result = Simulator(tasks, config).run()

    drops = [
        e.handle
        for e in result.events
        if e.lane == "Memory" and e.action == "DROP"
    ]

    assert drops[:3] == ["B", "A", "C"]
    assert result.peak_bytes <= 10
    assert result.completed_jobs == 7
    assert result.d2h_bytes == 0
