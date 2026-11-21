import pytest
from nntile_sim.core.engine import Config, Simulator, SimulationError
from nntile_sim.model import Access, Task


def task(i, handle, size, mode='R'):
    return Task(str(i), f'task_{i}', None, (Access(handle, size, mode),),
                float(i), float(i+1), i, ())


def config(**kw):
    return Config(capacity_bytes=12, h2d_bytes_per_us=1,
                  d2h_bytes_per_us=1, compute_order='trace',
                  memory_policy='lru-demand', **kw)


def test_adaptive_partial_eviction():
    tasks = [task(0, 'A', 10), task(1, 'B', 4), task(2, 'A', 10)]
    r = Simulator(tasks, config(chunk_sizes_by_handle=(('A', 4), ('B', 4)))).run()
    assert r.completed_jobs == 3
    assert r.h2d_bytes == 16
    assert r.peak_bytes <= 12


def test_adaptive_exact_size():
    tasks = [task(0, 'A', 10)]
    r = Simulator(tasks, config(chunk_sizes_by_handle=(('A', 4),),
                                chunk_allocation='exact-size')).run()
    assert r.peak_bytes == 10


def test_fallback_to_full_handle():
    tasks = [task(0, 'A', 8), task(1, 'B', 4)]
    r = Simulator(tasks, config(chunk_sizes_by_handle=(('A', 4),))).run()
    assert r.completed_jobs == 2
    assert r.peak_bytes <= 12


def test_global_fallback():
    tasks = [task(0, 'A', 8), task(1, 'B', 4)]
    r = Simulator(tasks, config(chunk_size_bytes=4,
                                chunk_sizes_by_handle=(('A', 2),))).run()
    assert r.completed_jobs == 2


@pytest.mark.parametrize('plan', [(("A", 0),), (("A", -1),),
                                  (("A", 4), ("A", 8)), (("", 4),),
                                  (("A", 1.5),)])
def test_bad_plan(plan):
    with pytest.raises(ValueError):
        config(chunk_sizes_by_handle=plan)


def test_unknown_handle():
    with pytest.raises(SimulationError, match='unknown handles'):
        Simulator([task(0, 'A', 4)],
                  config(chunk_sizes_by_handle=(('B', 2),))).run()


def test_legacy_rejected():
    with pytest.raises(ValueError):
        Config(12, 1, 1, memory_policy='legacy-baseline',
               chunk_sizes_by_handle=(('A', 4),))
