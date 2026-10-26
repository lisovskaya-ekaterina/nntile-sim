import pytest
from nntile_sim.model import Task, Access
from nntile_sim.core import Simulator, Config, SimulationError, order_tasks

def task(jid, accesses, deps=(), duration=10):
    return Task(jid, jid, None, tuple(Access(*a) for a in accesses), 0, duration, None, tuple(deps))

def cfg(cap=100):
    return Config(cap, 10, 5)

def test_load_compute_and_dirty_reload():
    ts = [task('1', [('x', 40, 'R'), ('y', 40, 'W')]), task('2', [('y', 40, 'R')], ('1',))]
    result = Simulator(ts, cfg(80)).run()
    assert result.completed_jobs == 2
    assert result.h2d_bytes == 40
    assert result.peak_bytes <= 80
    assert len([e for e in result.events if e.lane == 'Compute']) == 2

def test_dirty_eviction():
    ts = [task('1', [('x', 40, 'R'), ('y', 40, 'W')]),
          task('2', [('z', 60, 'R')], ('1',)),
          task('3', [('y', 40, 'R')], ('2',))]
    result = Simulator(ts, cfg(100)).run()
    assert result.peak_bytes <= 100
    assert result.completed_jobs == 3

def test_infeasible():
    with pytest.raises(SimulationError, match='requires more'):
        Simulator([task('1', [('x', 60, 'R'), ('y', 60, 'W')])], cfg(100)).run()

def test_orders():
    a = task('a', [('x', 10, 'W')])
    b = task('b', [('x', 10, 'R')], ('a',))
    assert [t.job_id for t in order_tasks([b, a])] == ['a', 'b']
    with pytest.raises(SimulationError, match='violates'):
        order_tasks([a, b], 'custom', ('b', 'a'))

def test_prefetch_overlaps_compute():
    ts = [task('1', [('a', 40, 'R')], duration=30),
          task('2', [('b', 40, 'R')], ('1',))]
    result = Simulator(ts, cfg(100)).run()
    compute = next(e for e in result.events if e.lane == 'Compute' and e.job_id == '1')
    load = next(e for e in result.events if e.lane == 'H2D' and e.handle == 'b')
    assert max(compute.start_us, load.start_us) < min(compute.end_us, load.end_us)
    assert result.peak_bytes <= 100


def test_dirty_data_is_unloaded_before_reload():
    ts = [task('1', [('x', 40, 'W'), ('q', 20, 'R')]),
          task('2', [('y', 50, 'R'), ('z', 30, 'R')], ('1',)),
          task('3', [('x', 40, 'R')], ('2',))]
    result = Simulator(ts, cfg(100)).run()
    unload = next(e for e in result.events if e.lane == 'D2H' and e.handle == 'x')
    reload = next(e for e in result.events if e.lane == 'H2D' and e.handle == 'x')
    assert unload.end_us <= reload.start_us
    assert result.peak_bytes <= 100
    assert result.completed_jobs == 3
