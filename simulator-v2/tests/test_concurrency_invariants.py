"""Targeted regression tests for the conservative three-lane baseline.

These are behavioral tests, not a proof of all memory/version invariants.
"""
import pytest

from nntile_sim.model import Access, Task
from nntile_sim.core import Config, Simulator, SimulationError


def task(jid, accesses, deps=(), duration=10):
    return Task(jid, jid, None, tuple(Access(*a) for a in accesses),
                0, duration, None, tuple(deps))


def overlaps(a, b):
    return max(a.start_us, b.start_us) < min(a.end_us, b.end_us)


def run(tasks, cap=100, h2d=10, d2h=5):
    return Simulator(tasks, Config(cap, h2d, d2h)).run()


@pytest.mark.xfail(strict=True, reason='Baseline delays D2H until Compute ends and H2D z already finishes')
def test_h2d_and_d2h_overlap_when_both_needed():
    # x is dirty and needed again; it cannot be dropped. While x is
    # unloading, z can be loaded on the independent H2D lane.
    tasks = [
        task('1', [('x', 40, 'W'), ('q', 20, 'R')]),
        task('2', [('y', 50, 'R'), ('z', 30, 'R')], ('1',)),
        task('3', [('x', 40, 'R')], ('2',)),
    ]
    result = run(tasks)
    loads = [e for e in result.events if e.lane == 'H2D']
    unloads = [e for e in result.events if e.lane == 'D2H']
    assert unloads, 'Scenario must force a dirty eviction'
    assert any(overlaps(a, b) for a in loads for b in unloads), (
        'Independent H2D and D2H lanes failed to overlap'
    )
    assert result.peak_bytes <= 100
    assert result.completed_jobs == 3


def test_pinned_compute_input_is_not_evicted():
    tasks = [
        task('1', [('x', 40, 'RW')], duration=30),
        task('2', [('y', 70, 'R')], ('1',)),
        task('3', [('x', 40, 'R')], ('2',)),
    ]
    result = run(tasks)
    compute = next(e for e in result.events if e.lane == 'Compute' and e.job_id == '1')
    evictions = [e for e in result.events
                 if e.handle == 'x' and e.action in ('DROP', 'UNLOAD')]
    assert all(e.start_us >= compute.end_us for e in evictions)
    assert result.peak_bytes <= 100


def test_dirty_reload_follows_completed_unload():
    tasks = [
        task('1', [('x', 40, 'W'), ('q', 20, 'R')]),
        task('2', [('y', 50, 'R'), ('z', 30, 'R')], ('1',)),
        task('3', [('x', 40, 'R')], ('2',)),
    ]
    result = run(tasks)
    unload = next(e for e in result.events if e.lane == 'D2H' and e.handle == 'x')
    reload = next(e for e in result.events if e.lane == 'H2D' and e.handle == 'x')
    assert unload.end_us <= reload.start_us
    assert result.d2h_bytes >= 40
    assert result.h2d_bytes >= 40


def test_capacity_boundary_and_infeasible_task():
    exact = run([task('1', [('a', 40, 'R'), ('b', 60, 'W')])])
    assert exact.peak_bytes == 100
    with pytest.raises(SimulationError, match='requires more'):
        run([task('1', [('a', 40, 'R'), ('b', 61, 'W')])])


def test_one_lane_never_overlaps_itself():
    tasks = [
        task('1', [('x', 40, 'W'), ('q', 20, 'R')]),
        task('2', [('y', 50, 'R'), ('z', 30, 'R')], ('1',)),
        task('3', [('x', 40, 'R')], ('2',)),
    ]
    result = run(tasks)
    for lane in ('Compute', 'H2D', 'D2H'):
        lane_events = sorted((e for e in result.events if e.lane == lane),
                             key=lambda e: (e.start_us, e.end_us))
        assert all(a.end_us <= b.start_us for a, b in zip(lane_events, lane_events[1:])), lane


@pytest.mark.xfail(strict=True, reason='Baseline prepares only immediate successor; triple overlap needs deeper lookahead')
def test_all_three_lanes_overlap_future_goal():
    # The current baseline cannot generally schedule D2H for a task beyond
    # the immediate successor while Compute is running. This is a design goal.
    tasks = [
        task('1', [('a', 40, 'R')], duration=50),
        task('2', [('x', 40, 'W')], ('1',), duration=50),
        task('3', [('y', 70, 'R')], ('2',)),
        task('4', [('x', 40, 'R')], ('3',)),
    ]
    result = run(tasks)
    compute = [e for e in result.events if e.lane == 'Compute']
    loads = [e for e in result.events if e.lane == 'H2D']
    unloads = [e for e in result.events if e.lane == 'D2H']
    assert any(max(c.start_us, h.start_us, d.start_us) <
               min(c.end_us, h.end_us, d.end_us)
               for c in compute for h in loads for d in unloads)
