import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from optimize_adaptive_fixed_slot import candidates, handle_sizes, optimize
from nntile_sim.model import Access, Task


def task(i, *accesses):
    return Task(str(i), f't{i}', None, tuple(Access(*a) for a in accesses),
                float(i), float(i + 1), i, ())


def test_candidates_graph_independent():
    assert candidates(10, (1, 2, 3, 4)) == (10, 5, 4, 3)
    assert candidates(1, (1, 2, 3)) == (1,)


def test_inconsistent_size_rejected():
    try:
        handle_sizes([task(0, ('A', 5, 'R')), task(1, ('A', 6, 'R'))])
    except ValueError:
        pass
    else:
        assert False, 'expected ValueError'


def test_fixed_slot_search_never_worse_than_baseline():
    tasks = [task(0, ('A', 10, 'R')), task(1, ('B', 4, 'R')),
             task(2, ('A', 10, 'R'))]
    result = optimize(tasks, capacity=12, policy='lru-demand', bandwidth=1,
                      counts=(1, 2, 3, 4), max_evaluations=20)
    assert result['baseline']['status'] == 'ok'
    assert result['best']['status'] == 'ok'
    assert result['best']['peak_bytes'] <= 12
    assert result['best']['makespan_us'] <= result['baseline']['makespan_us']


def test_baseline_kept_when_no_improvement():
    tasks = [task(0, ('A', 4, 'R'))]
    result = optimize(tasks, capacity=8, policy='belady-demand', bandwidth=1,
                      counts=(1, 2, 4), max_evaluations=10)
    assert result['baseline']['status'] == 'ok'
    assert result['best']['status'] == 'ok'
    assert result['best']['makespan_us'] <= result['baseline']['makespan_us']
