import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from analyze_repeated_subgraphs import analyze, shared_handles
from optimize_adaptive_fixed_slot import optimize
from nntile_sim.model import Access, Task

def task(i, iteration, handle):
    return Task(str(i), 'op', iteration, (Access(handle, 4, 'R'),), float(i), float(i+1), i, ())

def test_shared_handle_analysis():
    tasks = [task(0, (0, 0), 'A'), task(1, (0, 1), 'A'), task(2, (0, 1), 'B')]
    a = analyze(tasks)
    assert shared_handles(tasks) == {'A'}
    assert len(a['groups']) == 2
    assert a['membership_group_count_histogram'] == {'1': 1, '2': 1}

def test_focus_keeps_nonshared_unsplit():
    tasks = [task(0, (0, 0), 'A'), task(1, (0, 1), 'A'), task(2, (0, 1), 'B')]
    calls = []
    r = optimize(tasks, capacity=8, bandwidth=1, max_evaluations=4,
                 focus_handles={'A'}, on_evaluation=lambda row, plan, n: calls.append(n))
    assert calls == list(range(1, len(calls)+1))
    assert 'B' not in r['plan']
