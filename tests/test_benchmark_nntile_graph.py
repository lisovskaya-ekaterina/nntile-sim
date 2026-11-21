"""Smoke tests for the real-trace experiment driver."""
import importlib.util
from pathlib import Path

from nntile_sim.model import Task, Access

SCRIPT = Path(__file__).resolve().parents[1] / 'scripts' / 'benchmark_nntile_graph.py'
spec = importlib.util.spec_from_file_location('benchmark_nntile_graph', SCRIPT)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def task(jid, accesses, duration_us=10, deps=()):
    return Task(str(jid), str(jid), None,
                tuple(Access(*a) for a in accesses),
                0, duration_us / 1_000_000, None, tuple(str(d) for d in deps))


def test_benchmark_three_policies_per_capacity():
    tasks = [task(1, [('a', 40, 'R')], 40),
             task(2, [('b', 40, 'R')], 10, (1,))]
    rows, event_rows = module.benchmark(
        tasks, 'test.rec', [0.0001], 0.01, 0.01,
        chunk_mib=0.00001, compute_order='topological',
    )
    assert len(rows) == 3
    assert {r['policy'] for r in rows} == set(module.POLICIES)
    assert all(r['status'] == 'ok' for r in rows), rows
    assert all(float(r['speedup_vs_lru']) > 0 for r in rows)
    assert event_rows


def test_benchmark_skips_impossible_capacity():
    tasks = [task(1, [('a', 80, 'R')])]
    rows, event_rows = module.benchmark(
        tasks, 'test.rec', [0.00001], 0.01, 0.01,
        chunk_mib=0.00001, compute_order='topological',
    )
    assert len(rows) == 3
    assert all(r['status'] == 'skipped' for r in rows)
    assert not event_rows


def test_task_footprint_counts_handle_once():
    tasks = [task(1, [('a', 40, 'R'), ('a', 40, 'R')])]
    assert module.minimum_task_footprint(tasks) == 40
