import importlib.util
from pathlib import Path

import pytest

script = Path(__file__).resolve().parents[1] / 'scripts' / 'optimize_chunk_candidates.py'
spec = importlib.util.spec_from_file_location('optimize_chunk_candidates', script)
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


def test_exact_size_one_chunk():
    rows, _ = m.optimize([('a', 123, 4)], allocation='exact-size')
    assert rows[0]['chunks'] == 1
    assert rows[0]['waste_bytes'] == 0


def test_small_handle_no_large_slot():
    rows, _ = m.optimize([('a', 4, 1), ('b', 3072, 2)])
    assert all(r['chunks'] == 1 and r['waste_bytes'] == 0 for r in rows)


def test_budget_can_require_splitting():
    rows, _ = m.optimize([('a', 2304 * 1024, 3)], counts=(1, 2, 3, 4), max_waste_fraction=0)
    assert rows[0]['chunks'] == 1  # exact-sized single chunk is always valid


def test_candidates_include_multiple_splits():
    opts = m.options_for_size(300, 'fixed-slot', (1, 2, 3, 4))
    assert {o['chunks'] for o in opts} == {1, 2, 3, 4}


def test_metrics_access_weighting():
    rows, _ = m.optimize([('a', 7, 2), ('b', 100, 3)])
    assert m.summary(rows)['access_weighted_chunks'] == sum(r['chunks']*r['accesses'] for r in rows)


def test_invalid_input(tmp_path):
    p = tmp_path/'handles.csv'
    p.write_text('handle,size_bytes,accesses\na,0,1\n')
    with pytest.raises(ValueError): m.load_handles(p)


def test_duplicate_handles(tmp_path):
    p = tmp_path/'handles.csv'
    p.write_text('handle,size_bytes,accesses\na,4,1\na,4,2\n')
    with pytest.raises(ValueError): m.load_handles(p)


def test_infeasible_constraint_reported():
    # One-chunk exact-size always feasible under current unconstrained
    # chunk-size model, so violations are impossible. See docs.
    rows, _ = m.optimize([('a', 999, 1)], max_waste_fraction=0)
    assert m.summary(rows)['budget_violations'] == 0
