"""Unit tests for Milestone 7.1 handle statistics."""
import importlib.util
from pathlib import Path

MODULE_PATH = Path(__file__).resolve().parents[1] / 'scripts' / 'analyze_handle_sizes.py'
spec = importlib.util.spec_from_file_location('analyze_handle_sizes', MODULE_PATH)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

from nntile_sim.model import Access, Task


def task(*accesses):
    return Task('1', 'task', None, tuple(Access(*a) for a in accesses), 0, 1, None, ())


def test_collect_handles_and_access_frequency():
    sizes, counts = module.collect_handles([task(('a', 10, 'R'), ('b', 5, 'W')), task(('a', 10, 'RW'))])
    assert sizes == {'a': 10, 'b': 5}
    assert counts == {'a': 2, 'b': 1}


def test_fixed_slot_fragmentation():
    result = module.evaluate({'a': 10, 'b': 5}, {'a': 2, 'b': 1}, 4)
    assert result['chunks'] == 5
    assert result['payload_bytes'] == 15
    assert result['fixed_slot_reserved_bytes'] == 20
    assert result['fixed_slot_waste_bytes'] == 5
    assert result['exact_size_reserved_bytes'] == 15
    assert result['access_weighted_waste_bytes'] == 7


def test_inconsistent_handle_size_rejected():
    import pytest
    with pytest.raises(ValueError, match='Inconsistent size'):
        module.collect_handles([task(('a', 10, 'R')), task(('a', 11, 'R'))])


def test_nonpositive_chunk_size_rejected():
    import pytest
    with pytest.raises(ValueError):
        module.evaluate({'a': 10}, {'a': 1}, 0)
