import pytest

from nntile_sim.chunks import split_into_chunks


def test_split_with_partial_last_chunk():
    mib = 1024 * 1024
    chunks = split_into_chunks("A", 10 * mib, 4 * mib)

    assert [c.key for c in chunks] == [
        ("A", 0),
        ("A", 1),
        ("A", 2),
    ]
    assert [c.size_bytes for c in chunks] == [
        4 * mib,
        4 * mib,
        2 * mib,
    ]
    assert [c.reserved_bytes for c in chunks] == [
        4 * mib,
        4 * mib,
        4 * mib,
    ]

    assert sum(c.size_bytes for c in chunks) == 10 * mib
    assert sum(c.reserved_bytes for c in chunks) == 12 * mib


def test_exact_multiple():
    chunks = split_into_chunks("B", 8, 4)

    assert len(chunks) == 2
    assert [c.size_bytes for c in chunks] == [4, 4]
    assert [c.reserved_bytes for c in chunks] == [4, 4]


def test_smaller_than_chunk():
    chunks = split_into_chunks("C", 3, 4)

    assert len(chunks) == 1
    assert chunks[0].size_bytes == 3
    assert chunks[0].reserved_bytes == 4


@pytest.mark.parametrize(
    "size, chunk_size",
    [(0, 4), (-1, 4), (8, 0), (8, -1)],
)
def test_invalid_sizes(size, chunk_size):
    with pytest.raises(ValueError):
        split_into_chunks("A", size, chunk_size)

from nntile_sim.core.engine import Config, SimulationError, Simulator
from nntile_sim.model import Access, Task


def test_chunk_reservation_exceeds_capacity():
    task = Task(
        job_id="1",
        name="compute",
        iteration=None,
        accesses=(Access("A", 10, "R"),),
        start_time=0.0,
        end_time=0.001,
        submit_order=0,
        depends_on=(),
    )

    config = Config(
        capacity_bytes=10,
        h2d_bytes_per_us=1,
        d2h_bytes_per_us=1,
        memory_policy="lru-demand",
        chunk_size_bytes=4,
    )

    # A has 10 bytes of payload but requires three 4-byte slots.
    with pytest.raises(SimulationError, match="requires more GPU memory"):
        Simulator([task], config).run()