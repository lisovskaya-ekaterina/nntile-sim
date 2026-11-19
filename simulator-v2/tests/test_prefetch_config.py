import pytest

from nntile_sim.core.engine import Config, SimulationError, Simulator


def make_config(**kwargs):
    return Config(
        capacity_bytes=1024,
        h2d_bytes_per_us=10.0,
        d2h_bytes_per_us=10.0,
        **kwargs,
    )


def test_prefetch_disabled_by_default():
    config = make_config()
    assert config.prefetch_depth == 0


@pytest.mark.parametrize("depth", [0, 1, 2, 4, 8])
def test_valid_prefetch_depth(depth):
    config = make_config(prefetch_depth=depth)
    assert config.prefetch_depth == depth


@pytest.mark.parametrize("depth", [-1, -10, 1.5, "2", None, True, False])
def test_invalid_prefetch_depth(depth):
    with pytest.raises(ValueError, match="Prefetch depth"):
        make_config(prefetch_depth=depth)


@pytest.mark.parametrize("policy", ["lru-demand", "belady-demand"])
def test_prefetch_engine_runs(policy):
    config = make_config(
        memory_policy=policy,
        prefetch_depth=1,
    )

    result = Simulator([], config).run()

    assert result.completed_jobs == 0
    assert result.makespan_us == 0
    assert result.peak_bytes == 0
    assert result.h2d_bytes == 0
    assert result.d2h_bytes == 0
    assert result.events == []


def test_legacy_prefetch_depth_not_configurable():
    config = make_config(
        memory_policy="legacy-baseline",
        prefetch_depth=1,
    )

    with pytest.raises(
        SimulationError,
        match="Prefetch depth is not configurable",
    ):
        Simulator([], config).run()


@pytest.mark.parametrize("depth", [2, 4, 8])
def test_deeper_prefetch_not_supported_yet(depth):
    config = make_config(prefetch_depth=depth)

    with pytest.raises(
        SimulationError,
        match="Only prefetch_depth=1 is supported",
    ):
        Simulator([], config).run()