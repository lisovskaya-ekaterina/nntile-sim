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


@pytest.mark.parametrize("policy", ["lru-demand", "belady-demand", "legacy-baseline"])
def test_prefetch_not_implemented_yet(policy):
    config = make_config(
        memory_policy=policy,
        prefetch_depth=1,
    )
    simulator = Simulator([], config)

    with pytest.raises(SimulationError, match="Prefetch is not implemented"):
        simulator.run()
