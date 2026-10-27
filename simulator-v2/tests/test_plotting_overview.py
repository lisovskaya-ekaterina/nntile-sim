from nntile_sim.plotting import aggregate_utilization, compute_gaps


def test_aggregate_lane_busy_fraction():
    events = [
        {'lane': 'Compute', 'start_us': 0., 'end_us': 50.},
        {'lane': 'Compute', 'start_us': 25., 'end_us': 75.},
        {'lane': 'H2D', 'start_us': 50., 'end_us': 100.},
    ]
    centers, usage = aggregate_utilization(events, bins=2)
    assert len(centers) == 2
    assert usage['Compute'] == [100., 50.]
    assert usage['H2D'] == [0., 100.]
    assert usage['D2H'] == [0., 0.]


def test_compute_gaps():
    events = [{'lane': 'Compute', 'start_us': 10., 'end_us': 20.},
              {'lane': 'Compute', 'start_us': 30., 'end_us': 40.}]
    assert compute_gaps(events) == [(0., 10.), (20., 30.)]
