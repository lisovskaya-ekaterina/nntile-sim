from nntile_sim.plotting import compute_gaps, default_window


def test_default_window_centers_longest_stall():
    events = [
        {'lane': 'Compute', 'start_us': 0.0, 'end_us': 1000000.0},
        {'lane': 'Compute', 'start_us': 1050000.0, 'end_us': 2000000.0},
        {'lane': 'Compute', 'start_us': 2020000.0, 'end_us': 3000000.0},
    ]
    assert compute_gaps(events) == [(1000000.0, 1050000.0), (2000000.0, 2020000.0)]
    start, end = default_window(events, 200)
    assert start < 1.025 < end
    assert abs((end - start) - 0.2) < 1e-8
