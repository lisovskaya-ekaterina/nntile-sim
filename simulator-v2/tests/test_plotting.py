from nntile_sim.plotting import compute_gaps

def test_compute_gaps():
    events = [
        {'lane': 'Compute', 'start_us': 10.0, 'end_us': 20.0},
        {'lane': 'Compute', 'start_us': 30.0, 'end_us': 40.0},
        {'lane': 'H2D', 'start_us': 0.0, 'end_us': 10.0},
    ]
    assert compute_gaps(events) == [(0.0, 10.0), (20.0, 30.0)]
