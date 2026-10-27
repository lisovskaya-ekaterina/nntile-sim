# Plotly: Overview + Detailed timeline

Replace only `src/nntile_sim/plotting.py`, add `tests/test_plotting_overview.py`.
Do not change `engine.py`, `model.py`, `sim_cli.py`, or the baseline policy.

Install/check:

```bash
python -m pip install -e .
python -m pytest -q
```

Create HTML from existing CSV:

```bash
nntile-plot events_lru_1349.csv --output events_lru_1349_overview.html
nntile-plot events_lru_2048.csv --output events_lru_2048_overview.html
```

Optional `--overview-bins 1000` increases resolution of aggregated overview; `--window-ms 200` controls initial detailed zoom.

Overview uses *fraction of time each lane is active* in equal-sized time bins, not counts or bytes. Each lane is calculated independently, so overlap is permitted. Detailed timeline uses unchanged raw intervals. Compute stalls are computed from gaps between Compute events. The graph does not infer GPU memory occupancy from `used_bytes_after`.
