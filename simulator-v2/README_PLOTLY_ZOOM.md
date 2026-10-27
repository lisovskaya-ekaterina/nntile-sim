# Plotly timeline: zoom and Compute stalls

Replace only `src/nntile_sim/plotting.py` in the existing simulator-v2 project.
Optionally add `tests/test_plotting_zoom.py`.
No change to the engine, policies, or simulation results.

From the repository root:

```bash
python -m pytest -q
nntile-plot events_lru_1349.csv --output events_lru_1349_zoom.html
nntile-plot events_lru_2048.csv --output events_lru_2048_zoom.html
```

For a custom initial window (milliseconds):

```bash
nntile-plot events_lru_1349.csv --window-ms 500 --output zoom500.html
```

The HTML opens centered on the largest Compute stall. Buttons: largest stall, first 200 ms, full run. The lower chart plots stall start time vs stall duration. Plotly zoom/pan and hover remain available.

Future `nntile-sim ... --plot-html output.html` runs automatically use the new plotting module. No need to rerun simulation for existing CSVs.

Memory occupancy is not plotted: the current CSV's `used_bytes_after` does not give a reliable complete allocation/free timeline.
