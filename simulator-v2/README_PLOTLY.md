# Plotly integration for NNTile simulator

Copy files into `simulator-v2/` preserving paths. Existing `engine.py` and `model.py` remain unchanged.

Install: `python -m pip install -e .`

Run simulation and create plot in one command:

```bash
nntile-sim ../examples/tasks_mbs1.rec --gpu-memory-mib 1349 --h2d-gbps 12 --d2h-gbps 12 --memory-policy lru-demand --events-csv events_lru_1349.csv --plot-html events_lru_1349.html
```

Or plot any existing events CSV without re-running simulation:

```bash
nntile-plot events_lru_1349.csv --output events_lru_1349.html
```

The interactive HTML is standalone and contains Plotly JS locally. The first subplot shows Compute/H2D/D2H timeline with hover information. The second subplot shows compute idle durations by start time. Memory occupancy is deliberately not shown because `used_bytes_after` alone is not an exact timestamped memory allocation log.
