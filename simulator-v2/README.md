# NNTile GPU Memory Simulator

Experimental event-driven simulator for GPU memory management
on NNTile task traces.

## Features

- Deterministic topological Compute ordering
- Independent Compute, H2D and D2H lanes
- GPU memory capacity and dirty data handling
- LRU demand-only and legacy baseline policies
- Interactive Plotly visualization

## Installation

From the simulator-v2 directory:

    python -m pip install -e .
    python -m pytest -q

## Run

    mkdir -p results
    nntile-sim ../examples/tasks_mbs1.rec --gpu-memory-mib 1349 --h2d-gbps 12 --d2h-gbps 12 --memory-policy lru-demand --events-csv results/events_lru_1349.csv --plot-html results/events_lru_1349.html

## Plot an existing CSV

    nntile-plot results/events_lru_1349.csv --output results/events_lru_1349.html

## Documentation

- [Simulation model](docs/simulation-model.md)
- [Visualization](docs/visualization.md)

This is a research prototype, not an exact reproduction of StarPU.
Generated results/ files are ignored by Git.
