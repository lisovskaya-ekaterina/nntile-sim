# NNTile GPU Memory Simulator

Experimental event-driven simulator for GPU memory management
on NNTile task traces.

## Features

- Deterministic topological Compute ordering
- Independent Compute, H2D and D2H lanes
- GPU memory capacity and dirty data handling
- LRU demand-only, Belady demand-only and legacy baseline policies
- Interactive Plotly visualization

## Installation

From the simulator-v2 directory:

    python -m pip install -e .
    python -m pytest -q

## Run

    mkdir -p results

    nntile-sim ../examples/tasks_mbs1.rec \
    --gpu-memory-mib 1349 \
    --h2d-gbps 12 \
    --d2h-gbps 12 \
    --memory-policy lru-demand \
    --events-csv results/events_lru_1349.csv \
    --plot-html results/events_lru_1349.html

    nntile-sim ../examples/tasks_mbs1.rec \
    --gpu-memory-mib 1349 \
    --h2d-gbps 12 \
    --d2h-gbps 12 \
    --memory-policy belady-demand \
    --events-csv results/events_belady_1349.csv \
    --plot-html results/events_belady_1349.html

## Plot an existing CSV

    nntile-plot results/events_lru_1349.csv --output results/events_lru_1349.html

## Documentation

- [Simulation model](docs/simulation-model.md)
- [Visualization](docs/visualization.md)

## Memory policies

- `lru-demand`: demand-only transfers with LRU eviction.
- `belady-demand`: demand-only transfers with farthest-next-use eviction.
- `legacy-baseline`: earlier experimental policy with lookahead-one prefetch.

LRU and Belady use the same sequential demand-only execution model.
They differ only in eviction victim selection.

See [Simulation model](docs/simulation-model.md) for details and
experimental results.

This is a research prototype, not an exact reproduction of StarPU.
Generated results/ files are ignored by Git.
