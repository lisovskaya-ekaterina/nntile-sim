# NNTile GPU Memory Simulator

Experimental event-driven simulator for GPU memory management
on NNTile task traces.

## Features

- Deterministic topological Compute ordering
- Independent Compute, H2D and D2H lanes
- GPU memory capacity and dirty data handling
- LRU demand-only, Belady demand-only and legacy baseline policies
- Interactive Plotly visualization
- Optional chunk-aware demand-only simulation (`fixed-slot` and `exact-size`)
- Event-driven prefetch depth=1 for non-chunked LRU/Belady
- Validation and reproducible micrograph/NNTile benchmarks

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

## Prefetch and benchmark status (November 2026)

The engine supports `Config(prefetch_depth=1)` for non-chunked `lru-demand` and `belady-demand`. This is currently exercised through Python benchmark scripts, not a documented `nntile-sim` CLI flag. Prefetch overlaps transfers with Compute when scheduling permits; chunked demand-only remains sequential.

From the repository root:

```bash
python simulator-v2/scripts/benchmark_prefetch.py --output simulator-v2/docs/prefetch_benchmark.csv
python simulator-v2/scripts/benchmark_nntile_graph.py examples/tasks_mbs1.rec --capacities-mib 1024 1536 2048 --h2d-gbps 12 --d2h-gbps 12 --output simulator-v2/results/milestone6_3_1/tasks_mbs1_benchmark.csv
python simulator-v2/scripts/benchmark_nntile_graph.py examples/tasks_mbs4.rec --capacities-mib 1024 1536 2048 --h2d-gbps 12 --d2h-gbps 12 --output simulator-v2/results/milestone6_3_1/tasks_mbs4_benchmark.csv
```

The checked-in results are in `docs/milestone6_3_1/`. Across 24 simulated configurations, prefetch reduced makespan modestly (approximately 0.06–0.38%); for `tasks_mbs1.rec`, H2D traffic increased by roughly 59–71%. These are simulated, not measured GPU timings.

## Documentation

- [Simulation model](docs/simulation-model.md)
- [Visualization](docs/visualization.md)
- [Development roadmap](docs/development-roadmap.md)
- [Handle sizes and chunk selection plan](docs/handle-size-chunk-plan.md)

## Memory policies

- `lru-demand`: demand-only transfers with LRU eviction.
- `belady-demand`: demand-only transfers with farthest-next-use eviction.
- `legacy-baseline`: earlier experimental policy with lookahead-one prefetch.

In demand-only mode, LRU and Belady use the same sequential execution model.
They differ only in eviction victim selection.

See [Simulation model](docs/simulation-model.md) for details and
experimental results.

This is a research prototype, not an exact reproduction of StarPU.
Generated results/ files are ignored by Git.
