# Milestone 7.4 — Simulation-guided adaptive fixed-slot search

This experimental optimizer works **only** with `chunk_allocation='fixed-slot'`, `prefetch_depth=0`, and demand-only LRU/Belady.

## Algorithm

1. Parse any valid NNTile `.rec` trace; reject inconsistent handle sizes.
2. Group handles by their exact size in bytes; prioritize groups by access frequency.
3. Start with a no-chunking baseline.
4. For each size group, generate slot sizes as `ceil(size / target_chunk_count)` for target counts `1,2,3,4,6,8,12,16` (configurable).
5. Evaluate each one-group change with the real simulator, accepting a strictly better result. Repeat for a bounded number of passes.
6. Optimize makespan first, then total transferred bytes, then peak GPU reservation as tie-breakers.
7. Write `summary.json`, `chunk_plan.json`, and `search_history.csv`.

The search is greedy, **not globally optimal**. `--max-evaluations` limits simulation runs; if the budget is exhausted, results may be sensitive to group ordering. The algorithm can fail to find a feasible configuration if multiple groups must change simultaneously. Chunk counts are search hyperparameters, not hardcoded KiB thresholds. Plans are specific to the trace, capacity, and eviction policy.

## Important scientific limitations

- This is a sequential demand-only comparison. Event-driven prefetch is not combined with chunking.
- The no-chunking baseline runs through `_run_demand`, whereas adaptive plans run through `_run_chunked_demand`; their semantic equivalence for one-chunk-per-handle should be independently validated before drawing performance conclusions.
- Full-handle Compute access means that splitting does not reduce data required by a task. It changes eviction granularity and may increase transfers.
- Makespan is simulated, not measured GPU execution time. Chunk-management overhead is not explicitly modeled.
- `fixed-slot` reserves full slot bytes, including the last partial chunk. Transfers use actual payload sizes.
- A returned plan contains only changed handles; unlisted handles remain whole.

## Example

From repository root, with the editable package installed:

```bash
python simulator-v2/scripts/optimize_adaptive_fixed_slot.py examples/tasks_mbs1.rec \
  --capacity-mib 1349 2048 --max-evaluations 40 \
  --output-dir simulator-v2/results/milestone7_4
```

Repeat for `examples/tasks_mbs4.rec`. Use `--policy lru-demand` for the LRU experiment.
