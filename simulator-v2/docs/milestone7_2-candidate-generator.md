# Milestone 7.2 — Graph-independent chunk candidate generation

Run from repository root:

```bash
python simulator-v2/scripts/optimize_chunk_candidates.py \
  simulator-v2/results/milestone7_1/tasks_mbs1/handles.csv \
  simulator-v2/results/milestone7_1/tasks_mbs4/handles.csv \
  --output-dir simulator-v2/results/milestone7_2
```

Outputs per trace: `chunk_plan.csv`, `candidates.csv`, `summary.json`.

**Important scientific limitation:** If chunk size may be chosen freely per handle,
then choosing `chunk_size_bytes == size_bytes` always achieves one chunk and zero
fixed-slot waste. Thus a per-handle waste+chunk-count objective is **degenerate**:
without additional constraints or a workload-sensitive eviction/transfer model,
"adaptive chunking" will choose *no splitting*. This is an honest baseline,
not evidence that chunking is useless. Candidate generation is useful to supply
layouts to later event-driven evaluation; the generator does not optimize makespan.

The candidates are generated from target chunk counts, not hand-picked KiB
thresholds, so they apply to any trace and include small handles automatically.
The reported total reserved bytes assumes all handles simultaneously resident;
it is not a measured GPU peak. Access-weighted chunks are a proxy for per-access
management work, not actual number of GPU transfers.

**Next milestone:** expose per-handle chunk sizes in the engine, measure real
GPU occupancy, H2D/D2H, eviction counts and makespan, and compare against
one-chunk-per-handle. For nontrivial selection, search under constraints such as
shared size classes/allocator bins, per-transfer overhead, or partial access
semantics (only if the trace actually supports partial accesses).
