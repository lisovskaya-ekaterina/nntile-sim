# Handle sizes and intelligent chunk-size selection — research plan

## Goal

Choose a chunk size using observed NNTile trace characteristics and simulation cost, rather than arbitrary values. Start with a **global** chunk size; consider adaptive per-handle sizes only after measuring the baseline.

## Measurements

1. Count unique handles and accesses; validate that each handle has consistent declared size across tasks, or report inconsistencies.
2. Report handle-size percentiles (min, p10, p25, median, p75, p90, p95, p99, max), total unique bytes, and an access-weighted distribution.
3. Show log-scale histogram or size buckets; separate frequently reused handles from one-use handles.
4. For candidate chunk size `C`, compute `n = ceil(S/C)`, fixed-slot reservation `n*C`, and slack `n*C-S` per handle. Aggregate both unique-handle and access-weighted views; avoid treating the sum of all handle reservations as simultaneous GPU occupancy.
5. For exact-size allocation, final-chunk reservation is its payload size, so fixed-slot rounding slack is zero; chunk count and management granularity still vary.
6. Evaluate LRU and Belady under the **same** trace, compute order, GPU capacities and bandwidths. Compare makespan, H2D, D2H, peak reserved bytes, and chunk counts.

## Candidate selection

Start with a reproducible grid (for example 1, 2, 4, 8, 16 MiB), plus candidates suggested by size-distribution knees. Filter candidates by capacity feasibility and explicit memory-overhead constraints, then rank feasible candidates by simulated makespan and traffic. Report the Pareto frontier if no candidate dominates. Do not claim an optimal chunk size from percentiles alone.

## Definition of done

- Script with CLI accepting one or more `.rec` traces, outputting machine-readable statistics.
- Documented treatment of size inconsistencies and repeated handles.
- Candidate comparison with fixed-slot/exact-size and both memory policies.
- Tests for exact multiples, partial final chunks, repeated accesses, and inconsistent sizes.
- Results for `examples/tasks_mbs1.rec` and `examples/tasks_mbs4.rec`; clear distinction between simulation and measured GPU performance.
