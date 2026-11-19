# Development roadmap — working proposal (2026-11-19)

This is a proposed plan reconstructed from the current documentation and completed experiments, **not** an originally approved roadmap.

## Completed

- Trace parser, deterministic task order, capacity/dirty handling, LRU and classical Belady.
- Chunk-aware demand-only simulation; fixed-slot and exact-size allocation explored in Milestones 5.x.
- Event-driven handle-level prefetch depth=1 with H2D/Compute and D2H/Compute overlap (Milestone 6.2).
- Microbenchmarks and NNTile-trace comparison (Milestone 6.3.1; commit `0f956f4`).

## Priority order

1. **7.1 — Handle-size characterization.** Reproducible script and report for the `.rec` traces; measure size distribution by unique handle and by accesses, working-set sizes, and fixed-slot slack by candidate chunk size. Do not optimize on a single metric.
2. **7.2 — Data-driven global chunk-size recommendation.** Compare candidate sizes under both allocation modes and LRU/Belady, with memory overhead, H2D/D2H and makespan. Define objective and constraints explicitly; validate on both mbs1 and mbs4.
3. **7.3 — Chunk-aware event-driven prefetch.** Integrate prefetch depth=1 with chunk residency and correct dirty handling; maintain safety invariants and regression tests.
4. **8 — Scheduler extension.** Prefetch depth >1 and scheduling/backpressure, if experiments justify the complexity.
5. **9 — NNTile semantic validation.** Access modes, dependencies and data-version assumptions; some checks should run in parallel with stages 7.x.
6. **10 — Size-aware/cost-aware Belady.** Core research objective: evaluate variable-size eviction and writeback costs, with demand-only baselines and then prefetch.

## Deferred diagnostic work

Detailed repeated-H2D and overlap attribution from Milestone 6.3.1 is deferred until needed for algorithm design or correctness debugging. Current prefetch gains are modest, and increased H2D traffic is an observation, not proof of a bug.

## Release discipline

For each milestone: one coherent implementation batch, regression tests, reproducible experiment (when relevant), documentation update, then commit.
