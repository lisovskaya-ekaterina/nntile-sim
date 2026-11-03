# Milestone 5.1 — Chunk-aware demand-only simulation

## Experimental setup

- Trace: `examples/tasks_mbs1.rec`
- Compute tasks: 5484
- Compute order: topological
- GPU capacity: 2048 MiB
- H2D bandwidth: 12 GB/s
- D2H bandwidth: 12 GB/s
- Policies: LRU demand, Belady demand
- Chunk sizes: 1, 2, 4, 8 MiB
- Execution model: sequential, demand-only, without prefetch or overlap

Each chunk reserves a full GPU slot of the configured chunk size.
The actual payload of the last chunk may be smaller.

## Results

| Policy | Chunk size | Makespan (s) | H2D (MiB) | D2H (MiB) | Events |
|---|---|---:|---:|---:|---:|
| LRU | None | 249.350971 | 10877.472 | 14170.199 | — |
| LRU | 1 MiB | 249.382015 | 11151.619 | 14251.324 | 70438 |
| LRU | 2 MiB | 249.420793 | 11526.146 | 14320.580 | 44770 |
| LRU | 4 MiB | 249.455899 | 11846.784 | 14401.697 | 28241 |
| LRU | 8 MiB | 249.516801 | 12462.279 | 14483.162 | 24008 |
| Belady | None | 248.442663 | 5907.451 | 8745.465 | — |
| Belady | 1 MiB | 248.530676 | 6341.360 | 9318.776 | 49285 |
| Belady | 2 MiB | 248.739780 | 7539.248 | 10513.903 | 34318 |
| Belady | 4 MiB | 248.879295 | 8310.678 | 11339.086 | 23095 |
| Belady | 8 MiB | 249.218092 | 10105.537 | 13421.458 | 21564 |

Chunk eviction uses LRU or Belady priority at the chunk level.
When multiple chunks have equal priority, the chunk with the
smallest actual payload is evicted first. Remaining ties are
resolved deterministically by chunk key.

For LRU, all chunks accessed by the same Compute task receive
the same last-use timestamp.

Compared with the previous chunk-level tie-breaking rule,
the updated policy produced identical aggregate results for LRU
and only small differences for Belady on this trace.

All chunked runs completed 5484 Compute tasks and passed
the chunk residency event validator.

Peak GPU memory was 2048 MiB in every chunked run.

## Observations

1. Belady produces less H2D traffic than LRU for every tested chunk size.
2. H2D and D2H traffic increase monotonically with chunk size
   for both policies in this experiment.
3. Smaller chunks produce more simulation events.
4. All tested chunked configurations have higher transfer volumes
   than their respective non-chunked baselines.
5. Makespan differences are relatively small in this sequential model.

## Interpretation and limitations

Chunked simulation reserves GPU memory in fixed-size slots,
while transfers account for actual chunk payload sizes.

Rounding to full slots may increase memory pressure and cause
additional evictions. However, the experiments do not isolate
rounding losses from changes in eviction decisions.

These results do not establish that chunking is inherently
worse than handle-level memory management.

The current model does not include asynchronous transfers,
prefetch, Compute/H2D/D2H overlap, or per-operation overhead.

## Validation

The event validator checks:

- GPU memory occupancy and capacity limits;
- chunk residency transitions;
- matching D2H UNLOAD and Memory DROP events;
- event ordering;
- completion of all 5484 Compute tasks.

The validator was also tested against an intentionally invalid
DROP of a nonresident chunk.

## Future work

- Quantify GPU memory lost to slot rounding.
- Investigate why chunked Belady transfers more data than baseline Belady.
- Add asynchronous transfers and prefetch.
- Evaluate the tradeoff between chunk granularity and operation overhead.
