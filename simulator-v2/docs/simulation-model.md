# Simulation Model

## Input traces

The simulator reads NNTile .rec traces with tasks, data accesses,
dependencies, and recorded execution durations.

StartTime and EndTime are expressed in seconds.
Compute durations are converted to microseconds:

    duration_us = (end_time - start_time) * 1000000

## Compute ordering

Tasks use deterministic lexicographical topological sorting
with NetworkX, based on DependsOn relationships.

This order may differ from the original recorded execution order.

## Simulation resources

The event-driven model has:
- One Compute lane
- One H2D transfer lane
- One D2H transfer lane
- GPU memory with configurable capacity

## LRU demand-only baseline

The lru-demand policy:
- Loads data only when required by Compute.
- Does not perform speculative prefetch.
- Updates LRU recency at Compute accesses.
- Protects pinned objects against eviction.
- Allocates output-only W data without H2D.
- Writes back dirty live data when necessary.
- May discard dirty data that is no longer needed.
- Ignores WontUse control records.

LRU eviction does not use future access information.

## Belady demand-only policy

The `belady-demand` policy uses the same sequential demand-only execution model as `lru-demand`, without speculative prefetch.

The only difference between these policies is the eviction victim selection.

### Future-use information

Before simulation, the ordered Compute task sequence is scanned to construct a list of future task indices for each data handle.

Before preparing each task, its accesses are removed from the future-use lists. Therefore, each remaining list contains only future accesses.

When GPU memory must be freed, Belady:

1. Excludes objects required by the current Compute task.
2. Determines the next-use index of each eligible resident object.
3. Evicts the object whose next use is farthest in the future.
4. Gives objects with no future uses the highest eviction priority.
5. Repeats eviction until sufficient memory is available.

Ties are resolved deterministically using the data handle.

### Data handling

- Output-only `W` accesses allocate GPU memory without H2D.
- Dirty objects with future uses require D2H before eviction.
- Dirty objects without future uses may be discarded without D2H.
- Objects required by the current task are protected from eviction.
- Multiple objects may be evicted to accommodate a larger allocation.

### Limitations

This implementation uses the classical farthest-next-use eviction rule.

Classical Belady is optimal for minimizing cache misses in the standard equal-size-object caching model. This optimality does not generally extend to variable-size objects, dirty write-back costs, or minimizing total transfer time.

The current policy does not consider object sizes or transfer costs when ranking eviction candidates.

Size-aware and cost-aware eviction policies are future research directions.

## LRU vs Belady experiments

Experiments were performed on `tasks_mbs1.rec` with:

- 5,484 Compute tasks
- Deterministic topological Compute ordering
- H2D bandwidth: 12 decimal GB/s
- D2H bandwidth: 12 decimal GB/s
- No speculative prefetch
- GPU memory capacities: 1,349 MiB and 2,048 MiB

Total Compute duration is 247.162272 seconds.

### Results

| GPU memory | Policy | Makespan (s) | H2D (MiB) | D2H (MiB) | Compute idle (s) |
|---|---|---:|---:|---:|---:|
| 1349 MiB | LRU | 249.489330 | 12178.341 | 14452.729 | 2.327058 |
| 1349 MiB | Belady | 249.057322 | 9285.853 | 12401.272 | 1.895050 |
| 2048 MiB | LRU | 249.350971 | 10877.472 | 14170.199 | 2.188699 |
| 2048 MiB | Belady | 248.442663 | 5907.451 | 8745.465 | 1.280391 |

### Observations

At 1349 MiB, Belady reduces H2D traffic by approximately 23.8%, D2H traffic by 14.2%, and Compute idle time by 18.6%.

At 2048 MiB, Belady reduces H2D traffic by approximately 45.7%, D2H traffic by 38.3%, and Compute idle time by 41.5%.

Makespan improvements are smaller because Compute dominates the simulated execution time.

### Compute idle interpretation

For the sequential demand-only policies, Compute does not overlap with H2D or D2H.

Therefore:

`makespan = Compute duration + H2D duration + D2H duration`

Compute idle is measured as gaps between Compute intervals, including any initial delay before the first Compute task.

For all four experiments, the sum of Compute idle intervals agrees with total transfer duration to the displayed numerical precision.

These results characterize the simulator's demand-only model, not measured StarPU execution performance.

## Legacy baseline

The legacy-baseline policy preserves an earlier experimental
implementation with lookahead-one prefetch and a different
eviction strategy.

Comparing it with lru-demand does not isolate the effect of
prefetch or eviction independently.

## Limitations

- This is a research prototype, not an exact StarPU model.
- Dependencies come from DependsOn fields.
- Additional data hazards are not inferred automatically.
- NNTile access-mode and data-version semantics need validation.
- Classical Belady is implemented; size-aware and cost-aware
  eviction policies remain future work.
- Accurate GPU memory occupancy plots require timestamped
  allocation and release events.
