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
- Belady and size-aware Belady are future policies.
- Accurate GPU memory occupancy plots require timestamped
  allocation and release events.
