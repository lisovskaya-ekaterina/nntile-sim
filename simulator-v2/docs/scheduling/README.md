# Global scheduling: plan and schedule formats

Status: draft (Milestone 8.1a)

## Goal

Build an offline, trace-aware GPU memory scheduler.

The scheduler knows the complete compute trace and produces a plan
for H2D transfers, D2H transfers, memory release, and Compute.

The optimization objective is to minimize makespan while respecting
data dependencies, version correctness, transfer-lane constraints,
and GPU memory capacity.

This is not the existing one-task-lookahead prefetch mechanism.

## Two representations

### plan.json

A plan describes operations and their required dependencies.

It does not assign execution times.

Operations:
- H2D: transfer a chunk version from CPU to GPU.
- D2H: transfer a chunk version from GPU to CPU.
- Compute: execute a task using resident chunks.
- DROP: release GPU memory occupied by a chunk.

The plan may contain scheduling decisions, such as selected
evictions and transfers, without imposing a total order on all
operations.

### schedule.json

A schedule assigns start and end times to plan operations.

It records:
- the execution lane of each operation;
- the resulting memory allocation and release events;
- makespan and transfer statistics.

The schedule must be independently verifiable against the plan.

## Initial execution model

- One serial H2D lane.
- One serial D2H lane.
- One serial Compute lane.
- Different lanes may overlap.
- GPU memory is shared, with no explicit physical slot IDs.
- Fixed-slot chunk allocation.
- Initial chunk size: 768 KiB.
- Transfer duration = useful chunk bytes / transfer bandwidth.
- Compute durations are known from the trace.
- All required chunks remain resident throughout Compute.
- Writes create new chunk versions at Compute completion.
- Dirty chunks requiring preservation remain allocated until
  their D2H transfer completes.
- Clean chunks with a valid CPU copy may be dropped without D2H.

## Important modeling distinction

The useful size of a chunk and its reserved GPU memory are
different quantities.

The final chunk of a handle may contain less than 768 KiB of
useful data while still occupying a full 768 KiB GPU slot.

## Open questions

- How are final output versions identified and preserved?
- Which operation dependencies must be explicit, and which
  constraints should be checked by the execution validator?
- How should initial CPU-resident versions be represented?
- How should allocation and DROP events be represented in
  an executable plan versus a completed schedule?

These questions must be resolved before freezing the JSON schema.

## Compatibility

Existing Simulator, Event, SimulationResult, CSV export,
and prefetch behavior remain unchanged in Milestone 8.1a.
