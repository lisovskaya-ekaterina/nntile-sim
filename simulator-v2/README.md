# NNTile simulator v2 — first milestone

This package currently contains **only the trace reader and typed task model**.
It does **not** yet simulate memory transfers or execute scheduling policies.

Run without installation:

```bash
PYTHONPATH=src python -m nntile_sim.cli /path/to/tasks_mbs1.rec
```

Or install with `pip install -e .` and run `nntile-trace /path/to/tasks_mbs1.rec`.

Tasks are ordered by recorded StartTime, not by topological sorting. The
recorded durations are retained in microseconds (convert explicitly in the
simulation engine). A trace contains one observed run; it is not a guarantee
that the observed order is the only dependency-valid order.

Next milestones: validate dependency/order anomalies and access-mode semantics;
then implement memory state transitions, event-driven Compute/H2D/D2H, policies,
and synchronized Plotly timelines.

## Milestone 2 (experimental baseline)

`nntile_sim.core` provides a conservative event-based GPU-memory simulation:

```python
from nntile_sim.parser import parse_tasks
from nntile_sim.core import Simulator, Config

tasks = parse_tasks('tasks_mbs1.rec')
result = Simulator(tasks, Config(
    capacity_bytes=1349 * 1024**2,
    h2d_bytes_per_us=12000,  # 12 GB/s in bytes/microsecond
    d2h_bytes_per_us=12000,
    compute_order='topological',
)).run()
print(result.makespan_us, result.peak_bytes, result.h2d_bytes, result.d2h_bytes)
```

**Research limitations:** This is a correctness-oriented prototype, NOT yet a
validated performance model. It does not implement lookahead prefetch, LRU,
Belady, chunk-level transfers, Plotly, memory-usage sampling, or full
version/lifetime analysis. Dependencies are taken only from `DependsOn` fields
and do not infer read-after-write hazards. `W` is currently treated as a
write-only output; NNTile Adam access semantics need independent verification.
The engine currently serializes preparation of the next compute task and is
not a general-purpose offline planner. Check against synthetic cases before
interpreting real-trace results.
