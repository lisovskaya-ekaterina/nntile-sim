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
