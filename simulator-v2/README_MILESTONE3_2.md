# Milestone 3.2: topological + LRU + demand-only

Copy only these paths into `simulator-v2/`:
- `pyproject.toml`
- `src/nntile_sim/core/engine.py`
- `src/nntile_sim/sim_cli.py`
- `tests/test_lru_demand.py`

Do not replace your existing `model.py`: it already contains the seconds -> microseconds correction.

Install: `python -m pip install -e .`
Test: `python -m pytest -q`
Run: `nntile-sim ../examples/tasks_mbs1.rec --gpu-memory-mib 1349 --h2d-gbps 12 --d2h-gbps 12 --memory-policy lru-demand --events-csv events_lru_1349.csv`
Old behavior: `--memory-policy legacy-baseline`.

Assumptions: deterministic lexicographical NetworkX topological ordering; one Compute lane; no prefetch; transfers are demand-only and sequential in this baseline; LRU recency updates at Compute accesses; output-only writes allocate without H2D; live dirty evictions use D2H; dead dirty objects can be dropped. StarPU-like, not bitwise StarPU reproduction. WontUse ignored. Data version and trace semantics need further validation.
