# Milestone 7.4 diagnostic update

Scope: `tasks_mbs1.rec`, `fixed-slot`, Belady, no prefetch. Other traces/policies deliberately rejected by CLI.

`analyze_repeated_subgraphs.py` groups tasks by their recorded `Iteration` tuples; it does **not** prove that groups are isomorphic, independent, or sequential. `subgraph_analysis.json` reports per-group task-name distributions, membership of handles, and handles shared between groups. Tasks without iteration metadata are counted but excluded from group membership.

The optimizer now prints each evaluation, appends `search_history.csv` immediately, and writes `latest_plan.json` after each evaluation. It still writes `summary.json` and `chunk_plan.json` on normal completion. **This is checkpointing for inspection, not automatic resume.** The default `--focus shared` searches only handles appearing in multiple iteration groups. It groups *eligible* handles by size, not all handles with that size. `--focus all` restores the original scope.

No new heuristic byte thresholds were introduced. Candidate sizes remain derived from target chunk counts. The optimizer does not prove global optimality, and does not verify graph isomorphism or causal benefit from shared handles. A no-split baseline follows a different engine path than chunked execution: validate equivalence before interpreting small gains. Simulations of large chunk counts may still be slow.
