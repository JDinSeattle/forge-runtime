# Remaining acceptance work

This is an executable SDE baseline with recorded local tests. It is not full
completion of every compound requirement in the source plan. The
[implementation map](implementation-map.md) remains the detailed authority.
These are the most consequential open boundaries; no work schedule is assigned.

| Boundary | Concrete next implementation or experiment | Acceptance evidence |
| --- | --- | --- |
| Real model evaluation | Register exact native model IDs, capabilities, immutable prices and a small explicit budget. Run a fixed held-out task set through the CLI and trusted profiles. | Per-task target/regression results, complete or explicitly unknown usage, billed-cost accounting and raw latency. Fake scripts do not substitute for this. No provider keys are currently configured. |
| Remaining process fault matrix | Add bounded operator-only fault points around Docker create/start/exit and receipt publication. Kill the worker/runner at each point, keep original operation IDs and inspect before replay. | Actual launch counts and writer intervals, preserved unknown outcomes, no old-epoch writes, and consistent effect/capacity/fee ledgers. The current OS-crash result covers saved model output only. |
| Cross-store object retention | Implement age-bounded orphan collection with publication checks and protection against a still-publishing worker. Separately define recovery of a reserved volume whose import never produced a workspace journal row. | Publication/GC race tests; READY or recoverable references remain readable; incomplete imports are preserved until evidence permits release. Current workspace GC already seals a code snapshot before release. |
| Sustained event delivery | Extend real TCP tests until slow-client backpressure occurs; measure heap plateaus and save profiles. Keep the default-buffer sample separate from any constrained-socket experiment. | Slow subscriber actually disconnects while healthy cursors remain complete; post-close resource time series stays bounded. Existing short fanout/churn samples are narrower evidence. |
| Operational recovery | Rehearse coordinated database/journal/object/workspace backup and restoration, additive upgrades, and independent collector delivery. | Recovery point and unrecoverable uncertainty are explicit; a restored SQL snapshot alone never authorizes replay of unknown commands. |

Optional MCP/ACP, additional runners, S3, Temporal, Kubernetes and a Web UI remain
extensions. Their presence in the plan does not give them priority over these
acceptance boundaries.
