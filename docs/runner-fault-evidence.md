# Real runner fault evidence — 2026-09-11

The real runner process matrix passed all **11 cases in 33.93 seconds** under the deployed user-service / RootlessKit mapping, with the fixed ext4 pool and digest-pinned Python container. [Raw successful test log](../benchmarks/results/runner-faults-20260911T191948Z/test.log) and [binary hashes](../benchmarks/results/runner-faults-20260911T191948Z/binaries.json) identify this execution. A separate parse of the saved JSON checked the values below; its [machine-readable audit](../benchmarks/results/runner-faults-evidence-audit-20260911.json) is preserved. The parser author also authored the implementation and harness: this is evidence validation, **not an independent implementation review**.

| Case | Fixture workspace | Observed result | Docker create/start |
|---|---|---|---|
| Lease before import | `init-e61b7b789ae8` | Same source resumed at epoch 2 | 0 / 0 |
| One imported file | `init-5a4832edbd87` | Missing files filled from pinned source | 0 / 0 |
| Import before workspace row | `init-6dc1e60e53b0` | Revision 1 reconstructed | 0 / 0 |
| Prepared before dispatch | `fault-280f7182c884` | Proven never-started cancellation | 0 / 0 |
| Created before start intent | `fault-254c1ce80c0a` | Unknown retained until locked no-start proof | 1 / 0 |
| Docker start before runner observation | `fault-88842f79f46b` | Observed success, original receipt | 1 / 1 |
| Runner observed start | `fault-b637892499c6` | Observed success, original receipt | 1 / 1 |
| Actual exit before receipt | `fault-8a0d00d51be1` | Observed success, original receipt | 1 / 1 |
| Pinned artifact before operation finish | `fault-f41b865b9615` | Observed success, original receipt | 1 / 1 |
| Accepted Start response timeout | `fault-6c70d59711eb` | Same-ID client inspect, observed success | 1 / 1 |
| Old writer survives runner death | `fault-50ae5945eb93` | Epoch 2 adoption proves stop before new writer | 1 / 1 for old job |

All 11 workspaces are unique. The 8 operation reports retain the exact original operation/workspace/tenant/run binding. The 5 successful counter commands each wrote exactly `1\n`, and same-ID resubmission returned the immutable receipt instead of launching another container. Saved daemon events match each report's `job_id` and independently reproduce its create/start counters. The initialization cases recorded a pinned source hash, epoch 1 reservation, no workspace row and zero operations before resume.

Each successful case ended with a no-active stop receipt, durable snapshot and released volume lease. The three initialization cases each verify two artifact pins (stop/snapshot); each operation case verifies three (operation/stop/snapshot). This count is the explicitly checked reference set, not a global count of all historical pins.

The [takeover report](../benchmarks/results/runner-faults-20260911T191948Z/takeover.json) contains 41 increasing old-writer timestamps. Its last old write is `1789154421338093618 ns`; the first new write is `1789154422368981266 ns`, a positive gap of **1.030887648 seconds**. The original container's saved daemon history includes kill/die and its receipt records `interrupted=true`, exit 137, cancelled. Fresh old-epoch admission was rejected, and the old file remained unchanged through completion of the replacement job. These observations substantiate this tested handoff; they are not a proof of every possible scheduler or kernel interleaving.

## Failed evidence remains part of the record

The [first direct RootlessKit attempt](../benchmarks/results/runner-faults-20260911T190828Z/test.log) stopped before any harness workspace was created because inherited no-new-privileges prevented `newuidmap`. The subsequent execution used the same user-service architecture as the deployed runner.

The [first actual matrix](../benchmarks/results/runner-faults-20260911T191322Z/test.log) passed the three initialization cases and prepared case, then failed at created-before-start cancellation. Its [failed report](../benchmarks/results/runner-faults-20260911T191322Z/after_docker_create.json) retained workspace `fault-cde8e691cca1` and operation `fault-cde8e691cca1-op`. The cause was a lock-free preliminary backend cancellation returning `reconciliation_required` before the durable start-intent recovery path acquired the writer lock. Cancel/Stop/Adopt now cancel the local executor context, acquire exclusive writer ownership and inspect/cancel in one reconciliation decision.

The [same-operation recovery](../benchmarks/results/runner-faults-20260911T191812Z/recover_after_docker_create.json) passed in 1.55 seconds. Its original marker and operation ID exactly match the failed report; daemon create/start counts are 1/0. No replacement operation was submitted. It produced a cancelled, never-started receipt, then stopped, sealed and released the retained workspace. The later 11-case success is a new run; it does not replace or hide the failure and recovery evidence.

## Scope

This run exercises actual runner process death, typed local RPC, SQLite WAL/FULL, ext4, rootless Docker and filesystem effects. It uses no model, API credentials, production task or simulated execution backend. The reports explicitly mark PostgreSQL run/effect/budget/capacity/SSE evidence as **N/A**. Therefore it closes the runner components of F05/F07/F08 and initialization recovery, not the complete application fault matrix. Application-level acceptance must additionally run real worker processes and capture the PostgreSQL ledgers and their transitions.

The later [E21 application matrix](application-fault-evidence.md) supplies real worker-process and PostgreSQL evidence for the stated F05/F07/F08 paths. It is a separate execution; the runner-only reports above retain their original N/A scope.

Recovery accepts complete source files forming an exact subset of the pinned source. A torn single file, foreign file, altered source or legacy orphan lease lacking a source hash remains retained for explicit reconciliation; these cases are not advertised as automatic repair. [Reproduction and recovery commands](../scripts/faults/README.md) document the operator-only boundary.
