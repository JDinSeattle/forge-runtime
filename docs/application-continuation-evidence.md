# E25 — Application F02/F06/F09 process faults, 2026-09-11

The real continuation matrix passed **F02, F06 and F09 in 30.13 seconds**. It used private PostgreSQL schemas and restricted worker logins, separate OS worker processes, the production Driver on recovery, typed local runner RPC, the existing SQLite journal, the fixed ext4 volume pool and real rootless Docker. F02 uses an explicit claim-only fault child as detailed below. The model output and fee quote are deterministic fixtures. [Raw test/service log](../benchmarks/results/application-continuation-20260911T200939Z/test.log).

The [execution provenance](../benchmarks/results/application-continuation-20260911T200939Z/execution-provenance.json) identifies the binaries and sources. Their current bytes were checked against those captured hashes before archiving the exact [shared harness source](../benchmarks/results/application-continuation-20260911T200939Z/source-snapshot/application_test.go.txt) and [continuation source](../benchmarks/results/application-continuation-20260911T200939Z/source-snapshot/continuation_test.go.txt). The original F05/F07/F08 harness remains byte-identical to its E21 snapshot.

| Executable | SHA-256 |
|---|---|
| `bin/application-continuation.test` | `e61089b8db22fe613e7566ee80c9ef498455a2f8842667b20aa059de201e6acc` |
| `bin/forge-runner` | `6921c94212899fada5fabc730dcea7e4944d86c9ae2baccd5656e26dcbe969a0` |

The harness author's [saved-evidence audit](../benchmarks/results/application-continuation-20260911T200939Z/author-evidence-audit.json) rechecked the records and all 79 archived artifact sizes and digests. An [author evidence manifest](../benchmarks/results/application-continuation-20260911T200939Z/author-evidence-manifest.json) preserves the file fingerprints. This audit is not an independent implementation review or another OS execution. Historical SQLite zero counts and the rejected-approval error outcomes are saved harness assertions; this audit does not pretend to query an independent historical SQLite snapshot.

The separate [independent audit](../benchmarks/results/application-continuation-20260911T200939Z/independent-audit.json) recomputes the three cases from raw records. Peer review found three omissions in its initial version: immutable task identity across phases, patch summaries bound to the archived receipt payload, and the pending approval bound to the final decision and actual action. All three fabricated-summary regressions failed before the fix and are rejected after it; the original raw executions remain valid. The [correction record](../benchmarks/results/application-continuation-20260911T200939Z/audit-review-correction.json), [six passing tests](../benchmarks/results/application-continuation-20260911T200939Z/audit-regression.log), and [audit source fingerprints](../benchmarks/results/application-continuation-20260911T200939Z/independent-audit-manifest.json) retain this review history. The strengthened audit also accepts the earlier F05/F07/F08 records.

## Case identities and results

| Case | Run ID | Worker 1 → worker 2 PID | Result |
|---|---|---|---|
| [F02](../benchmarks/results/application-continuation-20260911T200939Z/F02/acceptance.json) | `run_PB56H7PUGG4B6KAKYZHLKHYOV6` | 437105 → 437145 | No external work before death; claim epoch 1 → 2; verified completion, 11.52 s |
| [F06](../benchmarks/results/application-continuation-20260911T200939Z/F06/acceptance.json) | `run_I6W6UUU5HPBZFYV44W6OQOTTS4` | 440613 → 441863 | Durable native patch receipt survives worker death and epoch 3 → 4 recovery; verified completion, 10.67 s |
| [F09](../benchmarks/results/application-continuation-20260911T200939Z/F09/acceptance.json) | `run_BVQGO2DWZ2LNV6YG7IFUFUBZOW` | 443963 → 444583 | Approval and workspace preserved after worker and runner death; verified completion, 7.94 s |

The schemas are respectively `appfault_slwbx6zofqpi4wndvuyrkt4i43`, `appfault_g33vm4tonlxkikhoosmvwosrnt` and `appfault_bns5hwve2hwy4fw5jt2cfprbbf`. Each retains a separate tenant, immutable source binding, role preflight and all relevant schema-scoped ledgers.

### F02: claim committed, no workspace/model/effect yet

Worker 1's test entry point calls the real `Store.ClaimOnRunner("worker1-0", 5s, "application-fault-runner")`, records the [claim-only pause](../benchmarks/results/application-continuation-20260911T200939Z/F02/worker1-paused.json), and never calls `Driver.Drive`. It has no heartbeat. This deliberately isolates the gap before any workspace preparation, model request or system effect, rather than describing a generic later RPC pause as that boundary. Worker 2 runs the ordinary production `Driver.RunWorker` loop.

The [pre-death capture](../benchmarks/results/application-continuation-20260911T200939Z/F02/before-worker-death-postgres.json) contains no effects, model attempts, quota reservations, artifacts or approvals. The harness also queries the original SQLite journal and records zero workspace, operation and volume lease rows for this run. PostgreSQL has already reserved one runner slot and one tenant active allocation, as required by claim; these counters are not confused with a prepared filesystem workspace.

The parent sends actual SIGKILL before the five-second lease expires. Worker 2's first committed claim uses epoch 2 after natural expiry. Later command approvals account for epochs 3 and 4; the original counter command's dispatch epoch is **3**, not the fault epoch 1. Recovery initializes the workspace once and runs the complete repair/verification sequence.

### F06: native patch receipt durable, PostgreSQL not settled

The fault targets `run_I6W6UUU5HPBZFYV44W6OQOTTS4_step_3_op_0`. A test-only wrapper pauses after real runner Start/Inspect has returned a successful durable patch receipt and before that result returns to Driver. At the pause, `app.py` already equals the complete trusted oracle. SQLite has a committed success and receipt ref; the receipt bytes are available and match their size/hash. PostgreSQL still records the original patch effect as `in_flight` with a null `receipt_ref`. The [published file](../benchmarks/results/application-continuation-20260911T200939Z/F06/patch-before-death-app.py), [original operation](../benchmarks/results/application-continuation-20260911T200939Z/F06/patch-before-death.json), [SQLite receipt ref](../benchmarks/results/application-continuation-20260911T200939Z/F06/patch-journal-receipt-before-death.json) and [PG capture](../benchmarks/results/application-continuation-20260911T200939Z/F06/before-worker-death-postgres.json) preserve that boundary.

After SIGKILL and natural expiry, the replacement worker reconciles the same operation at its original dispatch epoch **3**. The [post-recovery operation](../benchmarks/results/application-continuation-20260911T200939Z/F06/patch-after-recovery.json) is JSON-equal to the original, including receipt, arguments, policy, before/after tree hashes and revision 3 → 4. PostgreSQL contains one successful matching patch effect with the original hash and epoch.

| Patch observation | Value |
|---|---|
| Before tree SHA-256 | `97cbcadee2414cf6716e341acf03f560457bf0148c73b1cfbbd401ae65c157c6` |
| Expected and actual after tree SHA-256 | `d8fb198068602034f4323c0b2fc5d242d1160f27204dfc9b092d5e02e8799e93` |
| Original receipt SHA-256 | `d73dca9a062c2f4ce8278385a8545f0a83c17cac884ed7005d7e3c1540675360` |

This is a real native filesystem patch and receipt; the patch does not launch a Docker command. The separate counter command and trusted verification use actual Docker. The retained original receipt, unchanged resulting revision and immutable before-hash precondition substantiate recovery without reapplying the patch. This run kills the worker at the gap; it does **not** inject a PostgreSQL outage at that gap.

### F09: durable approval survives both process deaths

Worker 1 dies while the run is `waiting_approval`, then runner PID **443758** receives SIGKILL. The replacement runner PID is **444419**, using the same authoritative journal. A separate worker 2 starts while the original approval is still pending. The [before-death](../benchmarks/results/application-continuation-20260911T200939Z/F09/before-worker-death-postgres.json) and [after-restart](../benchmarks/results/application-continuation-20260911T200939Z/F09/approval-after-restart-postgres.json) captures retain the exact approval row, pending decision, run version **6**, approval version **1**, policy `workspace-v1`, effect ID, args hash and workspace revision **1**. No original command operation exists in the runner before approval.

The complete workspace tree hash is unchanged at `66ee6f4ae415b0735c6eb4d0645f5a8d72d33e7946d0219e319aa9158a375659`. The harness submits five incorrect bindings—args hash, workspace revision, policy version, approval version and effect ID—and requires conflict for every one. The [rejection report](../benchmarks/results/application-continuation-20260911T200939Z/F09/approval-after-restart.json) records these outcomes. Its subsequent persistent-state check proves that no rejected attempt decides or advances the approval. Finally, the operator submits the original correct binding, and the original counter command executes once.

At approval wait the tenant/runner capacity is correctly released at **0/0**, while the workspace remains retained. This differs from F02/F06's unsettled **1/1** allocations. F09 resumes because a durable approval decision queues work; it is not a lease-expiry experiment.

## Timing and accounting

All times below are UTC on 2026-09-11. SIGKILL times are recorded immediately before sending the signal; `Wait` confirms SIGKILL, but there is no separate kernel death timestamp. Expiry and claim times come from persisted database values/events.

| Case | Worker SIGKILL requested | Original lease expires | Replacement claim | Expiry → claim |
|---|---|---|---|---|
| F02 | 20:09:40.979436726 | 20:09:45.942514 | 20:09:46.002725 | 60.211 ms |
| F06 | 20:09:55.387129727 | 20:09:59.422783 | 20:09:59.517069 | 94.286 ms |
| F09 | 20:10:04.054954870 | N/A: waiting approval | 20:10:05.434188 | N/A |

F09's runner SIGKILL request is at **20:10:04.059776584**. Its valid approval commits at **20:10:05.348807**, before the replacement claim. The original heartbeat is rejected after recovery in all cases. These timings describe one acceptance execution, not recovery-latency percentiles.

All three runs end `completed` and `verified`. The original counter command has exactly one Docker create/start, one matching runner operation and one successful matching PG effect, with stdout and file count `1`. Each run has four completed fake-model attempts and four settled quota reservations: 150 + 150 + 150 + 120 = **570 fixture tokens and 570 synthetic microdollars**. Provider request slots and reserved tokens/cost, runner reserved slots and tenant active count finish at zero. The ledger's `actual_usage` label refers to supplied fake usage here, not a real provider invoice.

The external trusted target/regression reports bind to final workspace revision 4, with baseline target failure and final target/regression success. Ordinary `Driver.CleanupWorkspace` publishes the READY immutable snapshot before release. F02/F06 cleanup uses epoch 5; F09 uses epoch 4. All **79** READY objects are archived and digest/size checked: 26 for F02, 27 for F06 and 26 for F09. Run events are contiguous: the acceptance checks observe 60/62/59 events, and the later cleanup captures contain 63/65/62 after three cleanup events per case.

All PostgreSQL tables are read separately, without a common snapshot transaction. These exports are **non-atomic cross-table captures**. The audit rechecks immutable bindings, claimed epochs, artifact bytes and settled balances; it does not treat the captures as one simultaneous global state.

## Scope and reproduction

The [continuation guide](../scripts/faults/application/CONTINUATION.md) contains the exact opt-in build/service command, private-role setup, unchanged source/oracle requirements and same-run recovery procedure. This successful matrix did not exercise the separate `TestRecoverApplicationContinuation` failure-recovery entry point; that capability is not marked as independently accepted by this record. Each successful case did exercise ordinary snapshot publication and workspace release. Earlier E21 setup failures remain preserved in the [application fault record](application-fault-evidence.md); they are not erased or reclassified by this later result.

E25 substantiates the stated claim-before-work, patch-receipt-before-PG-settlement and approval-restart paths. It does not establish every process interleaving, PostgreSQL outage, network partition, model quality or actual provider billing. The implementation map is updated separately after review.
