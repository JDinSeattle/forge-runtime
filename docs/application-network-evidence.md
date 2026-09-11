# E27 — Application F10/F11/F12 cancellation and dependency faults, 2026-09-11

The real matrix passed all four cases in **35.56 seconds**: both F10 cancellation orders, F11 private PostgreSQL connectivity loss, and F12 original-runner process loss. It used private PostgreSQL schemas, restricted service logins, separate worker/API OS processes, production Driver and HTTP/SSE handlers, typed local runner RPC, the authoritative SQLite journal, fixed ext4 storage and actual rootless Docker. Model responses and fees were deterministic fixtures. [Raw test/service log](../benchmarks/results/application-network-20260911T203949Z/test.log).

The fault boundary matters: **F11 and F12 interrupt communication after the original command has already succeeded and durably recorded its native receipt, while PostgreSQL still records the effect as `in_flight`.** They do not interrupt a writer still modifying files. F10 similarly pauses a successful native final-diff receipt before Driver receives the result. These are receipt-reconciliation and terminal-ordering observations, not a replacement for the earlier active-process cancellation tests.

## Executed source identity

The test binary was built in `/tmp/forge-network-build-224a6b6` from production commit **`224a6b64ce0cc41c564ce2c9bfd2e74b4b9ccc0a`**, plus exactly the three frozen network test files. The build excludes the retry/deadline changes being developed concurrently in the main checkout. The [compilation provenance](../benchmarks/results/application-network-20260911T203949Z/compilation-provenance.json) records all **155 Go source hashes**; [execution provenance](../benchmarks/results/application-network-20260911T203949Z/execution-provenance.json) records the invoked binaries and harness sources.

| Executable | SHA-256 |
|---|---|
| `bin/application-network.test` | `e1d9e96ccb4df4797304c93a56f11f0f2bcfe28f443b7007ed795fa55f93c765` |
| `bin/forge-runner` | `6921c94212899fada5fabc730dcea7e4944d86c9ae2baccd5656e26dcbe969a0` |

An [author source verification](../benchmarks/results/application-network-20260911T203949Z/author-source-verification.json) matched all 155 isolated-build source files and both existing binary files to those recorded hashes. The five exact harness files remain in the [source snapshot](../benchmarks/results/application-network-20260911T203949Z/source-snapshot/network_test.go.txt); critical executed [Driver](../benchmarks/results/application-network-20260911T203949Z/source-snapshot/production/internal/application/driver.go.txt), [persistence execution](../benchmarks/results/application-network-20260911T203949Z/source-snapshot/production/internal/persistence/execution.go.txt), [Store](../benchmarks/results/application-network-20260911T203949Z/source-snapshot/production/internal/persistence/store.go.txt) and [HTTP server](../benchmarks/results/application-network-20260911T203949Z/source-snapshot/production/internal/httpapi/server.go.txt) files are also archived. This record establishes the identified binaries' behavior; it does not mark subsequent source changes as executed.

The harness author's [saved-record audit](../benchmarks/results/application-network-20260911T203949Z/author-evidence-audit.json) and [recomputation script](../benchmarks/results/application-network-20260911T203949Z/author-recompute.py) check the raw receipts, PostgreSQL bindings, HTTP/SSE records and artifact bytes. The [author manifest](../benchmarks/results/application-network-20260911T203949Z/author-evidence-manifest.json) fingerprints the evidence. This is an author audit of the saved execution, not an independent implementation review or a second OS execution.

## Independent audit

The [independent recomputation](../benchmarks/results/application-network-audit-review-20260911/independent-audit.json)
passes all four saved cases, checking 107 artifact objects and 259 cleanup-phase
events. Review initially found six altered-report variants accepted by the
auditor: missing terminal version/epoch/stop bindings, changes to already settled
effects, an inserted event during database loss, a released unknown allocation,
and a changed pending operation in the actual HTTP response. These were audit
omissions; the original execution records satisfy the stronger comparisons.

The [review manifest](../benchmarks/results/application-network-audit-review-20260911/manifest.json)
retains the failing negative-test output, corrected auditor/test source, passing
output and results. Seven regression methods now pass, covering four original
cases and rejection of all six altered variants. A separate reviewer rechecked
the correction and reran the frozen tests. No original matrix report was changed,
and saved-record recomputation is not counted as another process-fault run.

## Cases and outcomes

| Case | Run ID | Worker PIDs | Terminal result | Duration |
|---|---|---|---|---|
| [F10 cancel first](../benchmarks/results/application-network-20260911T203949Z/F10_cancel_first/acceptance.json) | `run_BLLHRTO2C5FNPBQGC6XOBKCIVC` | 953068 | cancelled, verified; version 29 | 6.45 s |
| [F10 complete first](../benchmarks/results/application-network-20260911T203949Z/F10_complete_first/acceptance.json) | `run_FXTWIJ6BFK52VNWTEVFZJOEUUH` | 955842 | completed, verified; version 28 | 6.36 s |
| [F11](../benchmarks/results/application-network-20260911T203949Z/F11/acceptance.json) | `run_SPYKFTUABYUKYR74Y3GNTYU5LR` | 958635 → 959672 | completed, verified; version 30 | 11.17 s |
| [F12](../benchmarks/results/application-network-20260911T203949Z/F12/acceptance.json) | `run_LLHVN4LI23RZGQVCYHGOIRRRV2` | 961980 → 962929 | completed, verified; version 31 | 11.58 s |

Each case has one private tenant/schema. The schema names, immutable run configuration, source binding, original operation IDs and restricted-role checks remain in the case records. API PIDs are respectively **953052, 955827, 958618 and 961963**. API logins pass production `CheckAPIRole` with NOBYPASSRLS and no ownership/superuser privileges. Worker logins pass `CheckWorkerRole` with explicit BYPASSRLS, no ownership/superuser/role-creation/database-creation privileges and schema-scoped runtime permissions. No provider key or authorization token is included in these evidence files.

### F10: cancellation and completion commit in both controlled orders

Both cases reach run version **27** after the repair and trusted target/regression checks. The actual `get_diff` operation already has a successful durable receipt; a test-only RPC wrapper pauses before returning it to Driver. The [cancel-first pause](../benchmarks/results/application-network-20260911T203949Z/F10_cancel_first/worker1-paused.json) and [PG boundary](../benchmarks/results/application-network-20260911T203949Z/F10_cancel_first/before-cancel-race-postgres.json) retain that mismatch.

In cancel-first, an actual HTTP POST commits `cancel_requested` version **28** before the wrapper resumes. The final-diff effect settles as succeeded. A real stopped-workspace receipt has `no_active_operations=true`, and the run commits `cancelled` version **29**. Two later HTTP Cancel calls both return HTTP 202 with the same cancelled version 29. In complete-first, the wrapper resumes and completion commits at version **28** before two HTTP Cancel calls; both return HTTP 202 with unchanged completed version 28. [Cancel-first responses](../benchmarks/results/application-network-20260911T203949Z/F10_cancel_first/http-observations.json), [complete-first responses](../benchmarks/results/application-network-20260911T203949Z/F10_complete_first/http-observations.json).

The cancelled run retains the observed outcomes: its two commands, native patch, final target verification, regression verification and final diff are all **succeeded**. Its sole failed effect is the intentionally failing baseline target check. `cancelled` and `verified` therefore coexist: cancellation wins run finalization after verification has already succeeded. This case does not recast completed effects as cancelled or claim that an executing command was interrupted.

### F11: private PG connectivity cut, then natural lease expiry

The original command is `{run_id}_step_1_op_0`, dispatched at epoch **2**. Its [pre-cut native operation](../benchmarks/results/application-network-20260911T203949Z/F11/original-before-outage.json) already contains success, stdout and a durable receipt. PostgreSQL's [pre-cut capture](../benchmarks/results/application-network-20260911T203949Z/F11/before-outage-postgres.json) still has that same effect `in_flight` with no receipt ref.

At **20:40:05.310496008 UTC**, the private transparent TCP proxy closes existing connections and refuses new ones. Its `connections=8` is a count of socket endpoints, not eight SQL sessions. The original PostgreSQL server stays running; only the fixture API and worker use this proxy, while a separate administrator connection remains available for read-only capture.

Actual submission and `/readyz` requests return **HTTP 500** during the cut. The preserved responses say `code=internal` and `retryable=false`; this record does not describe them as 503 or claim a retry hint. No new run or `run.created` event appears. The run retains its original allocation, and the harness records only the two existing local operations: baseline verification and the original command. [HTTP records](../benchmarks/results/application-network-20260911T203949Z/F11/http-observations.json), [during-cut PG capture](../benchmarks/results/application-network-20260911T203949Z/F11/during-database-outage-postgres.json).

Worker 1 receives SIGKILL, then connectivity is restored at **20:40:07.319174256 UTC**, after a **2.008678248 s** cut. In this full matrix the first restored GET succeeds with HTTP 200. Worker 2 claims epoch **3** after the original persisted lease expires. It adopts the original workspace and settles the original epoch-2 command receipt without another Docker create/start. The pre-cut and final original operations are JSON-equal, including args, policy, revision, output and receipt. The old worker's heartbeat is rejected.

### F12: original runner unavailable; API and SSE remain usable

The same durable-receipt/PG-in-flight boundary is captured for F12. Runner PID **961762** receives SIGKILL. The original worker then encounters unavailable runner RPC and production Driver records the effect as **unknown**, run state `needs_reconciliation`, and the original runner placement. Tenant active count and runner reserved slots remain **1/1**; the allocation stays reserved. The [outage capture](../benchmarks/results/application-network-20260911T203949Z/F12/during-runner-outage-postgres.json) preserves these facts.

During the runner outage, the separate API process returns HTTP 200 from `/readyz` and GET run, showing the original unknown effect and runner ID. A real HTTP message submission returns 202. The actual TCP SSE stream delivers the resulting durable `run.message_added` event **19** before the runner restarts. The saved during-outage stream is contiguous **1–19**. The later saved stream is contiguous **1–62**, and every saved event's payload/type/timestamp matches PostgreSQL. It is a prefix of the 65 events present at terminal capture, not evidence that this client received the terminal tail. [HTTP](../benchmarks/results/application-network-20260911T203949Z/F12/http-observations.json), [outage SSE](../benchmarks/results/application-network-20260911T203949Z/F12/sse-during-runner-outage.json), [later SSE prefix](../benchmarks/results/application-network-20260911T203949Z/F12/sse-observed.json).

F12 recovery uses the normal **Driver.Defer** path. Its original pre-outage lease was until **20:40:20.580425 UTC**. Defer ends that lease at database time **20:40:16.443552**, sets persisted `not_before` to **20:40:21.441861**, and records `run.reconciliation_wait`. Worker 1 then receives SIGKILL. The same journal returns under runner PID **962744**; worker 2 claims epoch **3** at **20:40:21.490410**, which is **48.549 ms after persisted not_before**. The harness does not edit lease/deadline rows. This is normal deferral and eligibility recovery, **not an untouched natural-expiry experiment**.

The original command receipt is unchanged through that recovery; the already successful effect is reconciled to success, and the old epoch's heartbeat is rejected. No replacement runner or new operation ID is assigned to erase uncertainty.

## Timing, artifacts and ledgers

Times below are UTC on 2026-09-11. SIGKILL times are recorded immediately before requesting the signal; process `Wait` confirms SIGKILL, but no separate kernel death timestamp is claimed. Claim times come from the persisted reducer input event, not a later observer's wall clock.

| Case | Worker 1 SIGKILL requested | Persisted lease end used for recovery | Replacement claim | Interpretation |
|---|---|---|---|---|
| F11 | 20:40:07.317524433 | 20:40:09.568131 | 20:40:09.603765 | natural expiry → claim **35.634 ms** |
| F12 | 20:40:16.557626128 | 20:40:16.443552 | 20:40:21.490410 | Defer ended lease; claim waits for `not_before` |

F12 runner SIGKILL is requested at **20:40:16.330717595**. Its message event commits at **20:40:16.472021**, while the runner is still absent. Each case's original counter command has exactly one Docker create, start and exit-0 die event for one container ID. The native stdout is `{"counter": 1}\n`, and the snapshot contains `app-counter.txt` with exactly `1\n`. The saved [daemon events](../benchmarks/results/application-network-20260911T203949Z/F12/original-docker-events.jsonl) and receipts support that operation's single execution; they are not a claim that the entire repair uses only one container.

| Case | READY artifact count | Artifact bytes | Terminal-capture events | After-cleanup events |
|---|---:|---:|---:|---:|
| F10 cancel first | 27 | 29,620 | 61 | 64 |
| F10 complete first | 26 | 29,162 | 59 | 62 |
| F11 | 27 | 29,622 | 62 | 65 |
| F12 | 27 | 29,877 | 65 | 68 |
| Total | **107** | **118,281** | **247** | **259** |

All **107** archived READY objects have matching byte lengths and SHA-256 digests. The **28** operation receipts match PG operation ID, tenant/run/workspace, canonical argument hash, kind, policy, expected revision and dispatch epoch. Public receipt grants are empty. Each run has seven effects: the expected failed baseline check plus six successes. All target/regression reports bind trusted evidence to final revision **4**. Each immutable code snapshot contains the full correct `app.py` oracle and hashes to the same candidate tree as the final diff.

Each case settles four completed fake-model attempts and four reservations: **150 + 150 + 150 + 120 = 570 fixture tokens and 570 synthetic microdollars**. Across four cases this is 2,280 of each. The `actual_usage` ledger label describes supplied fake usage here, not a provider invoice. Final provider active requests, reserved cost/tokens, tenant active count and runner reserved slots are zero. Every runner allocation is released, and no PG effect remains in flight or unknown.

Ordinary `Driver.CleanupWorkspace` publishes a READY immutable snapshot before the one workspace-release record; a second cleanup call returns the same cleanup ID and snapshot. This adds exactly three persisted events per case. These assertions and the snapshots establish the exercised cleanup path; separate historical SQLite zero-count checks remain harness observations, not an independent historical database snapshot.

The PG exports read each table separately. They are **non-atomic cross-table captures**. Byte/binding and sequence checks establish the stated saved relationships; the records are not one globally simultaneous transaction snapshot. Durations describe one acceptance sample, not percentiles or a general availability SLO.

## Preserved preflight failure and reproduction

The first no-slot API preflight failed on an immediate GET after PG proxy restoration: HTTP **500** was observed where the test assumed immediate recovery. Its earlier request during the cut also returned 500. The [original HTTP record](../benchmarks/results/application-network-api-preflight-20260911/F11/http-observations.json), private failed schema and [failed source fingerprint](../benchmarks/results/application-network-api-preflight-20260911/failure-test-source.json) remain preserved. No runner/worker process or volume was allocated by that preflight.

The minimal correction was test-only: allow a total five-second, 100-ms-interval recovery poll for a **read-only GET**. The failed submit is not retried. The [successful retry](../benchmarks/results/application-network-api-preflight-20260911-retry/F11/api-preflight.json) still records the first post-restore GET as **500** at **20:35:36.985590849**, then **200** at **20:35:37.086289746**, a **100.698897 ms** observation gap. These [HTTP records](../benchmarks/results/application-network-api-preflight-20260911-retry/F11/http-observations.json) are retained rather than reporting immediate restoration. No production database gate or retry behavior was changed for that correction.

The [network harness guide](../scripts/faults/application/NETWORK.md) provides the opt-in build/preflight commands, reviewed mapped user-service lifecycle and same-run failure recovery entry point. The successful matrix exercises ordinary cleanup but does **not** exercise `TestRecoverApplicationNetwork`; that failure-recovery command has no new actual-execution claim here. On a failure, its descriptor preserves the original schema/run/journal identity and requires reconciliation instead of deleting unknown work or editing its lease/deadline. E27 covers the stated controlled orders and dependency-loss paths; it does not establish all interleavings, multi-host failover, provider quality or real provider billing.
