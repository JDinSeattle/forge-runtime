# E21 — Real application process faults, 2026-09-11

The application F05/F07/F08 matrix passed **3 cases in 39.72 seconds**, using two independent OS worker processes per case, the production `application.Driver`, real PostgreSQL, typed runner RPC, a real runner process, SQLite WAL/FULL, the fixed ext4 volume pool and rootless Docker. The model returned deterministic fixture output; no provider API or actual provider payment was involved. The [raw test log](../benchmarks/results/application-faults-20260911T194600Z/test.log), [binary/source provenance](../benchmarks/results/application-faults-20260911T194600Z/execution-provenance.json) and [evidence manifest](../benchmarks/results/application-faults-20260911T194600Z/evidence-manifest.json) identify this execution. Its duration is one acceptance observation, not a latency distribution.

The harness author's [saved-evidence audit](../benchmarks/results/application-faults-audit-20260911.json) checked the records below. A separate agent independently recomputed bindings, ledger balances, trusted reports, daemon history and all 81 archived artifact digests with [audit-application.py](../scripts/faults/audit-application.py); its [independent audit result](../benchmarks/results/application-faults-20260911T194600Z/independent-audit.json) is preserved. These are reviews of the same saved execution, not a second OS experiment or an independent implementation review.

## Tested boundaries

| Case | Actual boundary and recovery | Before worker death | Original effect result | Duration |
|---|---|---|---|---|
| [F05](../benchmarks/results/application-faults-20260911T194600Z/F05/acceptance.json) | Accepted Start response delayed 2 seconds against a 500 ms target Start deadline; client inspects the same operation ID; worker is killed before returning that result to Driver; replacement settles the original receipt | Run `running`, effect `in_flight` | `succeeded`, counter `1\n`, Docker create/start 1/1 | 12.09 s |
| [F07](../benchmarks/results/application-faults-20260911T194600Z/F07/acceptance.json) | Runner exits 86 after observing actual container exit and before receipt publication; Driver commits unknown; runner journal restarts and replacement worker recovers the original container outcome | Run `needs_reconciliation`, effect `unknown` | `succeeded`, counter `1\n`, Docker create/start 1/1 | 14.88 s |
| [F08](../benchmarks/results/application-faults-20260911T194600Z/F08/acceptance.json) | Accepted command writes across natural lease expiry; replacement adoption stops the old container before a separately approved new writer executes | Run `running`, effect `in_flight` | `cancelled`, exit 137, `interrupted=true`, Docker create/start 1/1 | 12.75 s |

Each case uses its own private PostgreSQL schema, tenant and run. The exact original operation is `<run_id>_step_1_op_0`:

| Case | Run ID | Schema | Worker 1 → worker 2 PID |
|---|---|---|---|
| F05 | `run_ICRDMAOQZY3EUMEZ5XBGTODP5C` | `appfault_5pytkrcyncxqa6odyhm2ryuftu` | 106694 → 108086 |
| F07 | `run_OKCGGUK44KIVWRDFNQYFS3BUSX` | `appfault_ghbqyt6rtmoazhqjkbpunhoyoo` | 111726 → 113490 |
| F08 | `run_2K6OXI3QFTZH3X47HXRYX5LNTB` | `appfault_nrpugqpng3lq5isc36ax2cyxa5` | 116683 → 117384 |

The original PostgreSQL effect and runner operation retain their immutable ID, argument hash and dispatch epoch **2**. The replacement worker's first claim is epoch **3**. Each run has claim epochs 1, 2, 3 and 4; epoch 4 follows a later, separate command approval. Workspace cleanup uses its separate authority at epoch 5. The harness also checks that an old-epoch PostgreSQL heartbeat is fenced. Counting the final epoch alone would obscure the actual recovery boundary.

## Death, natural expiry and takeover

All times below are UTC on 2026-09-11. The SIGKILL timestamp is recorded immediately before `Process.Kill`; process wait confirms actual SIGKILL termination, but the harness does not record the kernel's exact death instant. Lease expiry is the durable PostgreSQL lease value. Replacement claim time is the database-clock value in its committed claim event. No lease value was rewritten to accelerate recovery.

| Case | SIGKILL requested | Original lease expires | First replacement claim, DB time | Expiry → claim |
|---|---|---|---|---|
| F05 | 19:46:05.082384675 | 19:46:09.120439 | 19:46:09.158365 | 37.926 ms |
| F07 | 19:46:18.617912629 | 19:46:22.922513 | 19:46:23.012538 | 90.025 ms |
| F08 | 19:46:30.756206510 | 19:46:35.225204 | 19:46:35.283816 | 58.612 ms |

F05's [runner marker](../benchmarks/results/application-faults-20260911T194600Z/F05/runner-fault-marker.json) records `after_start_acceptance` at 19:46:04.249350787. Its [worker pause](../benchmarks/results/application-faults-20260911T194600Z/F05/worker1-paused.json) records acceptance before Driver receipt at 19:46:04.703032873, after same-ID inspection. This exercises response loss after known local admission and a real worker death, rather than treating a Start acknowledgement as completion.

F07's [runner marker](../benchmarks/results/application-faults-20260911T194600Z/F07/runner-fault-marker.json) records `after_job_exit` at 19:46:17.177955150. The [worker pause](../benchmarks/results/application-faults-20260911T194600Z/F07/worker1-paused.json) at 19:46:17.239035772 follows the committed unknown state. The [pre-death PostgreSQL capture](../benchmarks/results/application-faults-20260911T194600Z/F07/before-worker-death-postgres.json) retains both the unknown effect and reserved capacity. Later recovery uses the same original container; no substitute operation ID is created.

F08's original command had been accepted when worker 1 died; Docker records its start at 19:46:30.878420870, shortly **after** worker death was requested. It then produced 63 increasing writes, including writes after lease expiry. The [saved stdout](../benchmarks/results/application-faults-20260911T194600Z/F08/original-command.stdout), [daemon events](../benchmarks/results/application-faults-20260911T194600Z/F08/original-docker-events.jsonl) and [acceptance report](../benchmarks/results/application-faults-20260911T194600Z/F08/acceptance.json) retain the raw nanosecond values:

| Observation | Unix nanoseconds | UTC |
|---|---|---|
| First old write | 1789155990900846515 | 19:46:30.900846515 |
| Last old write | 1789155995328654018 | 19:46:35.328654018 |
| Docker kill event | 1789155995379160209 | 19:46:35.379160209 |
| Docker die event | 1789155995576829247 | 19:46:35.576829247 |
| First new write | 1789155996755662730 | 19:46:36.755662730 |

The last old write occurs **103.450018 ms after lease expiry**. Expiry alone does not stop an already accepted job. The new write follows the last old write by **1.427008712 seconds** and follows the old container's die event. Its predecessor receipt records actual interruption. This demonstrates the tested adoption ordering on this host; it does not establish all possible interleavings or multi-host partition behavior.

## Ledger, artifact and verification checks

PostgreSQL capture files issue a separate SELECT for each table, without a shared snapshot transaction. They are **non-atomic cross-table observations**. The harness and auditors additionally check exact identities, claim events, original dispatch records and final balances; the word “capture” must not be interpreted as one simultaneous global state.

At the pre-death boundary all three cases retain tenant active count 1, runner reserved slots 1 and the original `runner_allocations` row in `reserved`. At completion each run is `completed` and `verified`, allocation is released, tenant active count and runner reserved slots are zero, and provider active requests, reserved tokens and reserved microdollars are zero. F08's original command effect is cancelled while the later successfully repaired run completes; effect outcome and run outcome are distinct.

Each run has exactly four completed model attempts and four settled quota reservations. Synthetic fixture usage is 150 + 150 + 150 + 120 = **570 tokens and 570 microdollars** at the fixture's one-microdollar-per-token quote. All request slots are released and the run cost equals the settled total. The ledger's `actual_usage` settlement kind refers here to supplied deterministic fixture usage, not a measured external provider bill. These checks show reservation and settlement consistency without making a billing accuracy claim.

The original target has one matching PostgreSQL effect and one matching runner operation. Raw daemon events reproduce one create and one start of that exact job. Archived stdout equals the decoded receipt output byte for byte. F05/F07's counter file and output both establish one execution. F08's 63 stdout timestamps match the saved writer samples. Run event sequences are contiguous at 62, 63 and 62 events respectively.

The complete trusted `expected/app.py` oracle repairs the existing clamp fixture. The original external target and regression grader validates the clean candidate, and the evidence includes the resulting reports and final patch. `Driver.CleanupWorkspace` then publishes a READY immutable workspace snapshot before releasing the leased volume. All three cleanup records are `released` with workspace revision 4 and cleanup epoch 5. Each case archives 27 READY artifact objects, including its code snapshot; all **81 object sizes and SHA-256 digests** match PostgreSQL metadata. The raw schema captures and archived bytes remain available after successful release.

The actual [F05 role preflight](../benchmarks/results/application-faults-20260911T194600Z/F05/worker-role-preflight.json) and matching F07/F08 reports verify distinct restricted worker logins: current and session identity match, explicit BYPASSRLS is present, and superuser, CREATEDB, CREATEROLE, schema CREATE and membership in the runs-table owner role are absent. The unchanged production `CheckWorkerRole` gate passes. The administrator is used for fixture provisioning, operator approvals and auditing; Driver execution and cleanup use the restricted worker role.

## Earlier failed attempts are retained

The [19:39:37 attempt](../benchmarks/results/application-faults-20260911T193937Z/test.log) failed during fixture preparation because the harness looked for `source/clamp.py`; the repository actually contains `source/app.py` and `expected/app.py`. No runner, worker or workspace had started. The fix reads the real complete oracle, retaining its actual function signature and inverted-bound exception behavior. An always-run fixture preflight now checks source and oracle paths, source hash, subprocess script construction and JSON round trips before any real case provisions processes.

The [19:42:17 attempt](../benchmarks/results/application-faults-20260911T194217Z/test.log) started the runner, then [worker 1 rejected the administrator login](../benchmarks/results/application-faults-20260911T194217Z/F05/worker1.log) through `CheckWorkerRole`. This was a fixture credential error; the production nonowner/non-superuser restriction worked. It failed before workspace allocation. The fix creates and validates the restricted fixture role before launching children, removes the administrator DSN from worker environments and reports early worker exit immediately. No production role check was bypassed. Both failed schemas and their logs are retained; the later PASS does not replace them.

## Acceptance scope and reproduction

E21 supports the combined application paths for F05 (local accepted-Start timeout and same-ID recovery), F07 (actual exit-before-receipt process crash, temporary unknown and original-job reconciliation), and F08 (natural lease expiry with an old writer, stop/adopt before the next writer and fenced old heartbeat). It adds the real PostgreSQL ledger dimensions absent from the earlier [runner-only matrix](runner-fault-evidence.md). It does not test API admission response loss, PostgreSQL outage, permanent daemon-history loss, remote network partitions or healthy API/SSE behavior during runner outage.

The [reproduction guide](../scripts/faults/application/README.md) describes the opt-in test executable, private schema and role, source preflight, existing pool requirement and reviewed user-service launch. The [captured harness source](../benchmarks/results/application-faults-20260911T194600Z/source-snapshot/application_test.go.txt) preserves the exact test used. No implementation-map rows are implicitly upgraded by this document beyond these stated observations.
