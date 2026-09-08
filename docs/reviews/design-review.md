# Independent design review

Reviewed 2026-09-07 by a separate review agent. This is an agent review of
`../SDE_IMPLEMENTATION_PLAN.md` supplied with the project, not a human or employer
endorsement. Implementation and benchmark results were not yet available when
this review was written. Passing a unit test is not evidence that its real
PostgreSQL, container, or remote-provider counterpart works.

## Assessment

The plan has a coherent single authority for run state (PostgreSQL), a separate
authority for observed command execution (the runner journal plus inspected job),
and deliberately limited host-failure recovery. These boundaries are appropriate.
The main risk is replacing the specified invariants with a smaller happy-path
demo while retaining the broader reliability claims. The following gates must be
demonstrated against real dependencies before those claims are made.

## Required adversarial acceptance tests

| ID | Fault or adversarial interleaving | Required evidence |
| --- | --- | --- |
| R01 | Two workers claim the same tenant/run concurrently; a heartbeat races lease expiry. | One current DB owner/epoch; CAS includes owner, epoch, version, and DB-time expiry. Only the current epoch may commit a state change. Record rows and affected-row counts. |
| R02 | Expire a worker lease while its command keeps writing; adopt using a higher epoch. | Runner rejects new old-epoch operations immediately, but does not admit a new writer until the previous writer has a stopped/completed receipt. A file-level concurrency probe must observe at most one writer. A DB counter alone does not prove this. |
| R03 | Crash after SQLite `prepared` commit, after Docker create/start, and after command exit before receipt commit. Retry the same operation. | Stable job ID is inspected. A persisted success is reused; a definitely unstarted operation can start once; ambiguous absence becomes `unknown`. Actual process-start count and final workspace agree with the journal. Do not infer “never ran” solely from a missing container. |
| R04 | Retry an operation ID with different arguments, revision, workspace, run, tenant, or tool; retry an old signed token after takeover. | All immutable bindings are checked before returning a prior receipt. Parameter mismatch rejects; invalid capability never returns another tenant's receipt. Expired or stale-epoch authority starts no new job. |
| R05 | Runner completes patch/command, then PostgreSQL becomes unavailable before effect publication. | Worker queries the original operation, persists the observed receipt, and proceeds once. It must not call the model again to reconstruct a completed tool choice or create another operation ID for the same uncertain effect. |
| R06 | Snapshot commit loses its response; restart after model result storage but before snapshot update. | Reuse the durable model result and original call IDs. Step, snapshot, effect, event, and artifact references remain consistent. Provisional/partial tool arguments cause zero executor calls. |
| R07 | Cancel races command completion, worker adoption, approval, and late model response. | `cancelled` means the controlled container/process has stopped. A terminal run never becomes runnable again. Record the actual executed effect, do not erase it because cancellation won the state race. Repeated cancellation releases capacity once. |
| R08 | Concurrent create requests use the same idempotency key, then same key with changed body or different principal. | Same principal/tenant/route/key and canonical body yield one run and the same response; changed body conflicts; no cross-principal replay. Verify after dropped HTTP response and across API processes. |
| R09 | Tenant B guesses tenant A's run, approval, project, event cursor, artifact, parent run, and workspace identifiers. Reuse pooled DB connections across tenants. | Read/write/SSE/download all reject. Composite foreign keys reject cross-tenant relations. Tests connect through actual non-owner `NOBYPASSRLS` API role; missing context exposes no business rows. `SET LOCAL` ends with its transaction. Verify role properties from PostgreSQL catalogs. |
| R10 | Viewer calls developer/admin routes; token is revoked or expired; client sends an alternative tenant header/body. | Authorization derives tenant and role from authenticated membership. API tokens are hashed and never logged. Health routes expose no secrets. SSE and downloads enforce the same ownership as normal reads. |
| R11 | Read snapshot while another transaction appends events/updates state; emit events between snapshot and SSE subscription. Lose NOTIFY; reconnect during replay. | State and `covered_seq` share one database snapshot. Replaying strictly after `covered_seq` reconstructs the latest state with no permanent omissions. Polling recovers lost notification. Duplicates are permitted and deduplicated by `(run_id, seq)`. |
| R12 | One SSE client stops reading while others consume; append a retention cutoff during replay; use malformed, negative, huge, expired, and future cursors. | Bounded fanout and per-client queue/write timeout isolate the slow client. Writer/model progress remains bounded. Expired cursor follows a tested snapshot-reset path; an impossible future cursor cannot silently hide completion forever. Reconnect churn has stable goroutine/FD/memory trends. |
| R13 | Multiple workers reserve the final provider slot/token/fee budget concurrently; stream ends without usage; worker dies; reconciler runs twice. | Atomic shared reservation never exceeds configured bounds. Unknown charged requests retain conservative cost; releasing concurrency does not refund billable cost. Reconciliation is idempotent. Use exact integer units and test overflow and negative input. |
| R14 | A tenant floods work while another is queued; approval waits; runner refuses capacity; repeated terminal messages arrive. | Tenant dispatch opportunities are fair under the documented non-preemptive model. Capacity counters match active allocations after every fault. Approval retains storage but releases execution slot. Capacity rejection backs off and cannot hot-loop. |
| R15 | Read/patch paths contain `..`, absolute paths, symlink swaps, FIFOs, devices, or links outside the workspace. Interrupt a multi-file patch publication. | Traversal-resistant filesystem access and allowed file types enforce confinement. No control credentials or baseline can be read/written. Precondition hashes reject stale patches; partial multi-file publication is reconciled, never blindly reapplied. |
| R16 | Command floods both stdout/stderr, forks children, exceeds memory/PIDs/time, or tries the Docker socket/network. Modify `.git` before requesting diff. | Real rootless container settings and effective resource limits are inspected. Both streams are drained with bounded storage. Cancellation stops descendants. Diff is produced against a trusted external baseline and includes new files. |
| R17 | Artifact upload succeeds but DB publish fails; GC overlaps active/unknown runs; artifact path uses another tenant namespace. | Unpublished artifacts cannot be downloaded as completed output. Hash/length validation precedes ready state; GC preserves receipts needed for reconciliation. Download authorization is independent of object key/path. |
| R18 | Provider stream fragments interleaved call IDs, retries after partial output, sends invalid/deep/oversized JSON, or returns missing usage. | Contract tests use raw provider-shaped HTTP streams and assert zero partial execution, correct call/result correlation, bounded parsing, distinct attempt identity, one retry owner, and conservative usage accounting. Paid smoke checks are separately reported. |
| R19 | Agent changes tests or reports success without fixing the target; fixture has unrelated passing tests. | Independent clean verifier applies the patch and runs trusted target plus regression tests hidden from the task workspace. `verified`, `regression_only`, and `unverified` remain distinct. Keep at least three real repair fixtures. |
| R20 | DB restoration or runner disk loss removes evidence while a command may still exist; unsupported snapshot schema is loaded. | Fail closed into documented reconciliation/migration state. Database restoration is not described as proof of workspace recovery. A restart alone is never evidence of successful recovery. |

## Design decisions to freeze before implementation

1. **Define recovery ownership explicitly.** A run can be lease-owned while not
   yet allowed to write. Persist the adoption phase and fence new effects until
   the runner has returned its reconciliation result.
2. **Specify immutable operation identity.** The deduplication key is stable, but
   a matching key is only reusable when tenant/run/workspace/tool/arguments and
   relevant preconditions match. Include operation retention and tombstone rules.
3. **Separate resource dimensions.** Execution slots, open provider requests,
   token windows, accrued charges, and storage have different release conditions.
   A single “reservation expired” path is insufficient.
4. **Keep claim/capability expiry compatible.** Runner tokens cannot outlive the
   current lease. Decide how runner clocks are bounded and how authorization is
   revalidated on retry without erasing receipts from an older epoch.
5. **Bound histories and streaming.** Document limits for event batches, replay,
   text buffers, tool arguments, logs, artifacts, and context. Include future
   cursor behavior and what the client does after reset.
6. **Distinguish demo and release evidence.** FakeProvider and fake execution
   are useful for deterministic control-plane tests, but cannot validate Docker
   isolation, RLS role configuration, remote provider compatibility, or model
   repair ability. Keep benchmark classes and exact commands separate.

## Review exit criteria

The implementation review should link each material finding to exact code lines
and a reproducer. A final evidence matrix must identify whether every plan
requirement is implemented, verified with the specified dependency, deferred as
an explicitly optional extension, or still incomplete. Published performance
figures must include environment, revision, workload, raw output, and limitations.
Repository and resume descriptions must report this as an independently built
project with agent assistance, never imply fictional employment or human review.
