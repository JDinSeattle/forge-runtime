# ADR 0001: Make execution receipts the recovery boundary

Date: 2026-09-07

Status: Proposed architecture; implementation and acceptance remain subject to the gates in [the implementation map](../implementation-map.md).

Scope: The mandatory single-runner, controlled-tenant SDE baseline in `SDE_IMPLEMENTATION_PLAN.md`, sections 1–18 and 20.

## Product problem

A coding agent issues `apply_patch` or a shell command. The command can succeed while its worker loses the reply. Retrying the model or replaying the tool can now overwrite new files, start a second process, or report an outcome that was never verified. A database lease tells the platform which worker may advance a run; it does not tell the platform whether an earlier process is still modifying a workspace.

Forge Runtime will make that gap visible and actionable. Given a run and operation ID, an operator should be able to answer: what was authorized, what was durably accepted, what actually ran, what evidence is available, and why execution may or may not continue. This is the product thesis behind **receipt-first execution**. It is an application of established journals, idempotency and fencing techniques to coding-agent operations, not a claim to have invented exactly-once execution.

The initial product is a CLI and HTTP API around a Go control plane, multiple workers, one independently restartable runner, PostgreSQL, and isolated task containers. It must actually produce and independently validate code patches. A receipt explorer that only displays simulated jobs would not meet that objective.

## Decision

Separate three records with different authority:

| Record | Owner | What it establishes | What it does not establish |
| --- | --- | --- | --- |
| Intent | PostgreSQL `effects` | A particular step requested an immutable operation with canonical arguments, policy, expected workspace revision and stable operation ID. | That the runner received or executed it. |
| Acceptance and execution journal | Runner SQLite, `WAL` with `synchronous=FULL` | The runner durably accepted the operation; its stable job identifier, observed lifecycle and highest workspace epoch. | That PostgreSQL has acknowledged its final outcome, or that disk loss is recoverable. |
| Receipt and published reference | Runner journal/artifact plus PostgreSQL reference | An observed result, its provenance and the hashes/revisions needed to reconcile it with the requested operation. | That model-written assertions are true, arbitrary remote side effects are observable, or an unverified task is fixed. |

PostgreSQL remains authoritative for run state, worker ownership, approvals, quota reservations and published events. The runner is authoritative for local job observations and its workspace write gate. Neither silently reconstructs the other's missing facts by replaying side effects. This is not a distributed transaction: explicit pending and reconciliation states cover the gaps between stores.

### Minimum receipt contract

The implementation must use typed, versioned references in Protobuf and domain types. The following fields are a contract sketch, not a second arbitrary-JSON protocol:

```text
schema_version
tenant_id, run_id, workspace_id, operation_id
accepted_epoch, tool_kind, canonical_args_hash, policy_version
expected_revision, observed_before_revision, observed_after_revision
operation_state: prepared | running | succeeded | failed | cancelled | unknown
stable_job_id, runner_id, accepted_at, observed_at
exit_code? and termination_reason?
input_file_hashes[], output_file_hashes[]
result_artifact_ref?, stdout_ref?, stderr_ref?, truncation_metadata?
evidence_kind: job_inspection | file_reconciliation | confirmed_cancellation
```

A receipt must match its persisted intent before it can resolve an effect. A content hash detects corruption or mismatch; it is not authentication or independent proof of execution. Internal transport uses service identity and execution capabilities, while task containers cannot write the journal or control artifacts. Clock timestamps aid diagnosis; ordering and authorization rely on committed versions, database time, epochs and validated token deadlines.

`unknown` is a first-class result of an observation attempt. It is not success, failure, or permission to retry. Receipts retain the actual terminal job outcome even when the run ends cancelled or failed; the run state must not rewrite history.

### Operation protocol

1. The worker validates complete model output and tool arguments. It commits the effect intent and stable operation ID before contacting the runner.
2. A current lease check authorizes a short-lived capability for the exact tenant/run/workspace/epoch and permissions. Its expiry is no later than the remaining database lease, less clock-skew allowance.
3. Under the workspace write gate, the runner checks capability, ownership, highest epoch, argument hash and expected revision. Existing operation ID plus identical identity returns the existing operation. An incompatible reuse returns `FAILED_PRECONDITION` without execution.
4. The runner journals `prepared` and a stable job name before launch. `StartOperation` confirms durable acceptance, not completion. For commands, use an inspectable container/job lifecycle instead of only an in-memory process handle.
5. The worker polls `InspectOperation`. The runner publishes observations and final receipt. PostgreSQL commits the matching result reference, next snapshot, and business event in one short transaction.
6. A timed-out start is followed by inspection of the same operation on the original runner. If inspection proves the original intent was not accepted, retry it with the same operation ID. Allocate a new ID only for a distinct authorized logical action after the previous action is resolved. Uncertain observation pauses dependent execution.

No SQL transaction stays open across model calls, Docker launches or RPC waits. Large results go to staged artifacts first; publication is a separate checked transaction. Orphans are recoverable storage states rather than completed outputs.

### Ownership is not execution termination

`lease_epoch`, workspace `revision`, and run `version` are separate values:

- Epoch fences who may request new actions. A newer epoch does not reverse an old action.
- Revision identifies the actual workspace content boundary against which an action was planned.
- Run version prevents conflicting committed state transitions.

On adoption, the runner first persists the new fence so older workers cannot start work, then inspects, waits for or stops the previous writer. It records evidence that the writer completed or stopped before completing adoption. Only then can the new owner write. Waiting for approval releases scheduling capacity while retaining storage and unresolved-effect evidence.

Business cancellation uses `CancelOperation` and executor confirmation. Cancelling an RPC only stops waiting for that RPC. A run remains `cancel_requested` until controlled jobs have been confirmed stopped; loss of the runner yields a reconciliation requirement, not an optimistic `cancelled` event.

### A concrete differentiator

The CLI's run inspection should expose a compact explanation derived from ledger facts:

```text
operation: op_example   run state: needs_reconciliation
intent: committed      runner: runner_local_1
accepted: confirmed    final receipt: unavailable
last observation: command container is still running
next permitted action: inspect or cancel the existing operation
blocked action: launching another workspace writer
```

This explanation should have an API representation and fixture-backed tests; it must not be prose invented by a model. During the worker-crash demonstration, the second worker should recover the already-recorded model response, inspect the original operation and attach the existing receipt. The demo should show the actual operation count and resulting file content.

The engineering distinction is the end-to-end treatment of uncertainty: the API, runner, event stream, quota accounting and operator explanation agree on the same evidence. A generic agent loop with a retry wrapper would not provide that property. Whether this design improves recovery time, duplicate-operation rates or operator diagnosis must be measured; no improvement figures are preclaimed.

## Alternatives and tradeoffs

| Alternative | Benefit | Reason for the baseline decision |
| --- | --- | --- |
| Run tools directly in each worker | Fewer services and less RPC overhead. | Worker restart loses process observations and makes cross-worker workspace ownership difficult to establish. Use this only as a bounded Gate B development adapter, never as proof of runner recovery. |
| Temporal as the authoritative run engine | Durable history, timers, retries and mature operational tooling. | The project's SDE objective includes implementing a deliberately small state machine and SQL scheduling. Temporal remains a credible production alternative; its Activities still need idempotent external effects. A replacement must remove the old authoritative scheduler. |
| Replay all unacknowledged tools | Simple recovery implementation. | Arbitrary shell tools may already have run. Lack of acknowledgement is not evidence of absence. |
| Put journal and workspaces entirely in PostgreSQL | One operational datastore for more metadata. | Command execution and file mutation still fall outside its transaction. Large files and process observations require a runner boundary regardless. |
| Start with multiple runners and snapshot migration | Better placement and host-failure coverage. | Requires trustworthy snapshots and proof the old writer stopped. The initial runner stays fixed per workspace; host or volume loss is explicitly outside automatic recovery. |
| Adopt a message broker immediately | Independent delivery and buffering features. | PostgreSQL already owns acceptance and durable events. Polling plus best-effort notifications suffices for the initial workload; profiling must justify another distributed state boundary. |

Costs accepted: extra round trips for acceptance/inspection, durable journal writes, storage for evidence, reconciliation branches, and operator intervention for irreducible uncertainty. SQLite WAL durability does not make a lost or corrupted runner volume recoverable. Rootless Docker is a controlled-deployment boundary with resource checks, not an absolute guarantee against arbitrary malicious code. Default command networking is disabled; unrestricted external side effects would need additional policies and cannot inherit an exactly-once claim.

## Acceptance evidence

The receipt contract is accepted only when real tests show all of the following:

1. A duplicate operation ID with matching arguments starts one actual command. Mismatched arguments start none and return a stable conflict.
2. A worker killed after command launch recovers on a second worker without a second launch. Verify journal, database, actual command counter, filesystem, quota and event sequence.
3. A crash after patch publication but before PostgreSQL effect completion resolves through matching before/after hashes and the original receipt.
4. A higher epoch cannot overlap an older workspace writer; expiry alone never implies the old command stopped.
5. A timeout and unavailable runner keep unknown work paused on its original runner. Recovery inspects real jobs before accepting new writes.
6. Cancellation is confirmed against the full controlled container/process tree. Late results never reopen a terminal run.
7. Model stream fragments never execute; a recorded complete turn is reused across snapshot recovery. Unknown usage stays conservatively accounted.
8. SSE and CLI inspection show committed state and evidence, survive reconnection and retain tenant authorization.

The complete requirement set and raw-evidence gates remain in [the implementation map](../implementation-map.md). Unit tests of a reducer alone do not establish these distributed properties.

## Source basis

The parent SDE plan, researched on 2026-09-07, is the baseline specification. Relevant primary references supplied by it are [Temporal Activity definitions](https://docs.temporal.io/activity-definition), [PostgreSQL locking reads](https://www.postgresql.org/docs/current/sql-select.html), [PostgreSQL notifications](https://www.postgresql.org/docs/current/sql-notify.html), [Go constrained filesystem access](https://go.dev/blog/osroot), and [Docker rootless mode](https://docs.docker.com/engine/security/rootless/). Dependency and protocol versions must still be checked, pinned and contract-tested at implementation time. This ADR records a project decision, not a claim of new upstream releases or industry adoption.

The repository should present independently built work, measured results and technical decisions. It must not invent an employer, production users, paid customer usage, incident history or performance metrics. Interview material can describe realistic failure cases through reproducible demonstrations and actual engineering evidence.
