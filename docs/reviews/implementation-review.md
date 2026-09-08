# Independent implementation review log

Date: 2026-09-07. Reviewer: separate review agent. Code is still being developed.
This log is not a release approval; it distinguishes inspected code, reproduced
behavior, fixes, and integration evidence still needed. The reviewer has not
edited application code.

## Pass 1: domain, reducer, and initial migration

Scope: `internal/domain/types.go`, `internal/runtime/types.go`,
`internal/runtime/reducer.go`, their tests, and
`db/migrations/00001_control_plane.sql`.

### IR-01 — Reconciliation could bypass pending workspace adoption

Priority: P1. Status: fixed in the working tree; independent reducer reproducer
confirmed the fix. The real runner interleaving remains an integration gate.

The original `EventReconciled` branch checked run status but not the adoption
stage. Claiming an expired `needs_reconciliation` run changed the stage to
`adopt_workspace`; an effect receipt arriving before `workspace_adopted` could
then settle the old effect and emit a new `execute_effect` command. This omitted
the specified takeover barrier. The developer added a stage check and a
receipt-before-adoption assertion to
`TestUnknownEffectRecoveryNeverReplaysOperation`.

Independent reproduction now returns
`invalid_transition: workspace adoption barrier has not completed`, with no
commands. The reproduction constructed a valid old unknown operation, a queued
second operation, expired epoch 1, and a new epoch 2 claim, then supplied a
settled epoch 1 receipt without an adoption event.

### IR-02 — Malformed snapshots can publish a nonterminal stop result

Priority: P2. Status: fixed in the working tree; independent reproducer and
domain/runtime package tests confirmed the correction.

`validateState` accepted `status=cancel_requested` and `stop_target=queued`.
`confirmStop` checked only that `StopTarget` was nonempty, then assigned it to
`Status` and emitted `publish_terminal`. A separate Go program reproduced the
accepted result `status=queued, stage=stopped, commands=[publish_terminal]`.
Current code locations are the `validateState` and `confirmStop` functions in
`internal/runtime/reducer.go`; initial reviewed lines were 412–427 and 328–345
respectively, before formatting shifted them.

The developer expanded snapshot validation to reject invalid stop targets,
incoherent status/stage combinations, malformed pending/remaining effects,
duplicate operation IDs, and unbound approvals. A regression test and malformed
snapshot fuzz seed were added. Re-running the independent reproducer now returns
`invalid_argument: invalid stop target`, unchanged input state, and no commands.

### IR-03 — Effects can refer to a nonexistent step

Priority: P2. Status: fixed and independently verified against disposable
PostgreSQL with `tests/review/schema_test.go`.

The initial migration's `effects` table declared `step_seq` but only referenced
`runs(tenant_id,id)` (initial lines 86–95). A valid run with an arbitrary missing
step can therefore have an effect inserted. This weakens the plan's requirement
that effects belong to a durable logical step. The migration now includes a
composite foreign key to `steps(tenant_id,run_id,seq)`. The independent regression
test attempts an orphan-step insertion and requires SQLSTATE `23503`.

### IR-04 — Approval run and effect run can disagree

Priority: P2. Status: fixed and independently verified against disposable
PostgreSQL with `tests/review/schema_test.go`.

The initial `approvals` table independently referenced an effect by
`(tenant_id,effect_id)` and a run by `(tenant_id,run_id)` (initial lines 96–105).
Both references can be individually valid while the effect belongs to a
different run in the same tenant. The migration now contains an effect unique
key on `tenant_id,run_id,operation_id` and binds approvals through that key. The
independent regression attempts the mismatched insertion and requires SQLSTATE
`23503`.
Application approval checks still need to bind arguments, workspace revision,
policy, and version.

## Pass 2: persistence, runner, and execution boundaries

### IR-05 — API startup audit missed inherited ownership

Priority: P1. Status: corrected query inspected; actual non-owner RLS behavior
independently verified. Inherited-owner rejection is not yet independently
integration-tested through `CheckAPIRole`.

The original `CheckAPIRole` in `internal/persistence/identity.go` checked direct
table ownership only. A role inheriting a table-owner role could pass that check.
The corrected query checks `pg_has_role(current_user,c.relowner,'MEMBER')` and
enabled RLS across the tenant business tables. The independent SQL test creates
a temporary non-owner, non-superuser, `NOBYPASSRLS` role and verifies missing
tenant context exposes no rows, scoped reads and writes enforce tenant ownership,
and transaction-local context and the test role disappear after rollback.

### IR-06 — Heartbeats and snapshots disagreed on authoritative lease expiry

Priority: P1. Status: corrected and independently verified against PostgreSQL.

`Heartbeat` renewed `runs.lease_until`, but the original `getRun` read the lease
from snapshot JSON. A subsequent transition could reject a renewed owner or
overwrite the renewal with the old expiry. `getRun` now overlays the authoritative
lease columns. `TestReviewHeartbeatRemainsAuthoritativeAcrossTransitions` checks
that a renewal is visible, survives another transition, and permits advancement
after the original expiry.

### IR-07 — Worker commit did not recheck lease expiry after waiting

Priority: P1. Status: fixed and independently verified with an actual PostgreSQL
lock interleaving.

The original `persistTransition` UPDATE filtered only by run version. A worker
could pass the reducer's time check, then wait on evidence access until expiry
and still commit. Worker-event updates now also check owner, epoch, and
`lease_until > clock_timestamp()`. Claim and authorized control paths are kept
separate. `TestReviewLeaseExpiryDuringEvidenceWaitFencesCommit` blocks the actual
artifact SELECT with a PostgreSQL table lock, observes the pending lock in
`pg_locks`, releases it after expiry, and requires a fenced error with no state or
event changes.

### IR-08 — Expired release grant could survive a workspace-lock wait

Priority: P1. Status: fixed and independently verified with a deterministic
runner backend, including `-race`.

`ReleaseWorkspace` originally verified its capability only before waiting for an
active writer. The grant could expire while blocked, after which the old epoch
could still delete a workspace if a replacement epoch had not reached the
runner. Authorization is now checked again after obtaining the workspace lock.
The independent test starts release while a writer holds the lock, advances the
grant clock, settles the writer, and proves the workspace remains intact and the
release returns `ErrFenced`.

### IR-09 — Late cancellation rewrote an observed successful operation outcome

Priority: P2. Status: fixed and independently verified with journal reopen and a
deterministic backend, including `-race`.

`reconcileLocked` previously assigned `cancelled` whenever a cancellation was
requested, even if backend inspection showed the job had already exited zero
before cancellation. It now distinguishes actual interruption from a later
request. The independent test interrupts receipt publication after successful
execution, closes and reopens SQLite, then requests cancellation and requires the
operation receipt to retain `succeeded`. Run cancellation and the operation's
observed outcome remain distinct facts.

### IR-10 — Writable workspace had no execution-time disk bound

Priority: P1. Status: unsafe admission fixed; actual quota-backed execution is
still an integration requirement.

`MaxWorkspaceBytes` bounded imports and tree reads, but a command could write to
the host bind mount without a disk quota while running. Docker admission now
requires a configured workspace quota and a verifier of actual filesystem or
volume enforcement; missing or rejected enforcement returns `ErrUnavailable`.
The independent protocol-stub test verifies rejection precedes any job command,
including under `-race`. This does not prove a real quota backend works. A disk
size poll or free-space check is not equivalent to enforceable quota isolation.

### IR-11 — Deferred running snapshot could not be reclaimed

Priority: P1. Status: fixed and independently verified against PostgreSQL.

`internal/persistence/execution.go` originally cleared `lease_owner` in `Defer`
while leaving the snapshot running. The next claim rejected that invalid running
state before it could install a replacement owner. Defer now expires the lease
while preserving the prior owner and epoch. The independent
`TestReviewDeferredModelRunCanBeReclaimed` drives a run to the model stage,
defers it, proves the previous lease is fenced, and requires a higher-epoch
replacement to enter workspace adoption without changing model accounting.

### IR-12 — Completed model replay used mutable current pricing

Priority: P1. Status: corrected; source and integration regressions independently
checked with PostgreSQL, SQLite, and race detection.

At the reviewed revision, `internal/application/model.go:27-39` chooses the
current configured model specification before loading a completed attempt, and
`completedModel` uses those rates for settlement at lines 207-219. The persisted
attempt price version is ignored. A configuration update between result storage
and replay can change the billed amount or conflict with an already committed
settlement. The attempt needs an immutable pricing snapshot, used for reservation
and every replay. Usage sums also need overflow checks before pricing and token
settlement. Required regression: store a completed result, restart with changed
configured prices, and require the original charge with no new provider call.

The updated source freezes pricing atomically with the attempt, rechecks the
reservation request hash, prices cache classes explicitly, checks token sums,
and keeps reducer budget accounting conservative when late billing decreases.
The reviewer also independently ran the implementation owner's pricing
integration tests under `-race`: crashes before reservation, after raw-result
storage and after settlement retain the original quote despite changed or
removed current configuration. Replaying the same completion makes no extra
provider call or charge. Persisted retry decisions, overall retry budget and
monotonic conservative admission tests also passed. The provider/backend in
these tests is deterministic; they make no paid-provider compatibility claim.

### IR-13 — Verification failure was absent from subsequent model context

Priority: P1. Status: corrected and independently behaviorally verified against
PostgreSQL and SQLite with a deterministic no-process verifier, including race
detection.

At the reviewed revision, `internal/application/context.go:61-123` rebuilds
history from model turns and regular tool effects; negative system-operation
ordinals are excluded. `verify` stores its report and the reducer starts another
context round on failure, but context construction does not read that report or
its diagnostic receipts. The model cannot see what failed, and an explicit
`finish` call receives a fabricated earlier-tool-failure result. Both native
continuation and rebuilt context need the durable verifier outcome and bounded
diagnostics. Required regression: the fixture model repairs only after receiving
the actual verifier failure, then verification succeeds.

The updated source includes `verificationFeedback` and handles `finish`
explicitly. `TestReviewFailedVerifierDiagnosticsEnableNextRepair` now verifies
both portable and native-continuation context paths. Its model deliberately
requests `finish` before making any edit, then refuses to repair unless the next
request contains the exact durable target-failure diagnostic and a correlated
error tool response for the original `finish` call. Only then does it issue a
real file patch. A final `finish` completes with verified state, exactly three
model calls, and two observed failing target checks (baseline and first
candidate). Native continuation also requires the opaque prior native state to
be present. Both cases passed with `-race`. The verifier checks actual file
contents using an explicit test backend; it does not execute Python and makes
no real-code-evaluation claim.

### IR-14 — Cancellation reopened runner admission

Priority: P1. Status: driver correction inspected; see remaining runner IR-18.

At the reviewed revision, `internal/application/driver.go:407-411` invokes
`AdoptWorkspace` for stop execution. Adoption deliberately clears the runner's
admission fence after stopping the old writer, so an already-issued same-epoch
execution capability can still start a delayed request after the stop receipt.
The terminal cancellation path must use `StopWorkspace` with permission
`cancel`, which leaves admission fenced. Required regression: hold an operation
request before runner admission, finish cancellation, then deliver that request
and require rejection without a process start.

The driver now calls `StopWorkspace` with `cancel` permission. IR-18 identifies
a separate delayed-adoption race in the runner's stop fence.

### IR-15 — Caller cancellation counted as a shared provider outage

Priority: P2. Status: correction and classification matrix independently checked.

At the reviewed revision, `internal/application/model.go:143-151` records every
stream failure as a provider circuit failure before identifying its cause.
`internal/quota/store.go:331-333` defines caller cancellation and permanent
invalid input as neutral outcomes. Repeated user cancellations or local sink
failures can otherwise open the shared credential-group circuit and block other
runs. Outcome classification must precede circuit accounting, with a regression
showing cancellations do not consume the provider failure threshold.

The updated code classifies cancellations, local consumer errors and permanent
failures as neutral and persists retry/outcome decisions for replay. This source
correction was inspected. The reviewer independently ran the owner's
classification cases under `-race`, covering user cancellation, request expiry,
database/consumer failures, permanent validation and transient provider errors.
The actual PostgreSQL integration for permanent failure verifies no retry and
no circuit trip. A complete user-cancellation-to-run-stop integration remains a
separate acceptance gate.

### IR-16 — Reconnect could subscribe to an already-stopped SSE hub

Priority: P2. Status: corrected and independently verified with race detection.

`internal/eventstream/hub.go:155-164,185-190` closes subscriptions when a
sequence gap stops the hub, but removal from `Manager.hubs` only occurs in a
subscriber's later `Close` callback. A reconnect in that interval can find the
dead hub, register successfully, and wait forever for live events. The
independent `TestReviewSSEReconnectAfterGapStartsFreshHub` reproduced missing
sequence 4 after a reset at sequence 3, including under `-race`. Hub retirement
must be atomic with registration under the documented lock order. A separate
test verifies the ordinary snapshot-read-to-registration race replays the
intervening event correctly and rejects a future cursor; it passed.

The corrected hub removes itself from the manager during teardown. Both
independent SSE tests passed with `-race -count=30`.

### IR-17 — API startup guard checked a different schema from Store queries

Priority: P1. Status: corrected and independently verified against PostgreSQL.

`internal/persistence/identity.go:80` originally restricted role/RLS checks to
`public`, while Store queries use unqualified names and the connection URL can
select another `search_path`. A non-owner API role initially passes the guard;
after it inherits ownership of the resolved private-schema `projects` table,
the guard still passes because it checks unrelated public relations. The
independent `TestReviewAPIRoleGuardChecksResolvedSchemaAndInheritedOwnership`
demonstrates this with a uniquely migrated schema and temporary NOLOGIN,
NOSUPERUSER, NOBYPASSRLS roles. All fixtures were cleaned. The guard must resolve
the actual business relations, require the complete expected set, and examine
those exact owners and RLS flags, or enforce a fixed public-only namespace.

The corrected guard resolves every expected relation through `to_regclass`,
rejects missing relations and examines the actual owner/RLS flags. The
independent PostgreSQL regression now passes.

### IR-18 — Delayed adoption could undo a same-epoch stop fence

Priority: P1. Status: corrected and independently verified with SQLite journal
reopen and race detection.

`internal/runner/journal.go:269-283` accepts equal-epoch adoption and clears the
same `adopting` flag used by `StopWorkspace`. A stop receipt therefore does not
survive an already-issued adoption request arriving late under the same lease.
`TestReviewStoppedWorkspaceRejectsDelayedSameEpochAdoption` prepares and stops a
workspace, confirms execution is initially fenced, and then observes successful
same-epoch adoption reopening it. Stop authority needs durable representation
that a delayed equal-epoch adoption cannot undo. This is distinct from the
driver choosing the wrong stop method in IR-14.

The updated runner persists a distinct stopped flag and refuses adoption that
would reopen it. The independent test now closes and reopens the SQLite journal
after obtaining the stop receipt, then requires both delayed same-epoch adoption
and execution to remain fenced. It passed under `-race`; it uses a deterministic
no-process backend and does not replace real-container cancellation evidence.

### IR-19 — JSON serialization changed accepted hashed arguments

Priority: P1. Status: control-plane correction independently verified against
PostgreSQL with race detection; runner admission correction pending.

`persistTransition` accepts raw effect arguments and hashes, then uses default
Go JSON serialization for snapshots and commands. Even with PostgreSQL `json`
columns, that serializer escapes literal `<`, `>`, and `&` inside
`json.RawMessage`. `TestReviewHTMLCharactersPreserveToolArgumentHashAcrossPersistence`
accepts a patch containing `x < y & z > w` and observes different argument bytes
on reload with the original hash. The current model-response artifact serializer
may incidentally escape those characters before the driver's later hash step;
the independent reproducer calls the valid public Store boundary directly.
Canonical arguments must be explicit and enforced before planning, or carried
as opaque bytes through every envelope. An already dispatched effect must never
be silently rehashed. Runner journal request serialization has the same
RawMessage representation and should follow the same invariant.

The corrected domain protocol explicitly defines canonical argument JSON, and
the reducer/system-effect planning rejects noncanonical input before hashing or
execution. The driver canonicalizes before creating its initial hash. The
independent regression now requires rejection of noncanonical HTML with
unchanged state/event sequence, then checks a separately specified canonical
literal containing HTML escapes and integer `9007199254740993`. Snapshot and
pending-command bytes and hashes survive the PostgreSQL round trip. This test
passed under `-race`.

Further integration issues were reported to implementation owners: source
`base_commit` must be bound to the actual immutable import; executable modes must
survive snapshots and diffs. Source binding checks and a 64-message/256 KiB
aggregate message limit are now visible in code. The reviewer has not yet
independently exercised these newer corrections. Potential billing-total
decrease after late unknown-reservation settlement was also reported as a
source-analysis concern, not a reproduced failure.

## Verification performed

The reviewer independently ran:

```sh
GOCACHE=/tmp/forge-review-go-cache go test ./internal/domain ./internal/runtime
```

Both packages passed on the observed working tree. Money tests include an
arbitrary-precision cost oracle and boundary cases; no money arithmetic bug was
identified in this pass. The reducer copies effect argument bytes, pending
effects, remaining effects, and approvals before mutation; no successful-path
aliasing bug was identified in the inspected code.

The independent reproducer was run from `/tmp/forge-independent-review` with a
local module replacement. Its two observations are recorded above. Later checks
used `tests/review`: SQL constraint/RLS tests create rollback-only fixtures;
actual Store tests create and clean a unique private schema in the explicitly
selected disposable database. Tests require a loopback URL and
`FORGE_REVIEW_ALLOW_FIXTURES=1`. No production or unrelated database was changed.

Runner regressions use real files and SQLite with a deterministic no-process
backend. The quota-admission test uses a local Docker protocol stub. These checks
do not substantiate real container isolation, provider compatibility, coding-task
success, or performance claims. Those remain covered by `design-review.md`
acceptance gates.

## Continuation review — 2026-09-08

### IR-20 — Fixed token reservation did not bound actual model input

Priority: P1. Status: source-confirmed defect; correction independently verified
against PostgreSQL with race detection.

`application.callModel` originally reserved `ModelSpec.ContextTokens` without
checking that the persisted request fit that value. `buildContext` imposed a
512 KiB wire limit, and provider request validation checked output limits, but
neither checked input against the frozen reservation or the model's context
window. Thus a small configured input quote did not establish a conservative
cost/token upper bound for a larger task, tool schema, or native history.

The independent `TestReviewModelInputMustFitFrozenReservationBeforeDispatch`
constructs an actual durable context containing 2,100 bytes of user text with a
one-input-token frozen quote and a two-token shared quota. A probe provider
counts dispatches without contacting an external service. The implementation
owner added the input-bound check before the reviewer ran the corrected
reproducer successfully, so this review does **not** claim an independently
executed failing test on the old implementation. On the corrected tree the test
passes under `-race`: zero provider dispatches. The fix checks the full portable
request and native payload with the provider's conservative wire-byte bound,
includes framing allowance, compares against the frozen input quote and context
window, and abandons the unissued reservation on rejection.

### Actual non-owner HTTP integration

`TestReviewHTTPUsesRealNonOwnerTenantScopeAndArtifactBytes` independently passed
under `-race` using a uniquely migrated PostgreSQL schema and a temporary
`NOLOGIN NOSUPERUSER NOBYPASSRLS` role. The connection pool sets that actual role;
the API role guard passes and the role owns no tested business relation. This
test does not assert the deployment entrypoint grants the minimum possible SQL
privileges: it explicitly grants fixture table access so RLS and HTTP authority
can be exercised independently of deployment bootstrapping.

The test verifies:

- Missing, revoked, and wrong-membership credentials return 403; error bodies
  carry the same generated request ID as the response header.
- A principal belonging to two tenants cannot fetch one tenant's run through
  the other tenant context; the response is 404.
- Viewers can read their run but cannot cancel it or add a message.
- Two tenants can have the same artifact ID with different real local bytes;
  each authorized download returns only its tenant's bytes, and a cross-tenant
  artifact listing returns 404.
- The actual SSE handler replays the committed `run.created` and
  `artifact.ready` events through the non-owner pool; a future cursor returns
  409 before streaming.

The prior PostgreSQL heartbeat, commit-time expiry, defer/reclaim, resolved-schema
ownership, canonical HTML/large-integer, and SSE hub regression tests also
independently passed with race detection in this continuation. At the initial
continuation snapshot, IR-18 still reproduced; its runner correction was in
progress at that point. A later independent SQLite-reopen regression now clears
the journal admission issue as recorded under IR-18.

### IR-21 — Retained SSE history gaps were silently skipped

Priority: P2. Status: corrected and independently verified with PostgreSQL, the
real HTTP handler, and race detection.

At this review snapshot `retained_from_seq` was not enforced by `Store.Events`
or `Manager.Subscribe`. The HTTP historical replay also did not assert adjacent
sequence numbers. Deleting earlier events would therefore allow an expired
cursor to skip missing history or obtain an empty 200 response instead of the
specified 410 reset. `TestReviewHTTPExpiredCursorRequiresSnapshotReset`
independently reproduces both an already expired cursor and a cutoff advancing
after the subscription's snapshot read but before historical replay. The test
atomically updates its private-schema cutoff to 3 and removes events 1 and 2;
both cases originally returned 200 and streamed events 3 and 4 to cursor 0.
`Store.Events` also returned no reset error for that expired cursor. The test
deliberately isolates cursor enforcement from the collector's run-eligibility
policy. The corrected tree carries `RetainedFromSeq` through the consistent run
read, rejects expired cursors in `Store.Events` and subscription admission,
checks adjacent historical sequence numbers, and emits 410 if a history failure
is detected before streaming. Both independently reproduced cases now pass
under `-race`.

`TestReviewTrimEventsPreservesActiveStatesAndReplayEvidence` independently
exercises the actual `TrimEvents` transaction. Fixtures reach their states
through Store/reducer transitions: old completed, failed, cancelled and
budget-exhausted runs are trimmed to exactly two tail events; running,
waiting-approval, needs-reconciliation, cancel-requested and queued runs remain
intact, as does a recent terminal run. The retained boundary equals the first
remaining sequence, and a repeated trim removes zero events. The source's
initial unexported pgx row-mapping fields were corrected by the owner before
this successful independent execution.

The same test replays every newly recorded noninitial snapshot by calling the
pure reducer with its stored `input_state` and `input_event` and compares the
entire resulting JSON to the committed snapshot, both before and after trimming.
It includes heartbeat renewal between transitions, approval, an uncertain
effect, verification and all terminal paths. A simulated legacy record retains
NULL inputs after trimming; this is explicitly unknown replay history, not
evidence that old v6 records can be reconstructed. These checks passed with
race detection. They validate event trimming, not artifact or workspace GC.

### IR-22 — Fixed worker transport ignored claimed runner placement

Priority: P1. Status: source-confirmed defect; corrected and independently
verified against PostgreSQL with race detection.

The worker entrypoint originally loaded a required `RunnerID` but used
unrestricted `Store.Claim`, while its Driver always dispatched through one
configured runner connection. With another enabled runner registered, run
placement and capacity could name a different runner from the actual transport.
The correction adds `ClaimOnRunner`, filters both new placement and existing
allocations, wires the worker's runner ID into the Driver, and rejects a supplied
claim for another runner before execution begins.

`TestReviewFixedRunnerClaimAndTakeoverRemainOnAssignedTransport` verifies two
registered runners each claim their own run, a mismatched Driver rejects before
any dependency call, and an expired run cannot be adopted by the other runner.
After process-free deferral releases an allocation, the same workspace affinity
still holds. Higher-epoch takeover succeeds on the original runner and each
runner retains exactly one slot. This test checks actual PostgreSQL placement
and the Driver's early guard; it makes no additional container/transport claim.

After the retention and placement changes, the complete independent review
package passed with `go test -race ./tests/review -count=1` against the explicitly
selected local review database (9.290 seconds). The subsequently strengthened
IR-18 reopen case passed separately with race detection. All SQL review fixtures
used temporary schemas/roles and were cleaned by the tests.

### Explicit terminal workspace cleanup review

The reviewer inspected migration 8, `persistence/cleanup.go`,
`application/cleanup.go`, the operator command, runner cleanup-purpose grants,
and the existing Stop/Seal/Release/storage ordering. No new blocking defect was
identified in the implemented complete-workspace cleanup path. The cleanup
record has its own 30-second database lease and stable fencing epoch; it does
not reopen the run or extend its execution lease. `release` permission is only
issued after the cleanup row references a READY code-snapshot artifact.

`TestReviewCleanupRequiresTerminalSettledAgedStateAndIndependentLease` passed
with race detection using a private PostgreSQL schema. It rejects active,
approval-waiting, reconciliation and cancel-requested states, an insufficiently
aged terminal run, an unreleased allocation, unknown/in-flight effects and a
failed effect missing its final receipt. It also checks that concurrent cleanup
owners are excluded, the old owner is fenced after lease takeover, the cleanup
identity/runner epoch stay fixed, release proof is unavailable before sealing,
and absent artifact metadata cannot seal a cleanup.

The existing independently driven repair fixture now has two cleanup subcases:
`cleanup_after_snapshot_publication` and
`cleanup_after_release_before_commit`. Each injects an error at that exact
application boundary, closes and reopens the real SQLite runner, expires only
the cleanup lease, then resumes under another cleanup owner. Both pass. They
verify that snapshot publication alone retains the files, successful release
removes them, and recovery/repetition yields one `workspace.released` event
without another execution-capacity refund. The entire terminal reducer state,
including the original execution lease, remains byte-identical. The saved
artifact is reopened after workspace deletion and its `clamp.py` bytes match
the actual completed repair. It is a filtered **code snapshot**, not a complete
forensic copy of the volume.

The reviewer independently ran the targeted cleanup/repair group with `-race`
(4.609 seconds). The main agent's separately captured full-suite output also
contains successful executions of these tests:
[`validation-race-20260908.jsonl`](../../benchmarks/results/validation-race-20260908.jsonl).
The backend in these tests starts no process and uses ordinary temporary
directories; this evidence does not establish physical ext4 slot reuse after
cleanup. Real fixed-volume/container checks belong to the runner integration
evidence.

One retained limitation was reported to the implementation owner: preparation
allocates a physical volume lease before creating the workspace journal row.
If preparation fails in that window, a terminal revision-zero run can have a
retained volume lease while Stop/Seal/Release return workspace-not-found. The
current cleanup therefore preserves that state rather than reclaiming it. Do
not infer absence of physical allocation from workspace-not-found or describe
every terminal initialization failure as automatically recoverable by this
command. Supporting it needs a separately bound aborted-preparation receipt or
an explicit reconciliation operation; the reviewer did not authorize a blind
directory deletion fallback.

This continuation does not establish real rootless-container behavior, three
successful code-repair fixtures, benchmark targets, minimum-privilege deployment,
or GitHub publication. The repository contains substantial transactional and
protocol work suitable for explaining actual engineering decisions; describing
the full baseline as complete still requires those integration artifacts and
the final requirement/evidence map.

### IR-23 — Shell-sourced service URLs lost multi-parameter connection strings

Priority: P2. Status: reproduced, corrected and independently verified.

`localsetup.Setup` originally wrote `FORGE_DATABASE_URL=<URL>` without shell
quoting to `api.env` and `worker.env`. A valid loopback URL containing
`?sslmode=disable&connect_timeout=5` was therefore interpreted as a background
assignment when sourced using the documented startup command. The parent shell
lost the service URL or retained an inherited administrator URL; service startup
then failed. The reviewer reproduced the missing variable with a temporary env
file and entirely fictional credentials, without connecting to a database.

The correction applies single-quote shell escaping only to these service URLs.
The `client.env` format remains compatible with the demo script's restricted
line parser. The reviewer independently ran
`go test ./internal/localsetup -count=1 -v`: the actual `/bin/sh` source test
passed for a multi-parameter URL, quotes and literal substitution syntax, with
an inherited administrator fixture value. This verifies literal round-trip
behavior rather than only comparing the generated quoting string.

### IR-24 — Fresh checkouts could not follow volume preparation instructions

Priority: P2. Status: reproduced, documentation corrected and independently
verified at the unprivileged directory boundary.

Tracking `var/go.mod` causes an ordinary umask-022 checkout to create `var/` with
mode 0755. The volume preparer correctly requires mode 0700, while local setup
only creates the private child `var/local`. The original README sequence did
not establish the parent mode. In a temporary checkout-shaped directory, the
reviewer ran the actual copied preparer and observed exit 1 with
`var must be runner-owned with mode 0700`, before storage or images were created.
The same guide also used three absolute mount-helper paths specific to the
original development machine.

The guide now checks that `var` is an ordinary directory owned by the current
user before explicitly applying mode 0700, and derives the privileged helper
path from the quoted current checkout path. The reviewer executed the exact
unprivileged permission command in a temporary directory and then opened the
real `Tree` boundary successfully. No image was allocated and no privileged
mount action was performed during this review. The production mode, ownership
and no-symlink checks were preserved.

### Final bounded review and finite SSE evidence

The final review examined operator configuration, local setup, API/worker/admin
entrypoints, patch export, terminal cleanup, generated-file checks, CI and the
README reproduction chain. IR-23 and IR-24 were the only newly confirmed P1/P2
blockers in that bounded pass and are closed above. The current protobuf
generated-file checker was also independently executed successfully. This does
not claim an exhaustive security audit or review the concurrent new Go-profile
implementation; the owner retains the final all-package and clean-checkout gates.

The reviewer authored the separate fixed-window SSE and connection-churn
harnesses; the owner executed them with actual PostgreSQL and loopback TCP.
The reviewer then independently recomputed the SSE percentiles and sequence
checks from all 10,800 raw healthy-delivery records. Twenty runs persisted
1,200 events with exactly 1,024-byte JSONB payloads over 12.009158 seconds
(99.923741 events/s). All 180 healthy readers received ticks 1 through 60 in
consecutive sequence, with zero duplicates, gaps or errors. Nearest-rank p95
was 96.131086 ms and p99 96.644534 ms. Source hashes in the report matched the
reviewed files. Twenty non-reading clients caused no early handler return or
queue overflow in this short sample; their actual disconnection was therefore
not demonstrated. The combined API/client process returned from 442 FDs and
1,026 connected-state goroutines to its baseline of 42 and 6, respectively.

The five-cycle churn report independently records 50 actual TCP connections
and 50 active handlers per cycle, followed by zero of each and exactly 50
handler returns. Every closed-state sample had 42 FDs and 6 goroutines. Heap
allocation after GC rose from the pre-warmed 4,543,072-byte baseline to
5,923,904 through 6,199,096 bytes across the five rounds; final heap and
goroutine profiles exist. These are finite lifecycle measurements, not proof
of long-term absence of leaks. See [the experiment protocol](../../benchmarks/SSE.md),
[SSE raw samples](../../benchmarks/results/sse-20260908T082217.145386435Z.json)
and [churn report/profiles](../../benchmarks/results/churn-20260908T082535.705148309Z/report.json).
