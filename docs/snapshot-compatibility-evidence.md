# E37 — Unsupported snapshot compatibility holds, 2026-09-11

The final targeted PostgreSQL/HTTP suite passes under `-race`: **nine compatibility
tests, eighteen leaf cases, and a separate additive upgrade test**, with package
time **15.685 s**. This run includes the rebuilt `forge-admin` subprocess and
existing TCP SSE connection tests. It ran on base `a0b3a5da` plus the preserved
E37 source changes. Three header/Unicode guard tests pass under `-race` in
**1.532 s**. These checks use unique private schemas. The running deployment and
public schema have **not** been migrated or restarted by this acceptance.

Evidence includes the [original defect](../benchmarks/results/snapshot-compatibility-20260911/before-failure.log),
[final PostgreSQL/HTTP/CLI execution](../benchmarks/results/snapshot-compatibility-20260911/complete-after-review.log),
[operator CLI execution](../benchmarks/results/snapshot-compatibility-20260911/operator-cli-race-pass.log),
[derived report](../benchmarks/results/snapshot-compatibility-20260911/report.json),
and [raw/source manifest](../benchmarks/results/snapshot-compatibility-20260911/manifest.json).
The source snapshots were copied after execution; this is not a retained test
executable or full build-input attestation. The operator executable's SHA-256
and size are recorded, but that binary is not included in the evidence bundle.

## Failure and compatibility boundary

Before the change, `getRun` decoded a schema-2 body into the current `State`
structure. GET returned HTTP 200 with that incomplete current-schema projection.
Claim then reached the reducer, rejected the version and rolled its entire
transaction back. In the retained five-claim reproduction, the other tenant's
healthy run claimed first; the incompatible run then caused four identical
errors, leaving its own tenant's healthy run unclaimed.

The new decoder streams the top-level members before decoding state or commands.
Exactly one case-folded version candidate must exist, with the exact decoded key
`schema_version` and the supported integer spelling. It rejects duplicate exact
keys even when both values are 1, ASCII case aliases, and Unicode long-s aliases
(including escaped spellings). A map lookup cannot enforce this contract because
it discards duplicates; decoding directly into `State` also accepts case aliases.
The direct SQL authority gate uses `json_each`, matching ASCII and U+017F folds
without depending on database collation, and rejects the same ambiguities. An
exhaustive offline Unicode guard verifies that this mapping remains complete for
the version field under the current Go Unicode tables.
Unknown versions, missing or malformed headers return
`snapshot_migration_required`. The API returns **409, retryable false**, with
fixed operator guidance, without returning a current-schema projection of an
unknown body. New SSE admission has the same response. An existing SSE hub retires
its generation when a later database read detects the mismatch. Artifact listing and
download use their own tenant-scoped metadata and remain available.

Migration 11 adds four nullable operational columns to `runs`: hold timestamp,
reason, observed schema and original snapshot SHA-256. They inherit the existing
RLS policy. A hold is separate from `runs.state`, the reducer snapshot and its
version. The existing worker's claim transaction acquires tenant then run locks,
checks the header before allocating a runner, and commits the hold plus tenant
rotation once. Both scheduler candidate queries exclude held runs. A claim
handles at most one incompatible candidate, avoiding an unbounded scan loop.

That transaction does not change snapshot text, pending commands, state,
version, lease, effects, model attempts, quotas, runner allocation or historical
snapshots/events. Incompatible state cannot produce fresh execution authority:
the Driver's existing `GetRun` boundary rejects it, and direct Heartbeat,
LeaseProof, Defer, worker event and transition-CAS paths also refuse it. Old
terminal states cannot authorize new cleanup proofs, advance cleanup, enter
whole-task retry, or lose their event history through retention trimming.

The hold does **not** revoke a capability already issued to the runner or prove
that a previously started job has stopped. Unknown operations, charges and
allocations remain reserved. This can legitimately exhaust a tenant's or
runner's capacity until an operator restores a compatible recovery path. The
fairness claim is that an incompatible candidate no longer monopolizes claim
selection; it does not promise spare physical capacity in that situation.

## Actual assertions and evidence

| Boundary | Verified behavior |
| --- | --- |
| Queue fairness | One incompatible oldest run and two healthy runs, including the same tenant; both healthy runs claim. The incompatible run gets one stable hold. Eight concurrent claims after reopening the Store do not select or modify it. |
| Unknown obligations | A future version larger than `uint32` preserves exact snapshot/command/history text, a pending unknown effect, prepared model attempt, dispatched unknown reservation of 1,234 micro-USD, active request/runner capacity and all existing run events. |
| Execution authority | Driver exits before the recording runner boundary; Heartbeat, LeaseProof, Defer, worker event publication and cleanup admission are refused. The original lease fields stay unchanged and expire naturally. |
| HTTP | GET, snapshot, new SSE, cancel, resume and approval return the explicit compatibility error. Foreign-tenant run and artifact reads remain 404. An existing artifact is listed and downloaded with its original bytes. |
| Operator rights | The API role cannot call raw inspection or unhold. Unknown versions cannot be force-cleared, current supported bytes remain held until explicit operator action, stale version/hash fails, and an unexpired lease prevents clear. |
| Safe clear | After the fixture restores a previously saved complete known v1 body, unhold changes only its four metadata fields. The ordinary scheduler can claim it afterward. |
| Malformed headers | Null, fractional, quoted, negative and excessively large version values remain held without panic, repeat selection or rewriting. Unit coverage also checks missing/case-mismatched members, exponent form and duplicate members. |
| Ambiguous members | Six preserved-text cases cover both ASCII alias orders, duplicate 2→1, duplicate 1→1, literal long s and escaped long s. HTTP returns 409; a live lease cannot issue proof, renew or publish a worker event; the expired run gets one hold and the same-tenant healthy run claims. |
| Existing SSE | An admitted TCP connection ends without client cancellation after the source reports the compatibility error; a new connection returns 409. The final fixture observation, including mutation and new admission, completes in 16.165 ms; this is not a latency benchmark. |
| Terminal evidence | An already acquired cleanup record cannot mint another proof or advance after the run body becomes incompatible. Retention deletes zero events, and retry creates no idempotency key. |

Full before/after records are in
[unknown-ledger.json](../benchmarks/results/snapshot-compatibility-20260911/complete-after-review/unknown-ledger.json),
[HTTP/operator observations](../benchmarks/results/snapshot-compatibility-20260911/complete-after-review/http-operator.json),
[approval records](../benchmarks/results/snapshot-compatibility-20260911/complete-after-review/approval.json),
and [terminal evidence](../benchmarks/results/snapshot-compatibility-20260911/complete-after-review/terminal-evidence.json).
The [existing SSE observation](../benchmarks/results/snapshot-compatibility-20260911/complete-after-review/existing-sse.json)
records both the old connection and subsequent admission. The [actual CLI stdout/stderr](../benchmarks/results/snapshot-compatibility-20260911/complete-after-review/operator-cli.json)
includes the refused future-version clear and successful guarded clear.

The API fixture uses an administrator connection with `SET ROLE` to a restricted
nonowner `current_user`, whose RLS startup check passes. Its actual identity is
recorded; this is not a separate restricted LOGIN experiment. The unknown effect,
model dispatch marker and stop receipts are explicit control-plane fixtures.
There is no provider request, Docker operation, restored runner SQLite database
or paired workspace-volume recovery in this acceptance. Artifact bytes are real.
The positive unhold fixture restores a saved complete known body; no production
code converts a future snapshot or merely changes its version number.

The [new additive rehearsal](../benchmarks/results/snapshot-compatibility-20260911/upgrade-after-review/postgres-upgrade.json)
applies PostgreSQL migrations 6 → 11 twice. All original columns and evidence
remain unchanged, priority defaults to zero, and the new hold columns default
to NULL. Historical 6 → 8 and 6 → 10 evidence remains unchanged.

## Operator procedure

First deploy migration 11 using the intended database's migration identity:

```sh
bin/forge-admin migrate
```

The new API/worker binary requires those columns. Migration adds metadata; it
does not make an old binary understand new snapshot versions or participate in
hold enforcement. Do not leave old mutating binaries active during a snapshot
format conversion. Coordinate stopping claims and active execution, preserve
the original runner/journal/workspace pairing, and take the documented paired
backup before any actual conversion. A compatibility hold is not a stop receipt.

Inspect a selected run with the operator identity and save its output privately:

```sh
umask 077
bin/forge-admin -tenant TENANT -run RUN snapshot-inspect > snapshot-review.json
```

Inspection is read-only and requires a table-owner-capable or superuser operator
role. It reports the SQL version, status, runner, lease expiry, current hash,
header support and any observed hold. `snapshot_text` is a JSON **string** which,
when decoded, preserves the exact PostgreSQL `json` column text. Avoid converting
it through a generic floating-point JSON state map. Raw inspection can contain
task data and is not exposed through the public API.

Install a binary which explicitly supports the stored version, or run a separate
reviewed, version-specific conversion that preserves effects, identities, money
and unresolved execution. No converter or `--force-schema` option is supplied by
this feature. If compatibility cannot be established, leave the run held.

After a fresh inspection, use the **current** SQL version and hash. The command
below never writes snapshot bytes, clears pending effects or releases capacity:

```sh
bin/forge-admin -tenant TENANT -run RUN \
  -expected-version REVIEWED_VERSION -snapshot-sha256 REVIEWED_HASH snapshot-unhold
```

Unhold locks tenant then run, checks the exact version/hash, validates the full
supported state and its SQL identity/status/version, and requires no live SQL
execution lease. It clears only compatibility metadata. Existing runner affinity,
operation IDs and reconciliation still govern later execution. API `resume`
cannot perform this action. Reinspect after any conflict; do not weaken the CAS.

## Reproduction and retained failures

Select the dedicated local fixture database with `FORGE_REVIEW_DATABASE_URL` and
set `FORGE_REVIEW_ALLOW_FIXTURES=1`. The existing helpers create and drop only
unique private schemas and test roles. From the repository root:

```sh
go test -race ./internal/persistence -run '^TestSnapshotHeader' -count=1 -v
go test -race ./tests/review \
  -run '^TestReviewUnsupportedSnapshot|^TestRecoveryPostgresAdditiveUpgrade' \
  -count=1 -v
```

To include the actual operator executable, build it explicitly and provide its
absolute path in `FORGE_REVIEW_ADMIN_BINARY`. Without that variable, only the
operator subprocess test skips; other compatibility tests still run when the
database variables are supplied. `FORGE_SNAPSHOT_REVIEW_OUTPUT` optionally saves
the raw reports to an absolute private directory. `FORGE_RECOVERY_UPGRADE_OUTPUT`
separately saves the additive upgrade report. Vet, the header race test and both
OpenAPI/sqlc generated-file checks pass; whole-tree/CI checks remain separate.

Initial development failures remain visible. One fixture initially used a
`jsonb` function on the preserved `json` column; another omitted the test server's
SSE manager and returned 500. That initial HTTP report's unconditional
`passed:true` field is incorrect: its actual response and failed test log are
authoritative. The reporting helper now derives that field from the test status.
The unchanged production HTTP path passes after configuring the fixture manager.
A CLI test import collision was corrected by aliasing `os/exec`; the real CLI
then passes. No original failed report was replaced with a successful one.

Independent review found a further pre-release P2: `{"schema_version":1,"SCHEMA_VERSION":2}`
passed the exact-key header check while the typed decoder saw schema 2. The
retained [independent reproduction](../benchmarks/results/snapshot-compatibility-20260911/ambiguity-independent-before.log)
and [six-case PostgreSQL failure](../benchmarks/results/snapshot-compatibility-20260911/ambiguity-postgres-before.log)
show live proof/heartbeat/event authorization and, in conflicting aliases,
repeated claim failure. Reverse ordering and duplicate 1 values were also
incorrectly admitted. The original raw reports and source remain in
`ambiguity-before-source/` and `ambiguous-before/`; none were rewritten.

After fixing both the Go reader and direct SQL gates, the same six cases pass
under `-race` in **9.472 s**; the complete final suite above includes them again.
The corresponding `complete-after-review/ambiguous-*.json` files preserve exact
JSON text with duplicates/order/escapes and byte-equal before/after execution
records. The [derived report](../benchmarks/results/snapshot-compatibility-20260911/report.json)
records each run separately; earlier successful subsets do not imply this
additional defect had already been covered.

The independent reviewer reran the original overlay counterexample and all three
header tests after the fix, under `-race` (1.027 s and 1.528 s respectively), and
[recomputed all six retained PostgreSQL after-records](../benchmarks/results/snapshot-compatibility-20260911/ambiguity-independent-after-audit.json).
The P2 is closed. That review reused the retained PostgreSQL execution rather
than claiming another independent database run.

One later CLI rebuild hit the local `/tmp` disk quota before any test started.
Its build failure is retained. Removing only the obsolete local test binary
allowed rebuilding; the final suite used that new executable and workspace-local
build scratch. Whole-tree and deployment checks remain separate.
