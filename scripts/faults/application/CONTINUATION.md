# F02 / F06 / F09 continuation matrix

This opt-in executable extends the application acceptance fixture with three separate cases. The executed F05/F07/F08 code, parameters and assertions in `application_test.go` are unchanged. The new `continuation_test.go` reuses setup primitives, restricted PostgreSQL roles, fixed scripts, process supervision, terminal ledger checks and artifact archiving. The model alone is fake; PostgreSQL, separate OS workers, typed RPC, runner, SQLite, ext4 and Docker are real.

## Boundaries and assertions

| Case | Injected boundary | Required evidence |
|---|---|---|
| F02 | Worker 1 calls the real production `Store.ClaimOnRunner`, then pauses before calling `Driver.Drive`; parent sends SIGKILL | Before death: zero PG effects, model attempts, quota reservations, artifacts and approvals; zero SQLite workspaces, operations and volume leases for this run. Worker 2 uses ordinary `Driver.RunWorker`; its first claim must follow the original five-second lease expiry, without changing the lease row. |
| F06 | A test-only RPC wrapper pauses after real `apply_patch` returns a successful durable receipt, before returning it to Driver | The file already matches the complete trusted oracle, SQLite has the committed receipt and its artifact bytes match the digest, while the original PG effect is still `in_flight` with no receipt ref. After worker SIGKILL and natural expiry, the original operation, receipt, before/after hashes, dispatch epoch and revision are unchanged; one PG effect settles successfully. |
| F09 | Parent kills worker 1 and the actual runner while the first approval is durably waiting, then restarts the same runner journal and a different worker | Original approval binding, run version, workspace revision and complete workspace tree hash remain unchanged. No original command exists in the runner journal before approval. Wrong args hash, workspace revision, policy, approval version and effect ID each return conflict without changing the approval. The exact original binding is then approved and the same operation runs once. |

F02 deliberately does not call `Drive` in worker 1: doing so could initialize the workspace or persist a system effect before a generic RPC pause. It uses the same claim method as the production worker loop. Its paused child has no heartbeat, so the harness checks that SIGKILL occurs before its original lease expires. Worker 2 runs the production worker loop normally. F09 is recovery from a durable approval decision, not a claim caused by lease expiry; the reports distinguish these triggers.

All cases must finish verified after the complete original clamp oracle is applied and the external target/regression grader runs. Each preserves the original counter command at one Docker create/start and counter `1\n`. Each has exactly four completed model attempts and four settled reservations, **570 synthetic microdollars and 570 fixture tokens**, zero outstanding request/budget reservations, zero terminal tenant/runner capacity, contiguous events and a READY code snapshot published before volume release. F06 additionally preserves and compares the patch-specific operation JSON before and after recovery. Captures read PostgreSQL tables separately and are **not cross-table atomic snapshots**.

The harness saves original worker PIDs, SIGKILL-request timestamps, wait-confirmed SIGKILL results, original leases and committed replacement claim times. A timestamp recorded before `Process.Kill` is not the kernel's exact process-death timestamp. Retained snapshots contain the underlying claim events for later audit. A run takes at most 100 seconds per case; a failed case stops the matrix and retains its schema, role, operation, artifacts and workspace.

## Build and execute

Run the source/oracle preflight before quiescing services:

```bash
GOCACHE=/tmp/forge-runtime-gocache go test ./scripts/faults/application \
  -run '^TestApplication(Fixture|Continuation)Preflight$' -count=1 -v
GOCACHE=/tmp/forge-runtime-gocache go test -c \
  -o bin/application-continuation.test ./scripts/faults/application
```

Use the same reviewed user-systemd service, `Delegate=yes`, RootlessKit UID map, private database environment handling and original runner configuration as the [first application matrix](README.md). Quiesce original workers/runner and confirm a free pool slot. This test creates no daemon, mount or image and does not replace the authoritative journal. Set:

```text
FORGE_TEST_DATABASE_URL=<existing private loopback /forge test administrator credential>
FORGE_APP_FAULT_RUNNER_CONFIG=<absolute project path>/var/local/runner.json
FORGE_APP_FAULT_RUNNER_BINARY=<absolute project path>/bin/forge-runner
FORGE_APP_FAULT_EVIDENCE=<absolute project path>/benchmarks/results/application-continuation-UNIQUE
```

The command **inside the reviewed mapped service** is:

```bash
"$PWD/bin/application-continuation.test" -test.v \
  -test.run '^TestRealApplicationContinuationMatrix$' -test.timeout=6m
```

These are operator commands. A local source/preflight PASS does not claim the real matrix has run. Archive the full raw test/service output, binary hashes and source snapshot after execution. The source contains no production fault API and never auto-approves arbitrary model output.

The executed 2026-09-11 three-case result and its precise scope are recorded in [E25 continuation evidence](../../../docs/application-continuation-evidence.md).

## Retained fixture recovery

Every submitted case writes an owner-only `recovery-descriptor.json` before launching any runner or worker. It includes the exact schema/run/tenant, fixed scripts, config-file SHA-256 and immutable journal identity; it contains no password, signing key or grant. Private subprocess settings are retained too. Failures before a run/descriptor exists allocate no workspace; retain their schema and logs for diagnosis.

Do not rerun the matrix into an existing evidence directory or create a new run as a substitute for a failed operation. After the original failure is diagnosed, the operator can use the explicit recovery entry point against that descriptor. It requests normal cancellation of the **same run**, waits for the real Driver to settle actual effects, then publishes a snapshot and performs ordinary `CleanupWorkspace`. It never changes deadlines or leases, approves commands, repeats original sampling, force-removes a container or deletes an uncertain workspace. Insufficient evidence fails recovery and leaves the original workspace retained.

With original services quiesced, use the same mapped service and private credential environment above, a **new** evidence directory, and this additional variable:

```text
FORGE_APP_CONTINUATION_RECOVER=<absolute original case path>/recovery-descriptor.json
FORGE_APP_FAULT_EVIDENCE=<absolute project path>/benchmarks/results/application-continuation-recovery-UNIQUE
```

Run:

```bash
"$PWD/bin/application-continuation.test" -test.v \
  -test.run '^TestRecoverApplicationContinuation$' -test.timeout=2m
```

Recovery requires the same complete runner config and journal identity, exactly one run in the named private schema, and unchanged fixed scripts. It creates a fresh restricted worker login in that schema because original fixture passwords are not saved. An expired original runtime deadline does not require rewriting it: the action is cancellation and settlement. If F02 never initialized a workspace, recovery verifies that both workspace and volume lease rows are absent and records cleanup as not applicable. Otherwise it requires a published snapshot before release. It archives `recovery.json` and the ledgers/artifacts in the new directory and explicitly records that recovery **does not pass the failed acceptance case**.

If config, source or journal identity changed, if the descriptor is missing, or if actual effects cannot be settled, do not edit the descriptor or remove storage to force this path. Preserve the named schema, journal and workspace for operator reconciliation. Restore normal services only after all test processes exit and the retained or released state is understood.
