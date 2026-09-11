# Real application F05 / F07 / F08 process acceptance

This opt-in Go test executable runs the production `application.Driver`, real PostgreSQL ledgers, typed runner RPC and real rootless Docker. Each case has two independent OS worker processes and a uniquely generated `appfault_*` PostgreSQL schema. The provider alone is deterministic fake output, charged at explicitly synthetic rates; no API key or actual provider payment is involved.

The parent uses an isolated loopback `/forge` test database supplied through `FORGE_TEST_DATABASE_URL`. It creates/migrates its own schema and never queries or mutates public business tables. The administrator is used for fixture provisioning, operator decisions and audit. Before launching processes it creates a unique nonowner LOGIN role with explicit BYPASSRLS, no superuser/CREATEDB/CREATEROLE/DDL authority, and runtime DML restricted to that fixture schema; migrations, credentials and membership writes are revoked. A real connection verifies session identity, schema, privileges and the unchanged production CheckWorkerRole gate, saved as worker-role-preflight.json. Both Driver workers and Driver.CleanupWorkspace use this restricted login. The admin DSN is removed from child environments. Every run gets a unique tenant and workspace. The configured original runner journal, immutable journal identity and mounted pool remain unchanged; an unrelated fresh journal cannot take ownership of those volumes. No daemon, image or mount provisioning is performed by this harness.

Run the fixture preflight before quiescing services. It exercises the real source/oracle files and subprocess JSON scripts without Docker or PostgreSQL. The real matrix also prepares all three scripts before creating any schema or process:

```bash
GOCACHE=/tmp/forge-runtime-gocache go test ./scripts/faults/application -run '^TestApplicationFixturePreflight$' -v
```

Build before quiescing services:

```bash
GOCACHE=/tmp/forge-runtime-gocache go build -o bin/forge-runner ./cmd/forge-runner
GOCACHE=/tmp/forge-runtime-gocache go test -c -o bin/application-faults.test ./scripts/faults/application
```

The operator must quiesce production workers and runner, ensure at least one free pool slot, and launch the test through the **reviewed user-systemd service with `Delegate=yes` and the same RootlessKit UID map as the production runner**. Direct RootlessKit execution from the agent's inherited no-new-privileges environment is known to fail before starting a test. Use the same successful service architecture as the runner matrix. Pass these environment variables to that service's test command, preserving private environment-file handling for the database credential:

```text
FORGE_TEST_DATABASE_URL=<existing private loopback /forge test database credential>
FORGE_APP_FAULT_RUNNER_CONFIG=<absolute project path>/var/local/runner.json
FORGE_APP_FAULT_RUNNER_BINARY=<absolute project path>/bin/forge-runner
FORGE_APP_FAULT_EVIDENCE=<absolute project path>/benchmarks/results/application-faults-UNIQUE
```

The command inside that mapped service is:

```bash
"$PWD/bin/application-faults.test" -test.v \
  -test.run '^TestRealApplicationFaultMatrix$' -test.timeout=6m
```

The original platform/runner JSON is read, and only a private copy's socket is changed. Existing signing-key bytes and database credentials are never printed or copied to reports. The workers are child invocations of the test executable, not goroutines standing in for processes. Premature worker exit is detected during every wait and reports the bounded worker log immediately. Both instantiate the actual `application.Driver`; its existing lease duration option is set to five seconds. Its heartbeat continues while worker 1 is paused, then stops only when the parent sends real SIGKILL. Worker 2 polls concurrently and cannot claim the unexpired lease. Recovery waits for database-clock lease expiry naturally; no lease timestamps or snapshots are patched.

## Cases

- **F05:** after the runner accepts the original approved command, an operator-only response delay exceeds the target client's 500 ms Start timeout. The client inspects the same ID. Worker 1 pauses before returning that result to Driver, leaving the PostgreSQL effect in flight, then is killed. Worker 2 adopts and settles the original effect from its real receipt. Other RPCs retain normal deadlines.
- **F07:** the real runner exits 86 after observing the command's actual container exit but before publishing its receipt. Driver records `needs_reconciliation`; its effect and capacity remain reserved. Worker 1 pauses at the next inspect, then is killed. The original runner journal restarts; worker 2 adopts and reconstructs the receipt from the same Docker container. The effect is not blindly resubmitted.
- **F08:** a long real writer continues after worker 1 dies. On natural lease expiry, worker 2's adoption stops that job and settles its interrupted receipt before the next separately approved writer runs. Raw old/new write timestamps and command stdout demonstrate interval ordering. An old-epoch PostgreSQL heartbeat is rejected.

The fixture operator approves only the exact hashes of two literal commands generated by this test. Any different operation or arguments fails the harness; arbitrary model output is never auto-approved. The remaining fixture patches `app.py` using the complete trusted `expected/app.py` oracle, then the platform runs the original external target/regression grader and finalizes a patch.

## Evidence and retention

Each case captures full schema-scoped `runs`, `effects`, `model_attempts`, `quota_reservations`, `runner_allocations`, `tenant_runtime`, `runners`, `provider_quotas`, `run_events`, `run_snapshots`, `artifacts`, `workspace_cleanup` and `approvals` rows at the relevant boundaries. It also preserves worker PID/stdout logs, the exact runner fault marker, original operation JSON, raw command stdout and Docker create/start events. Assertions require one original effect/runner operation, one container create/start, four model attempts and four settled quota reservations, 570 synthetic microdollars, 570 tokens, zero remaining request/budget reservations, zero terminal runner/tenant capacity, contiguous events and a verified completed run.

Each table is read separately, without a common snapshot transaction. These are non-atomic cross-table captures. Exact identity, epoch, event and terminal-balance assertions supplement them; they must not be presented as a simultaneous global state. `actual_usage` settlement values in this fixture come from deterministic fake usage and its synthetic quote, not a provider invoice.

`Driver.CleanupWorkspace` must publish the immutable snapshot in PostgreSQL before the test workspace's volume lease is released. All READY fixture artifact bytes are copied to the evidence directory afterward. Fixture schemas, their uniquely named appfault_worker_* role and reports are retained for review; the harness never drops a failed schema, erases a failed workspace or proceeds to another case after failure. Operators can archive and remove a successful *named fixture schema* and its matching worker role later, after checking its captured ledger and released cleanup row. The role name is in worker-role-preflight.json; its password is never reported. Production artifact GC may eventually treat the fixture's model/context objects as orphaned because its PostgreSQL schema is intentionally separate; evidence contains their bytes, and runner receipts/snapshots remain pinned by the original runner journal.

A PASS is application/runner protocol and recovery evidence with a fake model. It is not model-quality, actual provider billing, or external collector-delivery evidence. No application matrix result is claimed until the operator executes this harness and preserves its raw output.

The executed 2026-09-11 three-case result, earlier failed attempts and the precise evidence scope are recorded in [E21 application fault evidence](../../../docs/application-fault-evidence.md).
