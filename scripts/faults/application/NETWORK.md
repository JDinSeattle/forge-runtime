# F10 / F11 / F12 application matrix

This opt-in matrix adds four separate cases in new test files. The executed E21 and E25 source files are unchanged. It uses their private PostgreSQL schema setup, pinned clamp source, original runner journal and fixed volume pool. Production methods execute the work; deterministic fake model output is the only model used. No paid provider call or native-model-quality result is implied.

## Cases

| Case | Actual boundary | Required observations |
|---|---|---|
| `F10_cancel_first` | A real native `final_diff` has a durable receipt after actual repair/verification; the worker's RPC wrapper pauses before returning it to Driver. A real HTTP cancellation commits before the wrapper resumes. | Final run is cancelled; completed command/patch effects retain actual success; all model attempts and fees settle; repeated terminal Cancel does not change version/state; cleanup repeat returns the same snapshot/cleanup ID. |
| `F10_complete_first` | The same finalization wrapper resumes first; the harness waits for committed completion, then sends real HTTP Cancel twice. | Completed remains completed at the same version; successful effects remain; no duplicate fee or capacity release; repeated cleanup is idempotent. |
| `F11` | A real original command is accepted, actually finishes and publishes its receipt while Driver is paused with the earlier acceptance. The private API and worker PostgreSQL TCP proxy cuts existing connections and refuses new ones. | Submission/readiness return server errors, no new run/created event appears, and the workspace gains no additional operation while DB access is blocked. After worker death and proxy restoration a second worker claims from persisted eligibility and settles the exact original operation on the original runner. |
| `F12` | The real runner receives SIGKILL after the accepted original command has finished locally but before Driver learns its receipt. | Driver records unknown and retains original placement/capacity. A separate real HTTP API process stays ready, reads the unknown run and accepts a fixture message; actual TCP SSE delivers that durable message while the runner is still dead. The same journal restarts; a separate worker settles the original receipt without another Docker create/start. |

The cancellation cases exercise two controlled orders at the run-completion boundary. They do not claim every scheduler race or kill an actively writing command; earlier actual Stop/adoption evidence covers separate interruption paths. F11 is a **connectivity outage for the fixture API and worker**, not shutdown of the shared PostgreSQL server. The parent fixture administrator retains a direct connection for provisioning and read-only audit during the cut. No production connection is routed through the proxy.

F12 uses the production Driver's ordinary deferral on runner unavailability. That method may end its lease and set `not_before`; this is distinct from E25's deliberately untouched natural-expiry experiment. The harness never updates a lease, deadline or snapshot directly. It requires the next claim event to use the second worker, the next epoch and a database time at or after persisted lease expiry. This single-runner deployment does not establish multi-host failover safety.

## Real API identity and transport

The test API is a separate OS process running the production `httpapi.Server` and `eventstream.Manager` on a real numeric loopback TCP listener. It passes the unchanged production `CheckAPIRole` gate. Its unique LOGIN has NOBYPASSRLS, no superuser/CREATEDB/CREATEROLE/ownership and schema-scoped runtime permissions. Token/membership writes are revoked. The parent issues one fixture token and never saves it in evidence.

Workers use their separate explicit-BYPASSRLS, nonowner service login and the production Driver. All fixture commands are the same two literal approved commands, complete trusted patch and stop message used in E25. Four model attempts/reservations must settle to 570 fixture tokens and 570 synthetic microdollars. The original command has counter `1\n`, one Docker create/start and its original receipt. Final effect/capacity ledgers and snapshot-before-release are checked. PostgreSQL captures remain separate table reads, **not a common atomic snapshot**.

The transparent proxy only connects an explicitly configured loopback upstream. It never parses or records PostgreSQL bytes. A cut closes both endpoints of every existing proxied pair and refuses subsequent connections until restored. Its event `connections` count is the number of socket endpoints closed, not a count of SQL sessions. Private credentials stay in process memory/environment; reports contain role names, request paths/statuses and response bodies without Authorization headers.

## Checks before quiescing services

Build and run the pure fixture preflights:

```bash
GOCACHE=/tmp/forge-runtime-gocache go test -race ./scripts/faults/application \
  -run '^TestApplication(Fixture|Continuation|Network)Preflight$' -count=1 -v
GOCACHE=/tmp/forge-runtime-gocache go vet ./scripts/faults/application
GOCACHE=/tmp/forge-runtime-gocache go test -c \
  -o bin/application-network.test ./scripts/faults/application
```

The following real loopback test needs no PostgreSQL, runner or volume:

```bash
GOCACHE=/tmp/forge-runtime-gocache go test -race ./scripts/faults/application \
  -run '^TestFixtureProxyCutsExistingAndNewConnections$' -count=1 -v
```

An additional opt-in preflight creates one private PG schema and starts only a private API process plus proxy. It checks the real restricted login, HTTP/SSE, failed submission during the cut, recovery afterward and zero local volume lease. It does not start any runner or worker or stop original services. Supply the usual private fixture environment described below, choose a fresh evidence directory, and set `FORGE_APP_NETWORK_API_PREFLIGHT=1`:

```bash
"$PWD/bin/application-network.test" -test.v \
  -test.run '^TestApplicationNetworkAPIPrivilegePreflight$' -test.timeout=1m
```

Its queued private run/schema remain retained for review and have no workspace allocation. If this preflight fails, diagnose it before quiescing original services for the real execution matrix.

## Execute the actual matrix

The operator uses the previously reviewed user-systemd/RootlessKit mapping with `Delegate=yes`, original signing key and journal, and at least one free mounted slot. Quiesce the original workers/runner before running the matrix. The test provisions no daemon, mount, image or storage identity.

```text
FORGE_TEST_DATABASE_URL=<existing private loopback /forge administrator credential>
FORGE_APP_FAULT_RUNNER_CONFIG=<absolute project path>/var/local/runner.json
FORGE_APP_FAULT_RUNNER_BINARY=<absolute project path>/bin/forge-runner
FORGE_APP_FAULT_EVIDENCE=<absolute project path>/benchmarks/results/application-network-UNIQUE
```

Inside the mapped user service:

```bash
"$PWD/bin/application-network.test" -test.v \
  -test.run '^TestRealApplicationNetworkMatrix$' -test.timeout=8m
```

Every case has a 100-second bound and the matrix stops at its first failure. Reports include private role checks, worker/API/runner PIDs, the real receipt before the fault, HTTP responses, proxy cut/restore times, actual SSE events, original/replacement epochs, schema captures, original daemon history and artifact bytes. No result is claimed until this command actually runs. Archive the exact source/binary hashes and source snapshots **after all shared production edits are frozen**; current Store/HTTP code may differ from earlier E21/E25 builds.

## Recover an original failed case

Every case receives an owner-only `recovery-descriptor.json` before any process starts. It binds the existing config SHA-256, journal identity, schema/run/tenant and unchanged fixed scripts without storing secrets. Use a fresh output directory, restore normal direct PG connectivity, quiesce original services and select the explicit recovery entry point:

```text
FORGE_APP_NETWORK_RECOVER=<absolute original case path>/recovery-descriptor.json
FORGE_APP_FAULT_EVIDENCE=<absolute project path>/benchmarks/results/application-network-recovery-UNIQUE
```

```bash
"$PWD/bin/application-network.test" -test.v \
  -test.run '^TestRecoverApplicationNetwork$' -test.timeout=2m
```

This restores the original runner and asks the real Driver to cancel and settle the **same run**, then performs ordinary snapshot publication and workspace release. It does not resume failed acceptance, create a replacement run, approve commands, reset deadlines, edit leases or force-remove containers. A new restricted worker login is created because passwords are not retained. A zero-revision run is considered to have no workspace only when both SQLite workspace and volume lease rows are absent.

If any actual effect remains unsettled or identity/binding evidence disagrees, recovery stops and retains the schema, journal, artifacts and volume. An operator must reconcile that original state; removing storage or changing the descriptor to make the test pass is not a recovery path. A successful recovery records `recovery_only=true` and `original_acceptance_passed=false`. Restore normal services only after all test processes are gone and retained or released workspace state is understood.
