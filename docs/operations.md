# Operating the local baseline

## Authority and secrets

`forge-admin local-setup` requires the dedicated loopback `/forge` development
database owner and a new private output directory. It generates random role names
and credentials without printing them. Keep `api.env`, `worker.env`, `client.env`
and `runner.key` under ignored private state. The development compose password is
a fixture, not a remote deployment credential.

The API role has tenant-scoped business access and read-only authentication
tables. It cannot issue API tokens or migrate schemas. Its startup check rejects
superusers, RLS bypass, ownership (including inherited membership), missing
business tables and disabled RLS. The worker uses a separate nonowner role with
explicit cross-tenant service authority. Do not supply that role to the API.

Bind all local services to loopback. Remote API access needs TLS termination;
remote runners require the pinned mutual TLS peer identity in configuration.
The runner signing key must be a private regular file. A key rotation must retain
the old verifier until already-issued bounded grants have expired, or drain and
restart the local runner/worker pair together.

## Storage and cleanup

Execution capacity and storage capacity are separate. Approval waits and terminal
runs release active execution slots while retaining their physical workspace
leases. A four-volume pool can therefore be full even with zero active runs.
Inspect retained runs; do not raise a counter or remove a directory by hand.

After the configured retention period, an operator can clean one terminal run:

```bash
bin/forge-admin -config "$PWD/var/local/platform.json" -tenant demo \
  -run RUN_ID -age 168h workspace-gc
```

Use the admin database environment. `-age 0` is available for disposable fixture
cleanup after its artifacts have been reviewed; it is never automatic. Cleanup
refuses nonterminal runs, pending/unknown effects, unreleased execution allocations,
or a revision that differs from the terminal state. The separate cleanup ledger
owns a 30-second database lease and a fixed runner epoch. Cleanup grants cannot
authorize new execution or adoption.

Cleanup stops admission, seals a bounded **code snapshot**, verifies and publishes
the artifact, records its reference, then releases task-owned storage. The code
snapshot excludes ignored build/cache paths and is not a complete forensic disk
image. A snapshot error retains the workspace. Receipt and journal records remain.
After a crash in the sealed phase, retry releases the same workspace; it does not
attempt to snapshot a directory that might already have been removed. Another
operator must wait for the cleanup lease to expire before taking over.

Transport event trimming is independent:

```bash
bin/forge-admin -age 168h -keep 128 trim-events
```

One bounded batch covers at most 32 old terminal runs. It advances the retained
sequence and deletes expired rows atomically. API readers return reset semantics
for an expired cursor; audit snapshots remain. Idempotency keys currently remain
stored beyond their declared minimum 24-hour lifetime: this baseline has no
automatic key-expiration deletion or artifact-object orphan collector.

## Recovery and backups

- **Worker stops:** stop only that worker. It ceases claiming, loses its bounded
  execution lease and preserves remote runner facts. Start a new unique worker
  identity on the same configured runner. Do not migrate an unknown effect.
- **Runner stops:** retain the SQLite WAL, workspace images and object directory.
  Restart with the same journal and slot identities; prepared jobs are inspected
  by their stable IDs. Keep the rootless Docker daemon's state for reconciliation.
- **Database unavailable:** admission and new effects stop. Reconnect before
  inferring failure or refunding a reservation. Unknown provider usage retains a
  conservative financial obligation even when its concurrency slot expires.
- **Runner evidence insufficient:** the run remains `needs_reconciliation`.
  `resume` uses an expected version and cannot turn an uncertain command into an
  assumed successful or safe-to-repeat command.
- **Physical volume identity mismatch:** stop admission. Preserve manifest,
  mount-state, SQLite journal and exact error. Do not edit identifiers to force
  the verifier to accept a different disk.

Coordinate PostgreSQL backups with journal/WAL, artifacts, image manifests and
workspace snapshots. Quiesce task admission and inspect operations before backing
up live filesystem images. A point-in-time PostgreSQL restore alone cannot prove
the fate of an external container command.

The executed [SQL restore rehearsal](../benchmarks/results/database-restore-20260908.json)
used a custom-format dump and `pg_restore --exit-on-error` into an empty
disposable database, with no worker attached. Roles and grants were excluded.
Its matching counts validate metadata restoration only; the journal, workspace
images and artifact bytes still require the coordinated recovery set above.

Schema upgrades use additive migrations. Migration 00005 cannot reconstruct
byte-exact tool arguments previously reformatted by JSONB; such old pending runs
need reconciliation using canonical effect bytes and authenticated receipts.
Migration 00007 adds exact transition inputs; preexisting NULL inputs remain
unavailable, not reconstructed history. Review role grants for any newly added
table before exposing it through a service.

Migration 00009 adds reserved priority metadata (default zero; dispatch remains
FIFO). Migration 00010 adds the fixed membership-lock function used by approval
decisions. Existing deployments must run the owner migration and grant the
configured API role EXECUTE on that one schema-qualified function before using
the new approval binary; membership tables remain read-only to the API. See the
[exact upgrade and permission procedure](approval-authority-evidence.md#deployment-and-reproduction).
The [earlier additive rehearsal](integration-checks-20260911.md#current-additive-upgrade-coverage)
upgrades a private v6 fixture to v10 twice without changing its original records.
It does not migrate the running deployment or prove rolling compatibility.

Migration 00011 adds separate compatibility-hold metadata. Apply it before
starting a binary that reads these columns. Unsupported or ambiguous snapshot
versions return `snapshot_migration_required`; held runs retain their unknown
state and obligations until an operator supplies a compatible recovery path.
Use the [inspection and guarded-unhold procedure](snapshot-compatibility-evidence.md#operator-procedure),
which checks the current raw hash/version and absence of a live lease. Never
repair a future body by changing only its version field. The
[current E40 rehearsal](integration-checks-e40-20260911.md) passes private 6→11
upgrades with the actual current admin executable; it does not upgrade the live
services. Already issued runner grants still expire on their original deadline.

## Diagnostics

The API exposes `/healthz`, `/readyz`, `/metrics`; worker metrics have a separate
loopback listener. `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` enables an optional bounded
OTLP exporter. Operational metrics are observations and can repeat after recovery.
Business totals come from PostgreSQL quota/attempt/effect records.

Use [the independent review log](reviews/implementation-review.md),
[evidence ledger](evidence.md) and [raw scheduler results](../benchmarks/README.md)
to distinguish tested behavior from deployment expectations. The project does not
claim a multi-node SLO, paid-model task success rate or production security audit.

## Reproduce the saved-model process crash

Use the fake-only local deployment and an available retained-workspace slot.
Pause its ordinary workers so this fixture is claimed by the crash worker;
the API, runner and dedicated Docker daemon remain running. In one terminal:

```bash
set -a; . var/local/worker.env; set +a
FORGE_METRICS_LISTEN=127.0.0.1:8108 bin/forge-worker \
  -config "$PWD/var/local/platform.json" -id crash-fixture \
  -test-crash-after-model
```

In another terminal run `python3 scripts/demo-repairs.py --fixture clamp`.
The crash worker must exit with status 86 after logging the durable-result fault.
Start two ordinary workers with different IDs and free metrics ports, omitting
the crash flag. The existing CLI watch remains connected while the 30-second
database lease expires naturally. Do not edit the lease or resubmit the task.

Check the same run's model attempt rows, effect receipt references, final state,
released allocation and outstanding quota. Each durable model step should have
one saved result; the next owner must reuse that result. The operator fault flag
is rejected if any non-fake provider is configured. The recorded execution and
its precise scope are linked from [the case study](portfolio-zh.md).
