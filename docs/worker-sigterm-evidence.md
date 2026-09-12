# E48 — Actual worker SIGTERM and successor acceptance

Status: harness implemented; no-network preflight and vet passed. Actual worker,
private PostgreSQL and gRPC execution is **pending operator execution**. This
document does not claim the recording backend or Docker acceptance has passed.

The new entry point is `TestRealWorkerSIGTERMRecording` in
`scripts/faults/application/worker_sigterm_test.go`. It launches the supplied
production `forge-worker` executable twice. These are actual OS worker processes,
not test-binary children that instantiate a modified Driver. Worker defaults stay
unchanged: one configured slot, 30-second lease, 150-ms polling. The existing
application-fault fixture provides isolated-schema migration, restricted LOGIN
provisioning/preflight, captured ledgers and snapshot-before-release maintenance.

The first phase uses the production runner Engine and SQLite journal behind a
real private gRPC Unix socket. An explicitly configured `sandbox.TestBackend`
records jobs and remains running for twelve seconds. Its verification reads the
actual fixture file but executes no Python, shell or container. Provider scripts
are deterministic and indexed by durable model step. Their usage and prices are
synthetic; no paid provider or model-quality assertion is involved.

## Frozen acceptance sequence

1. Create one fresh `appfault_sigterm_*` PostgreSQL schema and a unique nonowner
   runtime LOGIN. It has explicit BYPASSRLS, schema-local runtime DML, no superuser,
   role/database/schema creation or ownership, and no credential/membership DML.
   A real connection passes production `CheckWorkerRole`. Workers inherit an
   environment allowlist and only the restricted DSN; admin DSNs and provider
   keys are excluded. Schema and role are retained for review.
2. Submit one run, start the real worker, and approve only its exact fixed command
   hash. Poll the actual runner operation until it is running. The approval/reclaim
   can already have advanced the epoch: all recovery assertions use the observed
   old epoch plus one, not an assumed epoch number.
3. Bind the child PID to `/proc/<pid>/exe` SHA and process start ticks both at
   launch and immediately before SIGTERM. Record database-clock samples before
   and after the signal syscall. Require clean exit code 0 within an explicit
   eight-second fixture bound. This bound is not a production SLO.
4. Observe the same runner job still running after worker exit, the same pending
   effect, and tenant/runner/allocation capacity still 1. No Stop/Cancel RPC or
   business cancel transition may be attributed to this process shutdown.
5. Submit a queued probe after exit. A fixture-only `not_before` admission gate
   postpones this second run until the main workspace is sealed and released.
   The gate changes only the probe's scheduling field; the main lease is never
   shortened, cleared or overridden. The probe remains version 1 / epoch 0 while
   gated, permitting sequential reuse of a single slot.
6. Start the real successor and wait for natural PostgreSQL-clock lease expiry.
   It must adopt/inspect the same original operation without sending another
   Start. The recording job finishes naturally before takeover; this phase does
   not claim that adoption interrupted a still-running container.
7. Complete the deterministic patch/finish sequence, seal/publish/release the
   main workspace, open the probe gate and complete/clean up that run. Stop the
   successor with SIGTERM and require clean exit as well.

Assertions bind the final SQL effect to its original dispatch epoch, exactly one
effect confirmation, one observed target Start RPC, one recording-backend Start,
successor Inspect/Adopt and zero successor Start for that operation. All observed
claim durations must be exactly 30 seconds. Original-worker claim events strictly
after the post-signal database-clock sample must be zero. Claims inside the tiny
before/after signal window are not mislabeled as definitively post-signal; the
raw timestamps and snapshots preserve that ordering boundary. The PID's confirmed
exit and subsequently queued probe are the direct stopped-process observation.

Final assertions require three completed main model attempts, no business cancel
events, no unknown/in-flight effects, zero active/request/runner counters and
contiguous main event sequence. Both cleanups use the restricted LOGIN and actual
`Driver.CleanupWorkspace`, preserving immutable artifact metadata before release.

## Operator execution

Only the root task/operator executes host networking and the worker processes.
Build both binaries from this worktree, with the configured local toolchain and
cache; no dependency download is needed. Preserve build output and binary hashes.

```sh
go build -o var/e48-build/forge-worker ./cmd/forge-worker
go test -race -c -o var/e48-build/application-faults.test ./scripts/faults/application
```

Required environment values:

```text
FORGE_TEST_DATABASE_URL=<authorized dedicated loopback /forge administrator DSN>
FORGE_RUN_WORKER_SIGTERM=1
FORGE_WORKER_SIGTERM_SOURCE_ROOT=<absolute worktree root>
FORGE_WORKER_SIGTERM_BINARY=<absolute worktree root>/var/e48-build/forge-worker
FORGE_WORKER_SIGTERM_EVIDENCE=<absolute fresh owner-only evidence directory>
```

Run the preserved executable with a three-minute test timeout. Expected normal
duration is about 45–65 seconds; no duration is claimed until actual execution.

```sh
var/e48-build/application-faults.test -test.v \
  -test.run '^TestRealWorkerSIGTERMRecording$' -test.timeout 3m
```

The evidence directory must not exist beforehand. It receives process identities,
signal windows/exit codes, worker stdout, selected source hashes, original job
identity, RPC observations, phase-specific SQL captures, final acceptance and
per-run cleanup/artifact copies. SQL tables are captured separately, not under a
single simultaneous cross-table snapshot. Signing-key bytes and restricted DSNs
remain in private configuration/process memory, not these JSON reports.

Failure retains the schema, role, journal, objects and workspace for diagnosis.
The test stops only children it launched and closes its own gRPC/Engine handles;
it does not turn an unknown outcome into release, remove a failed workspace or
erase prior evidence. It starts/stops no demonstration service and mounts no
volume. The source has no `cmd/forge-runner` or `internal/runner` edits.

An existing mapped Docker runner can later replace the recording service behind
the observer, after separate operator review of the exact original journal and
pool ownership. That mode is not implemented or claimed by this first entry
point. Actual container ID/create/start counts and survival across worker exit
remain required for the later Docker combination.

## Local preflight evidence

`benchmarks/results/worker-sigterm-preflight-20260911/` retains the initial compile
failure (missing import/incorrect signer call), corrected preflight race passes,
vet output and the source snapshot/hash. The preflight tests credential filtering,
fixed command-script binding and recording-job running/inspect/cancel behavior
without sockets, PostgreSQL or subprocess execution. It does not stand in for the
pending opt-in run above.
