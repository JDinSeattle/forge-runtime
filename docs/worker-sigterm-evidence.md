# E48 — Actual worker SIGTERM and successor acceptance

Status: **PASS for the integrated recording-backend acceptance**, 47.24 s on
commit `0c796c744b90bd08a26513a3586a475387ba775d`. Actual production worker OS
processes received SIGTERM and recovered through private PostgreSQL, real gRPC
and the production Engine/SQLite journal. The runner backend records jobs; it
executes no command subprocess or Docker container. The original 46.64 s run
remains **FAIL** because its confirmation query omitted `reconciled`; that raw
record is preserved separately and is not relabeled.

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

## Integrated host result

Raw evidence is in
`benchmarks/results/worker-sigterm-e48-integrated-20260912/`. Independent review
recomputed the observations below from the phase captures, RPC records, archived
receipt bytes and a read-only connection to the closed, private SQLite journal.
It verified all 81 files named by the raw manifest. No demonstration service was stopped or
shared workspace used by this recording acceptance.

| Observation | Actual result |
| --- | --- |
| Original / successor worker PIDs | `3383176` / `3383524` |
| SIGTERM exit codes | `0` / `0` |
| Signal send to observed/reaped exit | Approximately `1.576 ms` / `1.537 ms`; two individual samples, not an SLO |
| Original / successor worker lease | Epoch `2` / epoch `3`; worker IDs `signal-original-0` / `signal-successor-0` |
| Original SQL lease expiry | `2026-09-12T00:26:24.190562Z` |
| Successor committed claim timestamp | `2026-09-12T00:26:24.282880Z`, `92.318 ms` after expiry |
| Target operation Start | One RPC and one recording-backend Start; successor Start zero |
| Successor target Inspect / workspace Adopt | One / one |
| Target confirmation | Exactly one `reconciled`, event epoch `3`, receipt dispatch epoch `2`, succeeded and settled |
| Main / probe completion | Both completed, snapshot published and allocation released |
| Final tenant active / runner reserved slots / provider active requests | `0` / `0` / `0` |
| Final token / money reservations | `0` / `0`; committed synthetic usage and cost remain recorded |
| Main durable event sequence | 53 contiguous events |

The main run is `run_4KRCRAJNVBN2KYUSW5XEBTQTYF`, with target operation
`run_4KRCRAJNVBN2KYUSW5XEBTQTYF_step_1_op_0` and recording job
`forge-0b03b92c53517b32e4fc0ef80401314768b427e9`. Its full captured operation is
unchanged before the first signal and after the original worker exits, and still
reports running at that boundary. The recovered receipt, SQL effect and SQLite
operation row retain that ID, job ID, argument hash and dispatch epoch. The
archived receipt's bytes match the PostgreSQL artifact SHA and size. Cleanup
advances its own runner epoch after terminal completion; it does not rewrite the
original dispatch identity.

The queued probe was created after the original PID exited and remained version
1 / epoch 0 under its fixture admission gate until main cleanup. All captured
claim leases are 30 seconds. There are no original-worker claim events after the
post-signal database-clock sample, no business cancellation events and no Cancel
RPC. The final shared quota capture records 840 committed synthetic micro-USD
across the two runs and zero reserved cost/tokens. No actual provider charge is
involved.

All 318 captured code/configuration/script inputs match before and after the run
and were independently compared, byte for byte by SHA-256, with their Git blobs
at the stated commit. The preserved executable identities are:

- Worker SHA-256: `1e06ec9213fa50a98c296ee5f17fa99d28e244d300d379f706467d2a18b6365c`.
- Race test executable SHA-256: `7da90f279433c918b960f789a50bc7c0d523557df40e5d79e6687d8363e8f0a7`.

Both on-disk binaries still matched those hashes at independent review. Each
worker's launch and pre-signal `/proc` samples agree on executable SHA, PID and
start ticks. The Go build records `vcs.modified=true`: this was a working-tree
build, not an independently reproduced clean build or complete transitive
compiler/toolchain attestation. The launcher freezes the 318 selected inputs;
it does not attest every host or toolchain input.

The manifest excludes live object/workspace trees and SQLite; archived receipt
bytes are covered, and the private journal was separately hashed and inspected
read-only at review (schema version 5, zero unsettled operations). Table captures
and process identities are sampled separately, not an atomic distributed
snapshot. The recording job lasts twelve seconds and completes before the
30-second takeover, so this result does not prove adoption of a still-running
real container. The later Docker combination remains unverified by E48.

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
effect confirmation across `effect_completed` and `reconciled`; that single
confirmation must be a successful settled `reconciled` event from the successor
epoch, with the original dispatch epoch in its receipt. It also requires one
observed target Start RPC, one recording-backend Start,
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
duration is about 45–65 seconds; the integrated sample above took 47.24 seconds.

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
separate host acceptance above.

## First actual run and fixture correction

The original failed execution is preserved at
`benchmarks/results/worker-sigterm-e48-20260912/`, including its sealed manifest
and original source/binary identities. It is not rewritten or relabeled passed.
The isolated schema and both runs remain available; this correction performs no
SQL or workspace mutations against that run.

In `run/main-completed-postgres.json`, version 12 contains the target receipt:
`kind=reconciled`, owner `signal-successor-0`, event epoch 3, receipt epoch 2,
status `succeeded`, and `settled=true`. Its receipt reference and argument hash
match the one durable SQL effect. Version 17's `effect_completed` is the later
patch operation. Production `Driver` intentionally emits `reconciled` when
`CommandInspectEffect` recovers an earlier dispatch; both settlement events map
to the durable `tool.finished` stream event. The original query therefore counted
zero despite the recovery receipt being present.

The correction counts both terminal confirmation kinds, requires their total to
be exactly one, and additionally requires that unique confirmation to match the
successor owner/epoch and the original dispatch epoch with successful settled
status. A duplicate of either kind cannot be hidden by selecting only one kind.
`benchmarks/results/worker-sigterm-diagnosis-20260912/` preserves the old query
source and failing stdout, an offline audit with copy-only negative cases, and
compile/race preflight/vet logs for the correction. The audit diagnoses the old
failure; the later integrated run above separately executes the corrected SQL
on a fresh private PostgreSQL schema.
