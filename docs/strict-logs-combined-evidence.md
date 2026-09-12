# S12.9 combined fixed-volume log acceptance

Status: **implemented opt-in fixture; actual combined execution pending**. This
file does not promote S12.9, E44, or the overall acceptance goal. Offline tests
cannot demonstrate a Docker process, a kernel ENOSPC, a mounted spool, or a
PostgreSQL publication. No host/daemon/volume/model execution was performed while
implementing this fixture.

The two Linux test files are
`scripts/faults/application/strict_logs_combined_test.go` and
`scripts/faults/application/strict_logs_combined_helpers_test.go`. They use the
production `forge-runner`/`forge-worker` executables. All helper names begin `sl`;
existing application/E48/E50 files and production code remain unchanged.

## Fixed execution contract

Entry: `TestStrictLogsCombinedAcceptance`.
Opt-in: `FORGE_RUN_STRICT_LOGS_COMBINED=1` and
`FORGE_STRICT_LOGS_ACCEPTANCE=<scope>/acceptance.json`. The parent-owned launcher
is the execution entry after authorized preparation/mount/configuration and a
successful actual E50 phase:

```sh
/usr/bin/python3 -I scripts/faults/sigterm/run.py launch --phase logs
```

Do not run the test directly as host root, as the ordinary owner, against an
existing demonstration pool, or before the E50 run has settled. The launcher
supplies the same full subordinate UID/GID mapping as E50, the one declared test
binary, and the private loopback PostgreSQL fixture. It separately checks the
exact named test exists; a zero-test Go exit cannot count as acceptance.

The shared descriptor has purpose `worker-runner-sigterm-v1`, fixture ID
`lr20260912_a`, a new `var/lifecycle-rehearsals/lr20260912_a` scope, and four
256 MiB ext4 slots under its `pool-root`. Runner/worker/test binary paths and
SHA-256 values come from this descriptor. Docker is the fixed rootless endpoint
`unix:///run/user/1000/forge-runtime-docker.sock`; the source is exclusively
`lifecycle` and the pinned profile exclusively `lifecycle-python`.

Every phase retains the same journal path **and UUID**, owner markers, signing
key, runner root, artifact root, source, socket, four complete `VolumeSpec`
identities, and pinned image. All four volumes are verified, including the three
normally unallocated slots. Before opening an Engine, the fixture reads the
actual SQLite journal in a read transaction: all operations must be terminal and
all workspace/volume leases released. It also rejects a still-live UDS endpoint.
E50's completion JSON alone never authorizes reuse.

The main runner configuration is never rewritten. Root configuration supplies
`runtime/runner-byteguard.json` (only RunBytes=2 MiB) and
`runtime/runner-countguard.json` (only MaxOperations=2). The fixture verifies all
other decoded settings are identical, records exact input hashes, and restarts
only its own runner after the current workspace has been stopped, sealed, and
released. The final phase restores the default configuration and exits its own
runner. It never starts/stops the Docker daemon or an existing demo service.

The fixture creates a new `appfault_strictlogs_*` schema and a restricted worker
LOGIN. Worker environments exclude inherited provider keys and administrator
credentials. A separate fixture API connection selects only the already proven
private schema to issue temporary test tokens; the restricted worker role is not
granted authentication or tenant-bootstrap privileges. HTTP observations prove
the actual handler/authentication/tenant behavior, **not** deployment API-role
hardening. Credentials/key bytes and capability grants are excluded from raw
reports. Private recovery settings stay in `runtime/*-private`.

## Required cases and independent assertions

All six expanded case entries must pass to complete the five groups:

| Report entry | Actual workload | Required evidence |
|---|---|---|
| `L1` | Actual worker/Driver, real runner/Docker, known binary stdout and stderr, then repair/verification | Exact per-stream binary bytes, framed disk and immutable object equality; two pinned objects observed inside the existing 10-second `after_receipt_before_commit` delay while SQLite is nonterminal and neither object is PG READY; then one durable effect/receipt confirmation, real PG READY, authenticated exact-byte download and cross-tenant 404; production snapshot/publication/release. |
| `L2-L3-default` | 32 real typed process operations in one run under unchanged defaults; finite output overflow plus separate business cancellation | Both streams drain; actual policy stop, truncation and complete dropped-byte equation for overflow; ordinary cancellation is interrupted with no policy-stop claim; entry <=16 KiB, operation <=512 KiB, actual sum <=16 MiB; exact 32 reservations; 33rd ID is ErrCapacity without journal/container admission; same-ID retry unchanged; actual daemon create/start counts are one each; stopped spool sizes sampled again; cleanup does not refund lifetime reservation. |
| `L3-bytes` | Four real operations, RunBytes=2 MiB, MaxOperations=32 | Fifth ID refused while operation count remains below 32; physical spools, ledger, duplicate/rejection and cleanup/reopen observations use the same checks. |
| `L3-count` | Two real operations, MaxOperations=2, RunBytes=16 MiB | Third ID refused while byte reservation is only 1 MiB; same checks. |
| `L4` | Actual worker/Driver and finite paced two-stream command; physical pressure on the assigned fixed filesystem | Real kernel ENOSPC on an identity-checked trusted sibling file; **actual spool I/O failure**, original container stopped before pressure removal, no premature volume release/refund; same-operation inspection recovers only truthful retained bytes, production PG publication/HTTP, stop/seal/release, and a fresh real health operation; all four volumes reverified. |
| `L5` | Reuse E50's actual worker+runner SIGTERM execution, no second weaker replay | Original historical binary inputs match this binary; exact original operation/receipt and live SQLite binding; shutdown and post-adoption spools unchanged; raw daemon create/start/die counts each one; four exact process signals/exits; natural lease expiry; incomplete `runner_shutdown` log with no invented exact dropped total; live PG unique confirmation and released capacity; new authenticated download of the retained original log. |

Quota probes deliberately use direct typed RPC, not Driver/PG. L1 and L4 supply
production Driver/PG publication; L5 independently reads and joins the actual E50
producer's raw records and current authorities. The same-ID byte budget is
rechecked after normal journal reopen **after release**; the fixture does not
pretend an already released workspace can accept another RPC admission.

The initial L1 fault plan is scoped to its exact tenant/run/workspace/operation
and dispatch epoch, expires within five minutes, and is enabled with both
operator fault flags. The `.used` marker and the observed pin window are saved.
This is a delay/order observation, not a crash observation. Artifact IDs are not
content hashes: joins use the object reference's tenant/run/kind/key/SHA/size and
the actual PG artifact row.

The independent FLG1 reader does not call the production parser. It checks magic,
stream number, reserved bytes, sequence, positive bounded record length, CRC32,
file bounds, record/payload/stream totals and hashes. A missing/torn tail is
explicitly distinguished from a complete capture. It verifies normalized request
binding with grants removed and deadlines compared as the same UTC instant,
metadata hash, preview bound, immutable receipt, published object and durable
SQLite request/status/result/receipt. `termination_observed` means a policy-stop
observation; normal completion is checked separately against the actual stopped
Docker container.

## Bounds, failures and reporting

Execution is serial, with one task container running at a time, a 10-minute Go
context and launcher deadline. Commands are finite; the largest flood demands
1 MiB then sleeps for at most ten seconds, so a policy stop can be observed while
the task is still alive. The cancel program installs SIGTERM ignore before fork,
prints distinct parent/child ready markers, and has a 60-second natural bound.
The ENOSPC program sleeps eight seconds for pressure setup and emits at most
76,822 bytes over a bounded loop. No infinite output or host shell executes.

Default bounds remain entry 16 KiB including its 32-byte header, operation
512 KiB, run lifetime reservations 16 MiB/32 operations, preview 64 KiB. The
production profile is one CPU, 256 MiB memory with equal swap limit, 64 PIDs,
nonroot container UID 1000, no network and read-only root. A pressure file is
bounded by its one 256 MiB filesystem, written with real bytes, with device,
inode, owner, allocated blocks, error and statfs recorded. It is outside checkout
so Engine tree/file limits cannot substitute for the spool failure. No resizing,
remount, reserved-block change, sparse-allocation claim, or original pool reuse
is performed. Planned workload/output/artifact caps are not measurements.

If the full filesystem prevents truthful recovery/publication, that case fails
and leaves the unresolved run/slot intact. Failure cleanup stops only subprocesses
created by this fixture, does not send business cancellation, does not erase
unknown state, and does not automatically drop schemas. The pressure file is
removed only after the exact container is independently stopped and its file
identity is unchanged. Later retries require a separately authorized fresh
versioned fixture; the existing `logs-01` directory is never overwritten.

Output is the newly created `<scope>/evidence/logs-01`. The aggregate
`acceptance.json` has `passed=true` only after all six case entries and final
release/identity checks pass, the binaries/config/source still match their
before hashes, and children stop cleanly. `manifest.json` hashes every evidence
file except itself. Failed reports and all completed raw observations remain.
Input hashes are before/after identity checks; they do not independently prove a
clean source build. Root's build/launcher evidence remains a separate artifact.

## Offline verification at implementation freeze

The Linux offline tests validate framing/metadata/identity negatives, unsafe
scope/config/mapping cases, no-follow bounded file reads, bounded program shape,
and durable SQL request/result/receipt tampering. The real combined test skips
unless explicitly enabled. These tests are ordinary in-process checks and do
not contact PostgreSQL, sockets, containers, volumes or providers.

At source freeze, `go test -race ./scripts/faults/application -run
'^TestStrictLogs' -count=1 -v` passed 30 parent/subtest records with one
opt-in skip (package 1.037 seconds); `go vet ./scripts/faults/application`
exited zero. The raw logs are `/tmp/forge-strict-logs-final-offline-race.log`
and `/tmp/forge-strict-logs-final-vet.log`, retained for root archival.
These package timings measure offline checks only. Final source identity is
reported with the local fixture commit. Actual case durations, final resource counts,
ENOSPC outcome, source/binary attestations and independent raw review remain
**pending actual execution**. An offline PASS never changes those fields.

## Review correction after the initial fixture freeze

The initial `d494e85` freeze and its offline logs remain unchanged. Independent
source review found two acceptance defects, with no actual combined run yet:

- L5 previously accepted incomplete signal identity and trusted ordered summary
  lease timestamps. The revised pure oracle binds each of four signal reports
  to its separately recorded launch identity, exact PID/argv/executable/hash and
  strictly numeric start ticks. Original signals also bind their after-signal
  Docker/clock records; successor exits bracket the actual released cleanup.
  The historical E50 claimed snapshot is matched to the live PG row. Its
  `input_state` must equal the stopped-run snapshot with the **then-current SQL
  lease header** applied, so a prior heartbeat renewal is retained. The 30-second
  successor lease, epoch/version and both summary timestamps are recomputed from
  these authorities. Existing E48 captures were used only to verify field shape;
  they are not substituted for E50 evidence.
- L4 previously allowed the live worker to inspect/reconcile while the fixture
  was collecting full-filesystem evidence. It now identity-checks and SIGSTOPs
  only its own worker, confirms the stopped OS state, and requires at least
  24 seconds of real DB lease remaining. A context-independent 18-second
  watchdog resumes only the same still-bound process; `defer` resumes it on a
  failed assertion before process cleanup. Timeout always fails the case. The
  original operation is inspected to terminal using the real same-epoch lease
  before normal SIGCONT, then the production worker consumes that receipt. The
  selected fixed volume is reverified and pressure FD, spool and checkout must
  have the same device before the first pressure write.

New offline negatives include missing/nonnumeric ticks, changed PID/argv,
changed historical/live claimed rows and summary times, insufficient lease,
missing stopped confirmation, watchdog timeout and replacement-process identity.
This validates fixture guards only; actual pause duration, pressure/recovery and
all host acceptance observations remain pending. New logs use the distinct
`/tmp/forge-strict-logs-review-fix-*` prefix; the earlier freeze is not overwritten.

At the correction freeze, ordinary and race offline runs each passed 53
parent/subtest records with one opt-in skip; vet exited zero. Their logs are
`/tmp/forge-strict-logs-review-fix-frozen-{offline,race,vet}.log`. The pause gate
serializes actual STOP and CONT under one mutex: once watchdog recovery has
occurred, a late STOP is rejected. No actual process signal was sent by these
pure offline tests.
