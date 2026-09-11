# Implementation evidence — 2026-09-08

Forge has working database, reducer, provider, file-operation, RPC, API/CLI and
telemetry components. The evidence below supports those specific boundaries.
It does **not** establish that the entire SDE specification has passed. The
[140-row implementation map](implementation-map.md) keeps incomplete compound
requirements open.

The first implementation commit is `191cb89e06688df4f12d09e6e99712d15d8ea725`.
A clean-checkout run exposed a test isolation defect described below; that commit
is not claimed to have passed every delivery check. The measured scheduler report contains source hashes and exact
environment details. Earlier tests were reported through agent tool output and
the [independent review log](reviews/implementation-review.md); most original
stdout files were not retained. Those records are labeled accordingly rather
than reconstructed as raw logs. A retained race JSONL now records 233 passing test/subtest entries and zero
failures. Newer execution/load evidence is linked below. The corrected code
commit is `b924f1cc59583bfd0b812787aec98464cb997830`. Its source and independent
checkout checks are recorded in E13; earlier artifacts are not relabeled as
those checks.

| Record | What is supported | Evidence form | Main limitation |
| --- | --- | --- | --- |
| E01 | Pinned generation and compatibility checks | Source/generated files; observed generator exit status | Not a clean-clone or complete CI result |
| E02 | Pure/protocol/client/telemetry behavior | Existing test code, observed package results, independent review | Fixtures and in-memory exporters do not prove external execution |
| E03 | PostgreSQL transactions, quota, authorization and selected recovery | Actual local PostgreSQL tests, independent review records | Deterministic model/verifier; most raw stdout not retained |
| E04 | Runner journal/files/RPC boundaries | Actual SQLite/files/UDS/mTLS fixtures and review | Process backend is deterministic in these tests |
| E05 | 1,000 durable submissions and claims | Checked-in raw per-request JSON plus benchmark source | No terminal completion, model request or effect execution |
| E06 | Three actual Python repairs, rootless isolation/limits and terminal cleanup | CLI/run/artifact reports and real Docker logs | Scripted model; selected cases, not the entire fault matrix |
| E07 | Actual worker process exit and epoch-2 recovery | systemd exit log, ledger and repaired patch | One after-model-result boundary; not an active-writer crash |
| E08 | Fixed HTTP metadata workload at 50 requests/s | 1,000 raw latencies/status codes through nonowner RLS | 20-second local sample; execution excluded |
| E09 | 1,000 terminal simulated tasks with effects and quota | Per-run JSON and actual PostgreSQL invariant counts | Finite controller/FakeExecutor; not application.Driver or containers |
| E10 | 200 real TCP SSE connections and finite committed event delivery | Raw event latency, sequence and resource samples | Short window did not trigger slow-reader backpressure/disconnection |
| E11 | Five rounds of 50 actual SSE open/cancel lifecycles | Per-round TCP/handler/FD/goroutine/heap counts and pprof | Subsecond sample; no long-term memory stability claim |
| E12 | Actual Go repair with offline independent verification | CLI report, baseline/verification receipts and reapplied patch | Deterministic model and one Go fixture |
| E13 | Final source validation before first commit | Full race JSONL, vet and three generator logs | Opt-in workloads have separate execution records |
| E14 | Actual SQL backup and restoration | Custom-format backup checksum and restored record counts | SQL metadata only; no paired runner/workspace/object restoration |
| E15 | Actual slow-reader TCP disconnection with healthy sequence isolation | Separate default/constrained socket reports and profiles | Heavier one-run workload; not the exact 200-client baseline |
| E16 | Actual churn, heap diagnosis and six-minute prewarmed steady state | Original 24-cycle report plus 72-cycle strict repeat, per-backend counts and six profile times | Bounded local oscillation after row-pool saturation; finite window, no universal leak claim |
| [E20](sse-p02-evidence.md) | Exact 200-client/20-run SSE isolation, full cursor replay and missing hints | Same-parameter failure/fix/repeat, every raw delivery/publication/replay independently audited | Slow-client receive window explicit; production server defaults; no outage/model claim |
| [E21](application-fault-evidence.md) | Real worker F05/F07/F08 and PostgreSQL effect/quota/capacity reconciliation | Two worker PIDs, actual SIGKILL, natural lease expiry, Docker effects and 81 checked artifact files | Fake models and synthetic rates; phase exports are sequential, not atomic |
| [E22](../benchmarks/results/evaluation-oracle-v2-20260911/report.json) | Fixed eval-v2 source/oracle target and regression checks | All 16 actual isolated container outcomes match trusted grader assertions | Corpus validation only; no real-model quality or paid usage result |
| [E23](../benchmarks/results/delivery-validation-20260911/report.json) | Continuation whole-tree checks | Build, ordinary/race tests, vet, generated bindings and helper tests | Local working tree; original exact-commit CI remains separately identified |

## E01 — Schemas and generated code

The implementation locks Go dependencies and generator tools in
[`go.mod`](../go.mod) and [`go.sum`](../go.sum): Go 1.26.8 was used for the
recorded scheduler sample; SQLC is 1.31.1, oapi-codegen is 2.8.0 with runtime
1.6.0, and Go protobuf/gRPC generators are pinned as tool dependencies. These
are tested repository versions, not a claim that they are the newest upstream
releases.

The continuation agent actually ran these commands successfully after
[`00007_transition_audit.sql`](../db/migrations/00007_transition_audit.sql) was
added:

```sh
GOCACHE=/tmp/forge-runtime-gocache GOPROXY=off sh db/generate.sh
GOCACHE=/tmp/forge-runtime-gocache GOPROXY=off sh db/check-generated.sh
```

Both returned exit 0. SQLC checking now mirrors its inputs into a private
temporary directory, avoiding the broken absolute-path resolution of the
earlier script. [`Store.Events`](../internal/persistence/store.go) actually calls
the generated `PageRunEvents` query within its tenant-scoped repeatable-read
transaction. Future/expired cursor checks remain in Store and are not bypassed
by generation.

The HTTP schema/client agent previously reported passing
`sh api/check-generated.sh`, generated client compilation, and
[`tests/client/schema_test.go`](../tests/client/schema_test.go). This is a prior
execution record, not a newly captured raw log. Typed protobuf source/bindings
and RPC tests are under [`proto/runner/v1`](../proto/runner/v1) and
[`internal/runnerclient`](../internal/runnerclient). The root integration agent subsequently reported `make check-generated`
passing with protoc 36.1 after `go mod tidy`; the subsequent source and
clean-checkout logs are retained in E13. A new
[`proto/check-generated.sh`](../proto/check-generated.sh) and
[CI workflow](../.github/workflows/ci.yml) cover regeneration, builds, tests,
race/vet and helper checks. Actual execution results are recorded separately
in E13 rather than inferred from workflow source.

## E02 — Core, providers, clients and telemetry

[`internal/domain/types_test.go`](../internal/domain/types_test.go) uses exact
arithmetic checks, including a separate arbitrary-precision cost oracle.
[`internal/runtime/reducer_test.go`](../internal/runtime/reducer_test.go) covers
lease/version authority, sequential effects, adoption barriers, immutable
approval bindings, partial model output, trusted verification, budget stop
proofs, terminal monotonicity and malformed snapshots. Independent review
reproduced and verified fixes for the adoption bypass and invalid stop target
(IR-01/IR-02). Fuzz entrypoints exist for malformed snapshots, terminal behavior,
money and paths; the presence of seeds is not a claim of a long fuzz campaign.

The native OpenAI Responses and Anthropic Messages adapter tests use local
`httptest` servers and the pinned SDKs. They exercise fragmented/interleaved
arguments, final-turn validation, native continuation fields and signatures,
size/depth/schema bounds, SDK retries disabled, Retry-After, and stream failure.
An interrupted response yields no executable tool calls. See
[`provider/README.md`](../internal/provider/README.md),
[`provider_test.go`](../internal/provider/provider_test.go) and
[`adapters_test.go`](../internal/provider/adapters_test.go). These tests were
reported passing, including race checks, in earlier implementation output.
They made no paid API calls and establish no live model access or success rate.

CLI tests cover persistent body-bound idempotency, uncertain submission without
automatic mutation replay, reconnect/deduplication, expired cursor reset,
saved approval bindings, credential redirect rejection, and integrity-checked
downloads. Generated schema tests compare actual Go wire types and route
coverage. See [`cmd/forge/client_test.go`](../cmd/forge/client_test.go) and
[`tests/client/schema_test.go`](../tests/client/schema_test.go). These are local
protocol fixtures. The separate real CLI-to-container demonstrations are now
recorded under E06; they do not retroactively change the fixture tests
into container tests.

The continuation telemetry agent actually ran:

```sh
GOCACHE=/tmp/forge-runtime-gocache GOPROXY=off go test -race ./internal/telemetry
GOCACHE=/tmp/forge-runtime-gocache GOPROXY=off go vet ./internal/telemetry ./benchmarks
```

Final observed telemetry result: pass, 1.071 seconds; vet returned exit 0.
[`telemetry_test.go`](../internal/telemetry/telemetry_test.go) checks persisted
traceparent continuity into a worker/model span, real SSE writer flushing,
concurrent once-only end hooks, bounded labels, unknown-versus-zero usage,
secret exclusion, invalid traceparent rejection and panic cleanup. It uses an
in-memory OTel exporter. No real collector delivery or crash/adoption trace was
measured. Business hooks still need to be exercised at every actual boundary;
registered metrics alone are not business evidence.

The integration owner reported that all intended package tests passed in an
earlier `go test ./...` attempt, but that command **failed overall** because
mutable runner workspaces under `var/` were also scanned as Go packages. A
nested `var/go.mod` was then introduced to separate runtime data. The post-fix
[race JSONL](../benchmarks/results/validation-race-20260908.jsonl) now records
233 passing test/subtest entries, zero failing entries, one skipped opt-in
scheduler benchmark, and 15 packages with tests passing. Thirteen package-level
skips correspond to packages without runnable tests in this record. Its observed
window is 2026-09-08 07:59:44–07:59:57 UTC. Later Docker/load/cleanup additions
need the integration owner's final unified validation; this earlier log is not
presented as a check of code added after its timestamp.

## E03 — Actual PostgreSQL protocol and authorization tests

Tests use the dedicated loopback Forge development database. The observed
instance was PostgreSQL 17.11 with durable commit settings enabled. Review and
application helpers create uniquely named schemas and drop only their own
fixtures. Non-owner authorization tests create temporary `NOLOGIN`,
`NOSUPERUSER`, `NOBYPASSRLS` roles and use those actual roles through the pool;
they are not mocked RLS tests. The test administrator connection is separate
from the role whose HTTP/RLS behavior is measured.

The continuation SQLC agent actually executed:

```sh
FORGE_TEST_DATABASE_URL='<dedicated loopback /forge DSN>' \
GOCACHE=/tmp/forge-runtime-gocache GOPROXY=off \
go test -race ./internal/persistence -run '^TestConcurrentSubmitAndCursor$' -count=1
```

Observed result: pass, 1.229 seconds. The test launches 40 same-key submissions,
checks one logical run, rejects changed-body replay, and checks future cursor
and other-tenant reads. The placeholder above intentionally omits connection
credentials; it documents the command form rather than pretending to be a raw
terminal transcript. This test predates the final retention audit; that newer
behavior is covered by the separate review tests below.

The independent review log records passing real PostgreSQL/race checks for:

- Composite step/effect/approval parent constraints and actual tenant RLS,
  including pooled connection-local tenant context cleanup.
- Heartbeat-authoritative lease expiry and a commit blocked on a real
  PostgreSQL artifact-table lock until the old lease expires. The stale writer
  cannot commit after the lock is released.
- Deferred model runs reclaimed at a higher epoch, without corrupting the
  old running snapshot's owner/epoch.
- API startup rejection of inherited ownership and ownership in the actual
  schema resolved by `search_path`.
- Canonical JSON rejection/round trip, including escaped HTML and integer
  `9007199254740993`, without changing the originally hashed argument bytes.
- Real HTTP auth, revoked credentials, membership, viewer write rejection,
  cross-tenant run/artifact denial, correct local artifact bytes, committed SSE
  replay and future-cursor rejection through a non-owner database role.
- Expired event cursors and a cutoff moving between snapshot admission and
  replay returning reset/410 rather than silently omitting history.
- Actual terminal event trimming preserving running, approval, reconciliation,
  cancellation-requested, queued and recent-terminal histories. Repeated trim
  removes zero further events. Each new snapshot's saved `input_state` and
  `input_event` reproduces the committed snapshot through the pure reducer,
  even after heartbeat renewal and event trimming.
- Fixed worker transport/runner placement and takeover remaining bound to the
  configured runner rather than using another runner's allocation.

The tests are in [`tests/review`](../tests/review); their precise coverage and
finding history are recorded in
[`implementation-review.md`](reviews/implementation-review.md). The review continuation records the complete package passing
`go test -race ./tests/review -count=1` against the explicit local review
database in 9.290 seconds, followed by a passing strengthened IR-18 reopen
case. The review log is the retained execution record; do not relabel it as
original raw stdout.
Legacy snapshots with NULL audit inputs remain explicitly unreplayable rather
than being presented as reconstructed history. Event trimming does not prove
artifact/workspace garbage collection.

Quota tests in
[`internal/quota/store_integration_test.go`](../internal/quota/store_integration_test.go)
exercise concurrent admission across independent pools and tenants, idempotent
settlement, actual overage, unknown usage across deadline/window changes, and
one shared half-open probe. Request concurrency release does not refund
unknown token/cost obligations. The module's protocol is documented in
[`quota/README.md`](../internal/quota/README.md).

Application/pricing tests use real PostgreSQL, SQLite and actual repository
files, with a deterministic model and an explicitly no-process backend. They
verify a patch loop, an injected driver stop after complete model-result storage, unchanged
provider invocation count on recovery, frozen pricing despite changed/removed
configuration, persistent retry decisions, overall retry budgets and neutral
circuit treatment of local failures. The review also verifies that repair only
occurs after durable verifier diagnostics reach portable and native context,
and that an input exceeding the frozen reservation is rejected before provider
dispatch. See [`driver_integration_test.go`](../internal/application/driver_integration_test.go),
[`pricing_integration_test.go`](../internal/application/pricing_integration_test.go)
and [`tests/review/application_test.go`](../tests/review/application_test.go).
The model-result crash test injects `context.Canceled` from a fault hook,
expires the lease and reclaims through a different owner name in the same
process. It does not kill/restart an operating-system worker process.
The verifier in these tests reads actual file contents but does not execute
Python. Its `verified` fixture state is protocol evidence, not real code-eval
success.

## E04 — Runner files, journal and transport

The runner tests use actual files, `os.Root`, SQLite journaling and workspace
file locks. They cover immutable operation bindings, concurrent same-ID patch
admission, actual before/after file hashes, receipt-window recovery without
reapplying a patch, partial multi-file uncertainty, stable job identity after
journal reopen, expired grant rejection after waiting, snapshots/release and
cross-process workspace lock exclusion. The process backend is deterministic;
these tests do not launch Docker tasks.

The review independently verified post-lock release expiry and preservation of
an already successful operation when cancellation arrives later. It also
reproduced the delayed same-epoch adoption bug. A persistent stopped-admission
marker was then implemented. The review continuation records a passing
strengthened same-epoch/reopen regression with race detection. This confirms
the SQLite admission fix while the real stop/adopt/container gate remains open. See
[`internal/runner/storage_test.go`](../internal/runner/storage_test.go) and
[`tests/review/runner_test.go`](../tests/review/runner_test.go).

The RPC agent reported passing actual UDS permission/typed transport fixtures,
mutual TLS identity rejection, bounded result handling and Start timeout
inspection of the same operation ID without resubmitting Start. Tests are in
[`internal/runnerclient/transport_test.go`](../internal/runnerclient/transport_test.go).
The endpoint is a fixture service, not proof that a real container ran once.

## E05 — Raw scheduler sample

The [checked-in raw JSON](../benchmarks/results/scheduler-20260908T074355.805892677Z.json)
was generated by an actual passing
[`TestSchedulerCompetitionEvidence`](../benchmarks/scheduler_test.go) invocation
on 2026-09-08. It contains 1,100 individual submission/replay latencies, 1,000
successful claim latencies, hardware/Go/PostgreSQL/pool settings, source hashes,
workload bounds and invariant results. The copied checked-in report was
byte-compared with the original generated file.

Eight goroutines submit 1,000 logical runs across 10 tenants, with 100 explicit
same-key replays. Two worker goroutines then compete for all 1,000 runs using
the real scheduler and database; they share one pool and one synthetic runner
registration. Submission and claim are separate phases. No HTTP endpoint,
model request, runner operation or shell command is called.

| Observation | Measured value |
| --- | ---: |
| Submission/replay requests | 1,100 |
| Submission/replay rate | 2,106.4 requests/s |
| Submission p50 / p95 / p99 | 3.685 / 5.653 / 8.543 ms |
| Successful claims | 1,000 |
| Claim rate including retry wall time | 128.9 claims/s |
| Successful claim p50 / p95 / p99 | 6.477 / 10.758 / 15.969 ms |
| Claims per worker | 482 / 518 |
| Contiguous created/claimed events | 2,000 |
| Snapshots | 2,000 |
| Reserved allocations / tenant active / runner slots | 1,000 / 1,000 / 1,000 |
| Model-attempt rows / effect rows | 0 / 0 |
| Capacity-or-runner-lock polls / empty polls | 795 / 1,717 |

The sample passed in 8.519 seconds on a shared Linux amd64 development machine
with an i9-13900K, 32 logical CPUs, Go 1.26.8, PostgreSQL 17.11,
`fsync=on`, `synchronous_commit=on`, and pool max 16/min 1. This is one observed
sample, not a production capacity/SLO estimate. Successful-call latency excludes
unsuccessful polls; aggregate wall rate includes their cost. The single runner
row creates visible `SKIP LOCKED` contention, which the report does not hide.

Runs stop this experiment in `running` at epoch 1 and are deleted with their
private fixture schema. The result proves no lost/duplicate **claim** in this
sample. It does not prove 1,000 successful terminal tasks, exactly-once effects,
50 HTTP requests/s, 200 SSE clients, real executor throughput or live-model
success. Reproduction and interpretation are in
[`benchmarks/README.md`](../benchmarks/README.md).

## E06 — Real repairs, Docker isolation and cleanup

Storage preparation used the independently reviewed fixed ext4 image helper.
The operator supplied successful mount output for four slots, each with a
268,435,456-byte device, 234,594,304-byte filesystem and 65,536 inodes; the
preallocated image pool is 1 GiB. The assistant did not execute the operator's
privileged mounting command. The dedicated daemon was separately observed as
rootless with cgroup v2/systemd. Subsequent tests now exercise actual containers
through that runner, rather than stopping at setup observations.

The [three-repair report](../benchmarks/results/repairs-20260908T080253Z/report.json)
and its per-fixture directories contain actual CLI submissions, HTTP run state,
SSE events, downloaded artifacts, exported patches and reproduced source files.
The execution path uses real PostgreSQL, gRPC, SQLite, fixed workspace storage
and rootless Docker. The model is deterministic: these are execution-platform
checks, not evidence that a live language model solved previously unseen tasks.

| Fixture | Run | Database-observed duration | Result |
| --- | --- | ---: | --- |
| clamp | `run_WFH3XOP7FF7GFNFARNUPSR6VXW` | 3.457492 s | Completed, verified, baseline target failed, exported patch reapplied |
| expiry | `run_3GDLQQ7XWGFT6L3U2GMUWGFKK7` | 3.147722 s | Completed, verified, baseline target failed, exported patch reapplied |
| ranges | `run_RU75WLOSWLEOEYCRJCSLVQG5A6` | 3.354628 s | Completed, verified, baseline target failed, exported patch reapplied |

The initial [real isolation log](../benchmarks/results/real-docker-isolation-20260908.log)
passed in 2.335 seconds. The strengthened
[real limits log](../benchmarks/results/real-docker-limits-20260908.log) passed
in 5.027 seconds. The corresponding
[`docker_integration_test.go`](../internal/runnerclient/docker_integration_test.go)
asserts these actual task/container observations:

- UID/GID 1000, zero effective capabilities, `NoNewPrivs=1`, a read-only root
  filesystem, bounded `/tmp`, only loopback networking and no Docker socket.
- A task-created 0600 file inside a 0700 directory can be read and patched by
  the mapped runner without making it generally world-accessible.
- Cgroup `memory.max=268435456`, `pids.max=64` and
  `cpu.max=100000 100000`. CPU quota is inspected; this is not a CPU-throttling
  timing experiment.
- PID pressure forks 62 children before EAGAIN, with `pids.events` increasing;
  the memory-exhaustion operation exits 137 with `OOMKilled=true`.
- The fixed workspace reaches ENOSPC after 228,589,568 allocated payload bytes;
  its filesystem capacity remains 234,594,304 bytes.
- Repeating the exact command operation ID returns the original receipt and
  leaves the actual append counter at one, not two.
- Before Stop, the owned container cgroup has three tasks. After the durable
  stop receipt it has zero tasks and is removed. The operation exits 137,
  `NoActiveOperations=true`, and delayed same-epoch adoption is rejected.

These results close the tested process/isolation/resource cases. They do not
establish every launch/receipt crash window, old/new writer interleaving,
other-slot availability under ENOSPC, or long-duration resource stability.
No blanket statement that the entire runner fault matrix passed is justified.

The [actual workspace cleanup log](../benchmarks/results/real-workspace-cleanup-20260908.log)
records terminal clamp cleanup `cleanup_SBTIT46GVYPR55BVGZO643RETA` reaching
`released` only after snapshot artifact
`artifact_fd10a47cbc8c315f72922de329b97f479b0b2f912d65470a2804f98193f5baac`
was saved. [Operations guidance](operations.md) documents its stop/seal/publish/
release protocol, eligibility guards and separate cleanup lease. This proves
one actual terminal workspace cleanup path. It does not prove every cleanup
crash window, artifact orphan collection, or paired database/workspace restore.
Idempotency keys remain beyond their declared minimum retention period; no
automatic key deletion is claimed.

The Go RepoProfile has actual execution evidence in E12; SQL-only restoration
is recorded in E14. Native model evaluation, paired backup/restore and the
remaining fault/load cases are still open.

## E07 — Actual operating-system worker crash and recovery

The [worker log](../benchmarks/results/worker-crash-20260908.log) shows the
separate `forge-runtime-crash-worker.service` process exiting with code 86 at
08:08:54 UTC after its fake-model result was durably stored. The
[`forge-worker` entrypoint](../cmd/forge-worker/main.go) uses `os.Exit(86)` for
this explicit fake-only injection. This is distinct from the earlier in-process
`context.Canceled` unit fixture described under E03.

A different worker recovered
`run_K6VTFFCNYOARPLMUN5734RRNP2` at lease epoch 2. Its
[ledger](../benchmarks/results/worker-crash-ledger-20260908.json) records one
completed attempt for each of three steps, three model-start events, the
expected effect outcomes, a released allocation and zero outstanding
reservations. The [recovered repair report](../benchmarks/results/repairs-20260908T080852Z/report.json)
records completed/verified state, the failing baseline and a successfully
reapplied exported patch. Its total database-observed run duration is
32.875917 seconds. That total includes initial work and later verification;
it is not mislabeled as a separately measured recovery-only interval.

This is real process recovery at one named boundary with a deterministic
provider and real runner execution. It does not cover an active old command
still writing when the lease expires, runner process loss during Docker start,
or paid-provider uncertainty after an unobserved remote response.

## E08 — HTTP metadata at fixed offered load

The [HTTP raw report](../benchmarks/results/http-20260908T081337Z.json) and
[test stdout](../benchmarks/results/http-metadata-20260908.log) record 1,000
requests over a 20-second window, offered at 50 requests/s after 100 warmup
reads. The mix is 90% run reads and 10% task admissions, with a maximum of 16
inflight requests and a pool maximum of 16. The test uses an actual TCP HTTP
server and a real nonowner/NOBYPASSRLS PostgreSQL role, not a direct-handler
microbenchmark.

The sample achieved 49.99648 requests/s, p50 0.854613 ms, p95 4.106876 ms and
p99 7.027606 ms, with zero request failures. The database contains the seed plus
100 newly admitted runs. This meets the source plan's P01 latency targets for
this specific warm local workload. It does not measure task completion, model
or container latency, 200 SSE clients, production scale, or a sustained SLO.
The workload is implemented in [`benchmarks/http_test.go`](../benchmarks/http_test.go).

## E09 — 1,000 terminal tasks with a simulated executor

The [terminal raw JSON](../benchmarks/results/terminal-simulation-20260908T082015.827012264Z.json)
and [stdout](../benchmarks/results/terminal-simulation-20260908.log) record a
passing 1,000-task run in 72.196 seconds. Two worker goroutines each completed
500 tasks across ten tenants. The measured execution phase was 71.404840
seconds, or 14.004653 terminal runs/s; per-run p50/p95/p99 were
132.476242/199.318074/249.205390 ms. Each raw run record includes its terminal
state, version/event boundary, receipt and duplicate-confirmation result.

This benchmark calls actual PostgreSQL Store transitions, persisted model
attempts and shared quota APIs, writes real temporary local artifact bytes,
and runs actual `FakeProvider` stream assembly. Its controller is a finite
script in [`terminal_test.go`](../benchmarks/terminal_test.go), and its executor
is an idempotent in-memory fixture. It does **not** call `application.Driver`,
start a container, execute a command, contact a paid model or kill a process.
Synthetic verification deliberately produces `regression_only`, not a claim
that real code was verified.

The raw invariant checks record:

| Observation | Result |
| --- | ---: |
| Completed runs / `run.finished` events | 1,000 / 1,000 |
| Unique simulated executions / confirmed effect rows | 1,000 / 1,000 |
| Simulated Start requests / duplicate Store confirmations rejected | 2,000 / 1,000 |
| Fake-provider calls / completed attempts | 2,000 / 2,000 |
| Repeated settlement calls | 2,000, without duplicate charges |
| Event rows / snapshots / event gaps | 21,000 / 12,000 / 0 |
| Released allocations | 1,000 |
| Tenant active / runner reserved / provider active requests | 0 / 0 / 0 |
| Provider reserved tokens / reserved microUSD | 0 / 0 |
| Missing ready receipts / unsettled reservations | 0 / 0 |
| Committed synthetic tokens / microUSD | 96,000 / 6,000 |

The nonzero committed consumption is retained accounting, not a leaked
reservation. All outstanding resources return to zero. The fixture's executor
deduplication is an explicit assumption; PostgreSQL effect IDs, receipts,
rejected repeat confirmations and settlement totals are independently compared
against it. This closes the specified 1,000-task **simulated control-plane**
workload, not production runner exactly-once behavior. The actual same-ID
container command counter is separate evidence under E06.

## E10 — 200 TCP SSE connections with exact-size durable events

The [SSE raw report](../benchmarks/results/sse-20260908T082217.145386435Z.json)
and [stdout](../benchmarks/results/sse-fanout-20260908.log) record the actual
[`sse_test.go`](../benchmarks/sse_test.go) workload passing in 12.925 seconds.
It establishes 200 TCP HTTP/1.1 connections across 20 runs: 180 consumers read
normally and 20 stop reading response bodies after receiving headers. The
publisher commits five events/run/second for 12 seconds, each containing
exactly 1,024 stored payload bytes. PostgreSQL is real and durable; API and
writer pool maxima are each 16. The hub uses its normal 100 ms polling,
512-event history and 128-entry subscriber queue.

All 1,200 scheduled durable events were present with continuous sequences.
The 180 healthy clients received 10,800 unique deliveries with zero gaps,
duplicates, publication errors or failed/incomplete clients. Database-event
creation to client-receive latency was p50 91.231531 ms, p95 96.131086 ms and
p99 96.644534 ms. This meets the healthy-consumer latency and finite delivery
parts of P02 on this local sample. The database and client timestamps assume
the same host clock; this is not a multi-host clock-skew experiment.

The nonreading clients produced **zero** early server returns and **zero**
hub queue overflows. TCP buffers could absorb this 60 KiB/run window; the
sample therefore does not establish actual slow-client disconnection or
isolation under sustained backpressure. All subscriptions are established
before publishing, so this report also does not prove reconnect/retention
behavior or lost-NOTIFY recovery under this load. This original sample alone left P02 partial; [E20](sse-p02-evidence.md) records the later exact-workload acceptance. Actual transport backpressure at a separately
reported heavier workload was later verified in E15.

With the listener and pools kept warm, process goroutines returned from the
connection peak to the initial six and FDs to the initial 42 after clients
closed. API, clients, publisher and measurement harness share the same Go
process; those are combined resource measurements. A single start/end sample
does not establish long-term stability. The independent finite lifecycle
check below provides additional bounded evidence.

## E11 — Finite SSE connection and cancellation churn

The [churn report](../benchmarks/results/churn-20260908T082535.705148309Z/report.json)
and [stdout](../benchmarks/results/sse-churn-20260908.log) record five rounds
of 50 actual TCP SSE connections using the real PostgreSQL auth/RLS API.
The test passed in 0.810 seconds, with a reported measured interval of
0.692040 seconds. Each round opens all 50 connections and then cancels them;
the warmed listener and database pools remain open for measurement.

After every round, actual open TCP connections and active SSE handlers are
zero, each handler start has a corresponding return, and the process returns
to six goroutines and 42 FDs. Post-GC heap allocation for the five closed-round
samples is 5,923,904; 6,003,824; 6,107,408; 6,144,832; and 6,199,096 bytes.
The separate warmed one-connection baseline is 4,543,072 bytes. Heap grows
within this short sample and has **no leak/plateau assertion**; the observations
must not be presented as memory having returned to baseline.

The retained [heap profile](../benchmarks/results/churn-20260908T082535.705148309Z/heap.pprof)
and [goroutine profile](../benchmarks/results/churn-20260908T082535.705148309Z/goroutine.pprof)
accompany the exact configuration, machine, source hashes and measurements.
[`churn_test.go`](../benchmarks/churn_test.go) tests bounded connection/handler
cleanup. It publishes no events and runs no model, effect or container. Five
subsecond rounds do not prove absence of sustained growth, slow-client
backpressure behavior or long-term process stability; P05 remains partial.
The later E16 record adds the original two-minute experiment, its positive-growth diagnosis, and a stricter six-minute prewarmed repeat.

## E12 — Actual Go repository profile

The [Go CLI report](../benchmarks/results/repairs-20260908T083352Z/report.json)
records run `run_TZVQ2JBGYUBMQ7SW65XYIKPM5A` completing with trusted verification
in 25.428452 seconds from database admission to the terminal event. Its initial
target failed; the repaired integer-division implementation passed the target
and regression suites. The downloaded patch was applied to an independent
checkout and both `main.go` and `go.mod` matched the expected tree.

The pinned Go image was
`golang@sha256:9fdc884aacc3bec89b20ffc69f4bb369c78210e3e4f600387b5128b12c199f81`.
The first preflight exposed `noexec` on Docker's temporary mount; the
[diagnostic](../benchmarks/results/go-profile-diagnose-20260908.log) records the
actual mount and file modes. The Go profile now explicitly permits execution
inside the existing 64 MiB scratch mount. Python keeps explicit `noexec`.
The [corrected preflight](../benchmarks/results/go-profile-exec-preflight-20260908.log)
passed eight regression cases, and the complete CLI artifacts above establish
the baseline/repair/verification sequence. No API credentials or live model
were used. Independent review of the Go profile found no new P1/P2 issue in
that bounded change; this is not a general audit of arbitrary hostile code.

## E13 — Whole-repository source checks and clean-checkout correction

After business code was frozen, the integration owner ran `go test -race
-count=1 -json ./...` against the isolated local PostgreSQL test fixtures.
The [retained JSONL](../benchmarks/results/validation-final-race-20260908.jsonl)
contains **240 test/subtest passes, zero failures and six opt-in skips**;
16 packages passed and 12 packages had no enabled tests. Opt-in performance
and real-container results are recorded separately above, not treated as
executions inside this command.

`go vet ./...` completed with exit 0 and an
[empty diagnostic log](../benchmarks/results/validation-final-vet-20260908.log).
`make check-generated` completed with exit 0 for OpenAPI, SQLC and protobuf;
its [log](../benchmarks/results/validation-final-generated-20260908.log) records
the three commands. The Python suites passed
[14 volume-helper tests](../benchmarks/results/volume-helper-tests-20260908.log)
and [eight runner-configuration tests](../benchmarks/results/runner-config-tests-20260908.log).
The command ran in the working tree immediately before its first commit.
A subsequent [clean-checkout run](../benchmarks/results/clean-checkout-initial-failure-191cb89.jsonl)
failed while cleaning a persistence fixture: that early helper still used the
public schema, so a live worker could claim a test run and append a snapshot
during cleanup. This invalidated the original claim that every integration
helper was isolated. Persistence and quota helpers now use a new private schema
per test; cross-tenant quota pools deliberately share their one test schema.
The one identified terminal leftover was removed by exact fixture tenant ID;
demo runs were preserved. The fix changes test setup, not production scheduling.

The first GitHub job also failed before checkout because the runner parsed the
single-quoted Docker health command incorrectly. The workflow now uses double
quotes. Neither initial failure is reported as a passing CI result.

After isolation was corrected, the full
[race rerun](../benchmarks/results/validation-isolated-race-20260908.jsonl)
passed 240 tests/subtests with six opt-in skips while both live demo workers
remained running. Vet and all generators also passed. An independent Git clone
of `818d78f`, without local environment files, keys, journal or images, then
passed build, [ordinary Go tests](../benchmarks/results/clean-checkout-tests-818d78f.jsonl),
[all generators](../benchmarks/results/clean-checkout-generated-818d78f.log),
[22 Python tests](../benchmarks/results/clean-checkout-python-818d78f.log), and
[storage planning only](../benchmarks/results/clean-checkout-volume-plan-818d78f.json).
It left no tracked changes. This reproduction reused installed Go caches and
the existing isolated PostgreSQL service; it is not a fresh-machine container
deployment. See the [delivery manifest](../benchmarks/results/delivery-validation-818d78f.json).

All four retained demo workspaces were then sealed and released through the
actual operator cleanup workflow. The [cleanup log](../benchmarks/results/demo-workspace-cleanup-20260908.log)
retains their snapshot references; SQLite reported zero active volume leases
and four configured slots. Receipts, artifacts, database run history, mounted
images and source fixtures remain available.

Hosted CI then exposed two rollback-only SQL review tests that still expected
a pre-migrated public schema. Commit `b924f1c` gives those tests their own
migrated schema and grants the temporary nonowner role access to that exact
schema. Their original FK, tenant and transaction-rollback assertions remain.
The fix was independently reviewed. A new, initially empty PostgreSQL 17.11
instance limited to 512 MiB and two CPUs, with Go `GOMAXPROCS=2`, passed
[ordinary tests](../benchmarks/results/fresh-database-tests-20260908.jsonl) and
[race tests](../benchmarks/results/fresh-database-race-20260908.jsonl): 240 passes,
zero failures in each. Vet also passed. An independent clone of `b924f1c`
then passed build, [240 tests](../benchmarks/results/clean-checkout-tests-b924f1c.jsonl)
and [all generation checks](../benchmarks/results/clean-checkout-generated-b924f1c.log),
leaving zero tracked changes. The [final delivery manifest](../benchmarks/results/delivery-validation-b924f1c.json)
distinguishes this from the earlier checkout and the hosted CI result.

Hosted [GitHub Actions run 34206855353](https://github.com/JDinSeattle/forge-runtime/actions/runs/34206855353)
completed successfully for exact commit `b924f1cc59583bfd0b812787aec98464cb997830`:
build, ordinary tests, race tests, vet, all three generators and both Python
helper suites passed. The [captured job/step result](../benchmarks/results/github-ci-b924f1c.json)
retains the conclusion, timestamps and commit identity. Subsequent delivery
changes only add documentation and captured evidence; runtime and test code
remain identical to that tested commit.

## E14 — SQL metadata backup and restore

An actual `pg_dump --format=custom --no-owner --no-acl` of the idle demo database
was restored with `pg_restore --exit-on-error` into the empty disposable
PostgreSQL instance. The [retained report](../benchmarks/results/database-restore-20260908.json)
records the dump's byte size/checksum and matching source/restored counts:
five runs, 87 audit snapshots, 208 events, 30 effects, 15 model attempts,
101 artifact records, five cleanup records and zero reserved allocations.
The private dump remains in ignored local backup storage; its contents were
not committed. The disposable restore instance was then removed.

No worker was connected to the restored database. Roles/ACLs were excluded,
and runner journals, volume images and artifact bytes were not restored.
This validates SQL restoration and its metadata relationships, not complete
execution recovery or artifact readability after host loss.

## E15 — Actual default and constrained TCP backpressure, 2026-09-11

The new [`TestSSEBackpressureEvidence`](../benchmarks/sse_sustained_test.go)
passes two independently reported network experiments using the actual SSE
handler, a real PostgreSQL private schema and a nonowner RLS API role. Each
has one synthetic run, four healthy consumers and one client that stops reading
its response body after headers. It publishes 1,000 exact 16-KiB durable payloads
at 50 events/s over 20 seconds. This deliberately heavier workload is separate
from E10's exact 200-client/20-run/1-KiB/5-Hz sample; no production queue, poll
interval or write deadline was reduced to obtain these results.

| Socket mode | Healthy deliveries | p95 / p99 | Actual slow handler / TCP closes | Durable events committed after slow TCP close |
| --- | ---: | ---: | ---: | ---: |
| [Unmodified defaults](../benchmarks/results/sse-backpressure-default_socket-20260911T185400.450639770Z/report.json) | 4,000 | 98.172987 / 101.105084 ms | 1 / 1 | 581 |
| [Explicitly constrained sockets](../benchmarks/results/sse-backpressure-constrained_socket-20260911T185421.131587097Z/report.json) | 4,000 | 98.371661 / 101.373007 ms | 1 / 1 | 746 |

Both reports have 1,000 durable events, exact 16,384-byte stored payloads, zero
sequence gaps and complete consecutive delivery to all four healthy readers.
The continuation agent independently recomputed both latency quantiles and
verified every raw healthy sequence. Each mode observed one queue overflow and
one `slow_subscriber` metric. The handler returned and its actual server TCP
connection closed **before** harness cancellation. Only then was the slow body
drained: 2,755,688 buffered bytes for defaults and 12,295 for the constrained
mode, both ending in `unexpected EOF`. That error is the observed termination
of an incomplete chunked stream; it was not manufactured by client cancellation.
All remaining TCP connections and handlers reached zero after normal cleanup.

Default descriptors initially reported a 2,626,560-byte send buffer and
131,072-byte receive buffer; Linux may subsequently autotune them. The constrained
mode requested 4,096-byte server send and slow-client receive buffers; actual
values were 8,192 bytes. Healthy client receive buffers retained their default.
The reports retain each observed socket size and exact publication timestamps;
the measured publish durations were 20.015121 and 20.004659 seconds respectively.

[`internal/httpapi/sse.go`](../internal/httpapi/sse.go) now classifies network
write timeouts as slow-subscriber closures and preserves the stream error when
selecting a closed event channel. The real experiments verify the resulting
metric together with actual connection termination. The initial execution failed
before serving traffic because the new fixture requested a 15-minute lease above
Store's five-minute maximum; the fixture was corrected to five minutes and both
modes reran successfully. This setup failure is not a product backpressure failure.

Each directory retains its final heap and goroutine profiles. Those profiles
include the benchmark's retained raw samples; they are not used as the sustained
heap-plateau proof. The tested files' SHA256 values matched the inspected source.
The manifest records the base Git commit and dirty-tree status, so these working
tree results are not retroactively described as pristine-commit measurements.
No model, runner or container operation was invoked. These finite local samples
prove the specified actual transport isolation cases; they do not establish a
multi-host SLO, lost-NOTIFY recovery, or the full P02 workload under sustained
backpressure. [E20](sse-p02-evidence.md) later supplies that exact workload with
actual closure and complete replay, preserving its initial metric failure.
Reproduction and threshold definitions are in
[the experiment protocol](../benchmarks/SSE.md).

## E16 — Two-minute connection and heap observation, 2026-09-11

The [sustained churn report](../benchmarks/results/sse-sustained-churn-20260911T185535.771934398Z/report.json)
records `TestSSESustainedChurnEvidence` passing in 120.811 seconds, with a
120.053866-second measured interval. Three full-size warm-up cycles precede
24 measured five-second cycles. Each opens 100 real TCP/RLS-authenticated SSE
subscriptions, publishes ten durable 1-KiB events, checks all 1,000 healthy
deliveries, cancels the requests and waits for actual TCP/handler cleanup.
This is 2,400 measured connections and 24,000 measured healthy deliveries;
including warmup, the database has 270 event rows of this type with no sequence
gaps. No model, effect or container operation was involved.

Every measured cycle reports exactly 100 handler starts and returns, zero
remaining TCP connections/handlers, six goroutines and 42 FDs. The continuation
agent independently checked every cycle and recomputed the heap criteria:

| Post-GC HeapAlloc observation | Actual value | Predeclared limit |
| --- | ---: | ---: |
| First six samples' mean | 7,103,970.7 bytes | Reference window |
| Last six samples' mean | 7,560,397.3 bytes | Comparison window |
| Mean-window growth | 456,426.7 bytes | At most 1,048,576 bytes |
| Last 12 samples' OLS slope | 7,163.05 bytes/s | At most 8,192 bytes/s |

The actual HeapAlloc samples range from 6,957,136 to 7,733,888 bytes. These
results pass the experiment's fixed finite-window growth tolerances. The
positive slope is retained explicitly: memory did **not** return exactly to its
initial value, and this is not proof of a flat heap or absence of a long-term
leak. This initial result alone left P05 partial; the stricter follow-up below
provides the later bounded steady-state evidence. The last approximately 15 seconds shared the
host with unrelated application/retention compilation and race tests; this was
not an idle dedicated benchmark machine.

The [profile directory](../benchmarks/results/sse-sustained-churn-20260911T185535.771934398Z)
contains heap and goroutine pprof snapshots after warmup, rounds 6 and 12, and
at the end. The final goroutine profile accounts for six goroutines, including
the measurement, listener and database-pool maintenance. Sampled heap-profile
differences contain both retained and released allocations; their statistical
sampling is distinct from the exact MemStats values above. API, clients and
publisher share the measured Go process, while PostgreSQL runs separately.
The harness preallocates cycle records and clears per-socket observations so
retained raw delivery samples do not manufacture the measured trend.

The report retains configuration, pool sizes, source hashes, every closed/open
resource sample and exact sequence totals. All recorded source hashes matched
the implementation inspected after execution. The benchmark protocol states
its thresholds and reproduction commands before the measured results; no
threshold was relaxed to obtain a pass. The positive trend triggered the
independent diagnosis and stricter experiment below.

### E16 follow-up — Prewarmed six-minute steady state

The initial heap-profile differential identified retained `pgx.(*Conn).getRows`
objects through `Store.Authenticate`. Inspection of pinned pgx v5.10.0 found a
bounded mechanism: each physical connection retains closed `baseRows` through
its current `poolRow` backing array. The initial 64-row batch grows to a maximum
128-row batch and is replaced as calls consume it. `baseRows.Close` clears values,
scan plans/types, context, SQL and arguments. This source-level explanation was
an inference about the initial trend, so it was tested rather than accepted as
proof of a plateau.

[`TestSSESteadyStateEvidence`](../benchmarks/sse_plateau_test.go) and its
[prewarm/oracle helper](../benchmarks/sse_steady_test.go) retain the production
16-connection API pool and unmodified socket settings. Prewarm exercises every
physical connection through at least 256 real authenticated `Pool.QueryRow`
calls, followed by three full 100-client TCP cycles. The independently frozen
oracle then requires 72 five-second cycles, at least 256 further auth calls per
backend, first/last 12-sample mean growth ≤256 KiB, last-half OLS slope ≤1 KiB/s,
and a full-window post-GC heap range ≤1,536 KiB. No limit was adjusted after
observing this run.

The [raw steady-state report](../benchmarks/results/sse-steady-state-20260911T191244.281144193Z/report.json)
records **PASS**, 361.310 seconds test duration and 360.221470 seconds measured.
All 72 cycles complete 100 actual TCP connections and 1,000 consecutive healthy
deliveries, giving 7,200 connections and 72,000 deliveries. Every cycle returns
to zero active TCP connections/handlers, six goroutines and 42 FDs. There are
750 durable events including warmup, no durable sequence gaps, a matching final
API cursor and no slow queue overflow. The
[independent raw-data audit](../benchmarks/results/sse-steady-state-20260911T191244.281144193Z/independent-audit.json)
checks each cycle, cursor continuity, backend identity/counts, source hashes and
recomputes the statistics below.

| Post-GC heap measurement | Actual value | Frozen limit |
| --- | ---: | ---: |
| First / last 12-sample mean | 7,374,188 / 7,632,050 bytes | Reference / comparison |
| Mean-window difference | 257,862 bytes | 262,144 bytes |
| Last 36 samples' OLS slope | 32.2710 bytes/s | 1,024 bytes/s |
| Full-window minimum / maximum | 7,045,032 / 7,866,504 bytes | Reported |
| Full-window range | 821,472 bytes | 1,572,864 bytes |

The mean-window difference passes by only **4,282 bytes**; that margin is
retained explicitly. The six consecutive one-minute means are 7,374,188,
7,610,705, 7,483,673, 7,646,602, 7,453,571 and 7,632,050 bytes. They rise and fall
within the reported range, and the final three-minute trend is close to flat,
rather than continuing the initial experiment's approximately 7 KiB/s slope.
This supports bounded oscillation under this measured workload, not exactly zero
retained allocation or an unlimited-lifetime assertion.

All 16 physical backend IDs are unchanged between prewarm and measurement end.
Each has 266–286 prewarm auth calls and 420–477 measured calls, so every
connection turns over at least three 128-row batches during the measurement.
The actual `baseRows` structure is 264 bytes: the 16 ×128 current slots bound
these structs to 2,048 objects / 540,672 structure bytes. Allocator size classes,
wrappers and fixed connection/runtime caches are additional memory; this figure
is not a total heap bound.

[Profiles and differential text](../benchmarks/results/sse-steady-state-20260911T191244.281144193Z)
cover warmup, rounds 6, 12, 36, 60 and final. From round 36 to final, sampled
`getRows` retention decreases by approximately 512 KiB; positive sampled bufio
reader retention is offset by released bufio writers and rows. The initial
row-retention growth therefore does not persist across the warmed batches in
this sample. Heap pprof uses statistical sampling, so its estimated object counts
and positive/negative differences are not substituted for exact MemStats.
Original raw reports remain unchanged, and each experiment now includes
SHA256-matching source snapshots to preserve its tested working-tree version.
After this run, an additional oracle unit regression made backend-ID equality
mandatory rather than checking only the pool/count bounds. The independent audit
confirms the recorded run already has the same 16 IDs; its raw report and frozen
source snapshot are preserved. Both heap oracle tests pass `-race`, and benchmark
`go vet` passes after that harness-only strengthening.

This closes the local repeated-churn acceptance for the fixed six-minute
window. It remains separate from E15's actual slow-client transport termination
and E10's exact 200-subscription workload. API, clients and publisher share a Go
process; PostgreSQL is separate and the host is shared. There are no models,
effects, containers, retention deletes or outages in this experiment. Longer
lifetimes and different traffic/deployment combinations are not established.

## E17 — Cross-store orphan retention, 2026-09-11

The local collector holds an exclusive filesystem publication lock while reading
all PostgreSQL references and UUID-bound SQLite journal pins. Worker and runner
publishers hold the shared lock across durable bytes and authoritative reference
publication. Wrong/missing/recreated journals fail closed; a new SQLite file at
the same path cannot impersonate the prior journal. Directory fsync covers the
object and newly created ancestor entries. See [the protocol](object-retention.md).

The [retained race log](../benchmarks/results/artifact-retention-20260911/race.log)
uses actual private-schema PostgreSQL and WAL SQLite fixtures. It injects an
event-write failure after artifact metadata insertion, checks metadata and event
sequence rollback, then safely collects the unreferenced object. It also tests
both GC/publication orderings, old-key deduplication, runner-only references,
same-path journal replacement, actual publisher process death, staging ages,
batch limits and dry runs. Filesystem/SQLite tests and the real PG cases ran
under `-race`.

The [actual operator report](../benchmarks/results/artifact-retention-20260911/report.json)
records 101 READY keys and 159 total authoritative keys whose hash/size matched
before and after collection. Exactly one deliberately aged synthetic orphan was
selected by dry run and deleted by the seven-day policy. This is a local
retention/correctness result, not distributed object-store or object-retirement
semantics. Interrupted import recovery is separately exercised in E18; corrupt
partial files and legacy orphan leases without a source hash remain preserved.

## E18 — Real runner process fault matrix, 2026-09-11

All 11 [actual process cases](runner-fault-evidence.md) passed in 33.93 seconds
using the original journal, fixed ext4 pool and real rootless Docker. Evidence
includes daemon create/start counts, same-operation receipts, durable artifact
pins, three import-crash windows, response timeout, and old/new writer timestamps
across epoch adoption. Five successful write cases each executed once. Every
test slot was stopped, sealed and released; all four services were restored and
the API readiness endpoint returned ready.

The record retains the initial user-namespace startup failure, the discovered
created-container cancellation failure, its correction and the explicit recovery
of that exact original operation before the successful full rerun. Runner-only
reports explicitly mark PG effect/capacity/fee ledgers N/A. Application-level
worker/runner fault acceptance remains a separate requirement.

## E19 — Additive upgrades and independent collector, 2026-09-11

The [operational record](recovery-evidence.md) links actual PostgreSQL v6→v8 and
SQLite v3→v4 upgrade results. Existing unknown state, immutable request/receipt
bytes, and legacy audit gaps stay explicit. The SQLite migration fixture uses a
TestBackend and is not container-isolation evidence.

A distinct ordinary-user process extracted from the official digest-pinned
OpenTelemetry Collector image received and persisted four correlated spans from
the production OTLP exporter. The collector file proves API→run→model/effect
topology and excludes the fixture's sensitive-data canaries. No model or command
was dispatched for this telemetry test. Paired database/journal/object/workspace
restore has a reviewed executable harness and new source images; its first host
mount still awaits the operator, so full paired recovery is not yet claimed.

## E21 — Real application process faults, 2026-09-11

The [application record](application-fault-evidence.md) completes the combined
F05/F07/F08 paths in 39.72 seconds. Each uses actual typed RPC, Docker, SQLite,
PostgreSQL, two worker processes and a dedicated restricted worker login. Original
operation identity survives timeout/crash/lease replacement; the first worker
dies by SIGKILL, and the second claims only after natural database lease expiry.
All three repairs finish trusted verification, preserve four synthetic model
attempts and four settlements, release capacity and publish a snapshot before
releasing their workspace. Independent recomputation checks 81 READY artifacts,
all quota/capacity totals, original operation receipts and daemon histories.
The two setup failures remain linked. Model-stream, approval, business cancel,
database and combined API/runner outage cases retain their own open rows.

## E22 — Fixed evaluation corpus validation, 2026-09-11

The [eval-v2 report](../benchmarks/results/evaluation-oracle-v2-20260911/report.json)
records 16 actual isolated container checks across two Python and two Go tasks.
Every defective source fails an executed target assertion while its regression
suite passes; every oracle passes both suites. The initial Go corpus exhausted
the production 64 MiB scratch mount during compilation. Its failed evidence and
original corpus manifest remain retained; v2 removes incidental formatting
dependencies and keeps the same production resource limits. A plain exit 1 from
compilation is now rejected as infrastructure failure, not a successful red test.
This is held out from the demonstration fixtures only, without claims about
model training contamination. No native model has run on this corpus yet.

## E23 — Continuation delivery checks, 2026-09-11

The [retained local report](../benchmarks/results/delivery-validation-20260911/report.json)
and raw logs record successful build, ordinary tests, race detection, vet and
OpenAPI/SQLC/protobuf regeneration. Ordinary and race suites each contain 296
passing test/subtest entries, 19 passing packages and 16 explicit opt-in skips,
using Go 1.26.8, two Go scheduler threads and package parallelism two. Database
tests run in separate schemas on the existing loopback PostgreSQL fixture.
The real load/process/collector results above run separately and are not inferred
from skipped tests. Python checks cover 14 volume-helper, 8 runner-helper,
10 recovery and 13 evaluation tests. This working-tree record is anchored at
`a4541a1`; it is not relabeled as a test of an as-yet-uncreated commit.

## Updating this record

Last committed delivery fields: `tested_commit: b924f1c`;
`final_tree_validation: pass`; `clean_clone_reproduction: pass (bounded scope above)`;
`github_actions_execution: success (34206855353)`; `native_provider_evaluation: pending`.
The 2026-09-11 local working-tree continuation is recorded separately in E23;
its new GitHub Actions and clean-clone checks remain pending.
The earlier generator command reported by the integration agent and retained
raw workload reports are listed separately above.

Attach final test/race/generator logs for the final working tree and its commit
identity, plus clean-clone results, without relabeling older logs. Real
execution, HTTP, SSE, finite churn and simulated terminal results above
remain separate workload classes. Add any future native model evaluation with its exact configuration,
budget and independent verification artifacts. Promoting a map row requires evidence
matching its full scope; a measured scheduling result or passing protocol
fixture must not be reused as proof of unrelated container, load or billing
behavior. No fictional employer, production incident, user count or model
performance belongs in the project narrative.
