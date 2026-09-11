# E43 — production trace and metric boundaries

This acceptance extends S16.1, S16.2 and S16.3 on isolated base commit
`5de6f0f`. It changes PostgreSQL via additive migration 12 only in disposable
schemas. It does not deploy the live services or migrate the public database.
The parent integrates this isolated worktree with E41/E42/E44 separately.

[E46](integration-checks-e46-20260911.md) subsequently integrates E41/E42 and
repeats the actual Collector chain successfully, retaining its own source and
record identity. It also corrects the runner's default metrics port to 8099 so
it does not conflict with the API's 8097 listener. E44 remains separate.

The instrumented path is a real TCP HTTP API with the existing restricted
NOBYPASSRLS review role, private PostgreSQL schema, production Driver, real gRPC
transport, production Engine, durable SQLite journal, actual artifact bytes,
and an independent official OTel Collector. Model behavior is a deterministic
local Provider and sandbox execution uses explicit **TestBackend**. This proves
protocol, observation, storage and trace propagation. It is not Docker/Python
execution evidence, paid-model quality, process-kill recovery, or journal trace
continuity across runner restart.

## Authoritative latest run

`benchmarks/results/telemetry-chain-20260911-frozen/` retains Collector output,
actual Prometheus scrapes, process/image identity and report. A pre-test manifest hashes 199 source inputs; `_source/` retains modified
source bytes (leading underscore keeps evidence copies out of `go test ./...`).
The report records
actual PG enqueue/claim/attempt traceparents and SQLite operation IDs; the graph
is parsed from Collector file output and bound back to those identities.

The reviewed run completed at state version 24. It observed four real
Provider.Stream invocations (one local typed rate limit and three completed
responses), four successful claims, six new Engine executions, 23 committed
state transitions and 19,404 newly READY bytes. Duplicate submit, approval,
terminal drive and READY publication are exercised. The worker queue gauge is
one after creating an unclaimed terminal-retry child; active allocations are
zero, all inflight gauges are zero, and reserved budget is zero because this
fixture's explicit price is zero. Zero fixture budget is not real billing data.

The collected graph has 109 spans across forge-api (8), forge-worker (73) and
forge-runner (28), including 20 steps, four queue intervals, four verification
phases, six Engine executions, one PrepareWorkspace, three AdoptWorkspace,
six StartOperation and twelve InspectOperation server spans. Client/server
parents cross the transport, and the detached Engine span has its actual
StartOperation server parent. Run retry, model retry and lease takeover links
resolve to real exported predecessor contexts. Saved-model replay and six
receipt replays are explicit events. Main trace contains no task/baggage canary;
metrics contain neither canary nor run ID.

This success path does not call StopWorkspace, CancelOperation, SealSnapshot
or ReleaseWorkspace. Their production interceptors use the same finite method
mapping and existing runnerclient regression suite; this Collector run does
not claim separately observed executions of those RPC methods.

The modeled interruption cancels the **Drive context** after the model result
has committed and before reducer advance. It waits for real lease expiry,
claims a new epoch, adopts and replays the saved result. It does not kill the
OS process. Original SQLite execution contexts are intentionally not persisted;
recovery of an operation not live in the process emits context_missing instead
of fabricating a previous operation link.

## Checks and review corrections

- Actual reviewed Collector run: `go test -race` PASS, package 6.118 s.
- PostgreSQL callback/rollback test PASS 1.297 s in its first explicit PG run;
  this test exercises the real transaction wrapper and explicit transition
  observations. It is not an unknown-job/Docker reconciliation scenario.
- Private PostgreSQL 6→12 migration run twice, original run/effect/artifact/
  snapshot data unchanged, missing telemetry history remains NULL; latest
  grouped check PASS 1.311 s. Historical 6→10/11 evidence stays unchanged.
- Full touched package tests passed under race. The first broad command did not
  populate FORGE_TEST_DATABASE_URL, so persistence integration cases skipped;
  the final complete package run populates it from the private review DSN and
  passes (persistence 12.312 s, application 32.397 s, runner 14.047 s). Vet and
  the sqlc generated-file check also pass. Logs
  distinguish these runs and do not promote skipped cases to evidence.
- OTLP HTTP backpressure: production Setup, blocked local HTTP receiver, full
  nonblocking queue, 4,096 model hooks complete under a two-second bound and
  local metrics remain correct; PASS 2.107 s. This is an exporter boundary test,
  not a business recovery test during Collector loss.
- Retained graph negatives reject missing RPC parent, wrong run/attempt IDs,
  dangling/removed retry links, duplicate span IDs, invented SQLite operation
  and changed PG attempt context. Unit tests cover schema pin, unknown usage,
  finite labels, repeated cleanup and confirmed/ambiguous COMMIT boundaries.

Independent root review found two observation errors before final acceptance:

1. Two `clock_timestamp()` calls in Defer can land in the same microsecond.
   The initial timestamp heuristic wrongly counted an explicit yield as expiry
   after the first Defer (real PG -race FAIL 0.386 s). Migration 12 now stores
   `lease_yielded`: Defer sets true and Claim resets false in their existing
   transactions. After fix, 100 cycles included **87 equal timestamp pairs**
   and zero false expirations, PASS. This flag has no execution authority.
2. Idempotent Submit initially annotated the candidate ID that was never
   inserted. The strengthened audit rejects the retained pre-fix identities
   run with `idempotent enqueue has a fictitious run identity`. Submit now
   annotates the existing run and preserves its original stored traceparent.

Old raw is retained under `telemetry-chain-20260912-first`, `...-second`,
`telemetry-chain-20260911-final`, `...-identities`, `...-reviewed`, and
`telemetry-e43-review-corrections`. The first collector run failed due to an
overlong test Unix socket path; the harness now uses its own short private
`/tmp/e43-rpc-*` directory. The first core integration attempt also exposed a
preexisting SQL `not_before=-infinity` value incompatible with the new Go time
projection; observation reads now map nonfinite timestamps to absent metadata.
The first two directory names say September 12; actual timestamps in their raw
records are September 11. Raw filenames and results have not been rewritten.
The old `passed:true` reports used weaker graph checks; only the reviewed and final frozen runs
support the corrected identity acceptance.

## Reproduce without paid calls or live mounts

From this worktree, use an existing test administrator DSN for loopback /forge.
The helpers create and drop only their own schema and role. The review env file
is local and ignored; do not commit credentials.

```bash
set -a
. /absolute/path/to/private/review-database.env
set +a
export FORGE_TEST_DATABASE_URL="$FORGE_REVIEW_DATABASE_URL"
export GOPROXY=off GOSUMDB=off GOENV=off GOTOOLCHAIN=local TMPDIR=/tmp
# Optional: export GOCACHE to an existing writable shared Go cache.
mkdir -p var/local/build-tmp
export GOTMPDIR="$PWD/var/local/build-tmp"
python3 scripts/telemetry/acceptance.py \
  --docker-host unix:///run/user/1000/forge-runtime-docker.sock \
  --image otel/opentelemetry-collector-contrib@sha256:799dc6cf12c96192af37b5bdba804da8c10b3bc563b43cb90c3f3c58d9572ad6 \
  --output "$PWD/benchmarks/results/telemetry-chain-NEW-UNUSED-NAME"
go test -race ./tests/review -run '^TestReviewTelemetryRetainedGraphRejectsBrokenBindings$' -count=1 -v
go test -race -exec '/usr/bin/env TMPDIR=/tmp' ./internal/telemetry ./internal/persistence \
  -run 'TestTelemetry|TestCollectorBackpressure|TestObservation|TestBoundedPhase|TestPinnedGenAI' -count=1 -v
```

The image must already be available; the helper creates only its own stopped,
network-none extraction container and removes it after copying the official
binary. It starts the Collector as an ordinary-user process with private config
and loopback OTLP HTTP; it never starts a Docker container or changes daemon
networking. Extraction/container/process identities are retained. The official
binary SHA256 is `8524ac54f6e1d4d00d9ba5eea91daadec2ebc31e4da80db9c17eba2e859ecdd4`.
The helper preserves explicit Go cache settings and terminates its own test
process group on timeout. Child output and the Collector's file output remain
available on failure. Field mapping, counter limits, global-gauge aggregation
and migration semantics are documented in `internal/telemetry/README.md`.

After the final runtime run, the harness archival directory was renamed from
`source` to `_source` to keep copied Go files outside package discovery. Source
bytes and the pre-test input hashes remain unchanged. This evidence-path-only
helper change was syntax checked; the recorded trace was produced by its archived
pre-rename helper.
