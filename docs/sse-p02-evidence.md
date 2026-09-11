# E20 — Exact SSE workload, actual slow-client recovery and missing hints

This record covers the exact P02 traffic shape and the related F13 consumer
failure cases. Its first full run failed a real observability assertion. That
failure, the isolated regression and the subsequent fix are retained separately;
passing data delivery does not turn the failed original report into a pass.

## Fixed workload and acceptance

[`TestSSEP02IsolationEvidence`](../benchmarks/sse_p02_test.go) creates a private
PostgreSQL schema with 20 simulated active runs and a non-owner, NOBYPASSRLS API
role. Exactly 200 TCP/HTTP1 SSE connections start before publication: nine
healthy readers and one deliberately non-reading client per run. Each run
commits five events per second, with exactly 1,024 bytes of stored JSONB payload.
The server uses its production 100ms poll interval, 128-event subscriber queue,
512-event history, five-second write deadline and unmodified TCP socket buffers.
Healthy client socket buffers are also unchanged. Only slow clients request a
4,096-byte receive buffer before connecting; observed kernel values are in the
reports. This is a documented client condition, distinct from E10's original
all-default-client baseline and E15's heavier one-run experiments.

The experiment publishes until all 20 slow handlers and server TCP connections
actually close, then commits at least 50 further events per run. It never reads
the slow response bodies or cancels their requests to manufacture that closure.
All 180 healthy readers must receive every event through the final durable
cursor, within ten seconds after publication, with p95 latency below one second.
Every scheduled event is retained; per-run publication boundaries are exactly
200ms apart. Maximum scheduling lateness must stay below one second and actual
publication duration must not exceed the scheduled duration by more than one
second. The maximum publication window is 25 minutes.

After observing all slow TCP closes, the harness drains their bodies and
reconnects those 20 clients with normal receive windows and the original
`Last-Event-ID`. Each must replay all missed events through the final cursor
within 30 seconds. These replay records are separate from the healthy live
latency distribution. The required result is complete final-cursor delivery,
not merely delivery observed before a time window ended.

Production fanout currently uses durable polling without a LISTEN consumer;
`AppendWorkerEvent` emits no wake hint. The fixture makes absent hints observable
without pretending a production notification optimization exists. A private
PostgreSQL channel first commits 200 unique hints and a real listener must receive
them. Every later event commits normally, after which its optional hint is
issued in a separate transaction that is deliberately rolled back. None of
those hints may arrive, while normal event delivery and eventual replay must
remain complete. No shared `forge_wake` channel or service setting is changed.

## Preserved failed run and root cause

The [original report](../benchmarks/results/sse-p02-isolation-20260911T193300.591537223Z/report.json)
records **FAIL** after 311.390 seconds test duration. It contains 1,551 ticks per
run over 310.214798 seconds, 31,020 durable 1-KiB events, 279,180 complete healthy
deliveries and 31,020 replayed events. All 20 slow TCP connections and handlers
closed, followed by 50 further events/run. Healthy p95 was 80.125676ms, maximum
scheduling lateness 103.781924ms, and all final cursors and cleanup checks passed.
The single failed predicate was the slow-subscriber metric: **1 instead of 20**.

The [independent audit](../benchmarks/results/sse-p02-isolation-20260911T193300.591537223Z/independent-audit.json)
checks every compressed publication, healthy delivery and replay record, as well
as the notification counts and source snapshots. It remains explicitly failed.
The directory also retains the execution stdout, binary identity, source
provenance, original source snapshots and a
[root-cause record](../benchmarks/results/sse-p02-isolation-20260911T193300.591537223Z/root-cause.json).

The pinned chi response wrapper implements legacy `Flush()` and discards the
underlying `FlushError`. `http.ResponseController` selects that method before
following `Unwrap`, so it can return nil after a socket write timeout has
cancelled the request. The SSE handler may then keep its default `client_closed`
reason instead of observing the timeout and recording `slow_subscriber`.
The [regression before the fix](../benchmarks/results/sse-p02-isolation-20260911T193300.591537223Z/flush-regression-before.log)
directly reproduces `flush timeout swallowed: <nil>`.

[`internal/telemetry/http.go`](../internal/telemetry/http.go) now exposes an
error-returning flush while retaining chi's status/byte accounting and original
Flusher, Hijacker, ReaderFrom, Pusher and Unwrap behavior. A plain ResponseWriter
does not acquire an unsupported Flusher interface. The same error-propagation
regression, ordinary-writer checks and capability tests pass with the fix;
[the full telemetry race run](../benchmarks/results/sse-p02-isolation-20260911T193300.591537223Z/flush-regression-after.log)
passes in 1.099 seconds. The benchmark's only change is adding the fixed telemetry
source to its provenance manifest; traffic parameters and acceptance thresholds
remain unchanged.

## Same-parameter repeat

The [fixed-version report](../benchmarks/results/sse-p02-isolation-20260911T194432.961963009Z/report.json)
records **PASS** in 311.190 seconds, with a 310.217267-second publication interval.
The [independent full raw-data audit](../benchmarks/results/sse-p02-isolation-20260911T194432.961963009Z/independent-audit.json)
also passes. It checks every publication, live delivery and replay entry, exact
per-client/per-run sequence and tick order, shared event creation timestamps,
latency quantiles, the absolute 200ms schedule, final cursor equality and all
nine frozen source hashes against execution provenance.

| Fixed-version observation | Actual result |
| --- | ---: |
| Per-run events / durable total | 1,551 / 31,020 |
| Healthy clients at exact final cursor | 180 / 180 |
| Complete unique healthy deliveries | 279,180 |
| Healthy live latency p50 / p95 / p99 | 83.355038 / 92.813347 / 94.436406 ms |
| Maximum observed live latency | 255.937496 ms |
| Maximum publication scheduling lateness | 135.695807 ms |
| Actual average events/run/second | 4.999722 |
| Slow handlers / TCP connections / slow metrics | 20 / 20 / 20 |
| Further committed events after all slow closes | 50/run; 1,000 total |
| Reconnected slow clients / complete replayed events | 20 / 31,020 |
| Reconnect and complete replay duration | 0.116061 seconds |
| Positive hints committed / observed | 200 / 200 |
| Later hint transactions rolled back / hints observed | 30,820 / 0 |
| Durable sequence gaps / final TCP / final handlers | 0 / 0 / 0 |

All slow TCP connections closed by the tick-1,501 observation, approximately
300.316 seconds after publication began. The 128-event subscriber queues did
not overflow; the production write deadline performed isolation in this traffic
shape. Only after those actual closes were observed did the harness drain the
20 old bodies, each returning 1,787,667–1,787,670 bytes and `unexpected EOF`.
Their subsequent subscriptions used the original cursor 2 and each received
every event through sequence 1,553. The 1,551-event replay exceeds the hub's
512-event history and exercises SQL pagination across the retained history.
Healthy latency statistics exclude this intentional delayed replay.

All observed server socket samples retain initial send/receive buffer values
2,626,560 / 131,072 bytes; the 180 initial healthy clients and 20 normally reading
reconnects have those same values. Each intentionally slow client has the
requested receive window represented by an actual 8,192-byte Linux buffer.
There are 200 initial connections and 20 subsequent reconnects. Server defaults
were not reduced to force backpressure, and the earlier all-default-client
baseline remains separately identified.

The [result directory](../benchmarks/results/sse-p02-isolation-20260911T194432.961963009Z)
retains compressed JSONL for all three raw sample classes, the report, stdout,
final heap/goroutine profiles, binary SHA256/build ID and exact source snapshots.
All nine recorded source files were last modified before binary creation. The only benchmark change from
the failed run adds `internal/telemetry/http.go` to the provenance manifest;
the corrected HTTP wrapper is the sole behavior change. Ordinary/race repository
validation ran elsewhere on the same host during the repeat, so the two latency
samples are not a controlled performance comparison of the wrapper versions.

This verifies P02's exact finite workload and F13's slow-consumer isolation,
durable-cursor recovery and absent-optional-hint behavior. The original failure
stays failed, and neither result is described as model execution or unlimited
service-lifetime evidence.

Reproduction commands and the predeclared protocol are in
[benchmarks/SSE.md](../benchmarks/SSE.md). `python3 benchmarks/audit_sse_p02.py <result-directory>` recomputes the
raw-data audit without database access or report changes.

These are finite local process-level measurements with synthetic event-producing
runs. They do not execute models, tools, containers or paid requests. API,
clients and publishers share a process and the host can run other validation
work. Concurrent retention deletion and PostgreSQL outage behavior remain
separate acceptance boundaries; the heap-plateau experiment is E16.
