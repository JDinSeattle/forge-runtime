# Fixed-window SSE fanout experiment

`TestSSEFanoutEvidence` checks a 12-second sample of the implementation-plan load:
20 active runs, 200 simultaneous TCP SSE connections, and 5 persisted events per
second per run. Each PostgreSQL JSONB payload occupies exactly 1,024 bytes when
rendered by `payload::text`; the SSE event wrapper adds its own fields and framing.
This is 1,200 committed source events and 10,800 expected healthy deliveries.

```sh
FORGE_RUN_BENCHMARK=1 \
FORGE_TEST_DATABASE_URL='postgres://forge_admin:forge_dev_only@127.0.0.1:REPLACE_PORT/forge?sslmode=disable' \
GOCACHE=/tmp/forge-runtime-gocache \
go test ./benchmarks -run '^TestSSEFanoutEvidence$' -count=1 -timeout=90s -v
```

Run from the repository root. Use the local port reported by `docker compose
port postgres 5432`. Run this experiment without `-race` for latency evidence and
without other intentional load experiments in parallel. The test is skipped
unless explicitly enabled. It uses `testutil.Database` to create and later drop
a private schema in the explicitly selected loopback `/forge` database.

The API pool runs under a unique PostgreSQL `NOLOGIN NOSUPERUSER NOBYPASSRLS
NOINHERIT` role, selected using `SET ROLE` on each connection. That role owns no
business tables, receives only SELECT and schema USAGE privileges for this
fixture, and must pass the application's `CheckAPIRole`. HTTP requests use a real
issued bearer token and a viewer membership. The privileged fixture connection
creates the schema, identity and simulated runs and persists the events; it does
not serve API reads. The unique role's grants and role are removed on cleanup.
This exercises effective database permissions; it is not a test of separate
login credentials or external secret distribution.

`httptest.NewServer` starts the real HTTP handler on an actual loopback TCP
listener. The clients use HTTP/1.1, the actual SSE endpoint and its production
poll interval, history and subscriber queue defaults. No custom slow writer,
reduced queue, reduced socket buffer, model, container or effect is inserted.
Each of the 20 synthetic runs is claimed with the real persistence methods and
remains active for the experiment; fixture cleanup removes it afterward.

All 200 subscriptions establish before publication starts. Every run has nine
normal readers and one reader that stops reading the response body after headers.
The slow reader's position within its ten-client group comes from a fixed PCG
seed of `20260908`; generated identifiers and operating-system scheduling still
vary. Twenty producers each schedule 60 events at 200 ms intervals, beginning
200 ms after a common start. Each append uses a real committed PostgreSQL
transaction. Every publication records its scheduled time, actual completion,
append duration and scheduling lateness. The report retains the actual elapsed
publication window and event rate, so a delayed publisher cannot masquerade as
a workload that met the target rate.

Healthy clients check the run ID, event type, SSE ID, payload tick and every
consecutive sequence after their initial cursor. The test fails on a duplicate,
gap, publisher error, missing delivery, wrong persisted payload size, or failure
to receive all 60 events within the five-second catch-up grace. It separately
checks that latency p95 is below one second. "No permanent loss" in this result
means that finite committed set was delivered completely to every healthy
reader; this sample does not inject reconnect churn or retention deletion.

Latency is `client receive time - PostgreSQL event created_at`, including commit,
polling and transport. The local database/container and Go process share the host
clock. Quantiles use nearest rank over all 10,800 healthy delivery samples. The
JSON report preserves each created/received timestamp, run index, client index,
sequence and payload tick, as well as all 1,200 publication samples.

Resource samples contain actual Go heap allocation/in-use bytes, goroutine
counts and Linux `/proc/self/fd` counts before subscriptions, after all 200 are
ready, every second during publication, after catch-up and after connections
close. Both database pools are warmed to their configured maximum before the
first reading. The before/after readings use a completed GC and retain the same
idle listener and pools. API, publishers and client readers share one process;
these figures include the harness and retained raw measurement samples. They
are not service-only memory or a proof of leak-free long-term operation.

The report records actual early server handler returns for slow clients and hub
queue-overflow callbacks. A slow body reader may receive its entire roughly
60 KiB stream into ordinary TCP buffers during this short sample. Zero slow
disconnects means that transport backpressure was not observed; it does not
establish slow-client disconnection. The independent bounded-queue regression
tests cover queue overflow behavior. Do not present those unit results as an
observed network disconnect in this experiment.

Each completed invocation writes `benchmarks/results/local/sse-<timestamp>.json`
and logs the path. This directory is ignored by Git. The report includes machine
and Go details, PostgreSQL version/durability settings, pool parameters, source
hashes, samples, correctness counts, slow-client observations and limitations.
Review a report before copying it into versioned `benchmarks/results/` as a
recorded sample. Report only its measured values; a single short local sample
does not establish sustained capacity, a production SLO or real-agent throughput.

## Five connection-cancellation cycles

`TestSSEConnectionChurnEvidence` is a separate lifecycle experiment. It opens
and cancels five rounds of 50 real TCP SSE connections. It uses the same private
PostgreSQL schema, restricted role, bearer-token authentication and actual API
handler setup. One complete one-connection warm-up precedes the baseline.

```sh
FORGE_RUN_BENCHMARK=1 \
FORGE_TEST_DATABASE_URL='postgres://forge_admin:forge_dev_only@127.0.0.1:REPLACE_PORT/forge?sslmode=disable' \
GOCACHE=/tmp/forge-runtime-gocache \
go test ./benchmarks -run '^TestSSEConnectionChurnEvidence$' -count=1 -timeout=40s -v
```

Each round requires 50 HTTP responses, 50 actual TCP connections observed by
`http.Server.ConnState` and 50 active handlers before cancellation. After
cancellation it joins every client goroutine, closes idle client connections,
and waits up to two seconds for both TCP connections and active server handlers
to reach zero. Every started handler must have returned. It then allows 50 ms
for process cleanup, runs GC and reads the actual Go heap, goroutine and FD
counts. Goroutines may be at most eight above the pre-warmed baseline and FDs
at most four above it. The report records both the values and these explicit
allowances; zero TCP connections and zero handlers remain exact requirements.

The overall experiment context is 25 seconds. It writes a timestamped directory
under `benchmarks/results/local/churn-<timestamp>/`, containing `report.json`
with every round's open/closed values and final `heap.pprof` and
`goroutine.pprof` profiles. Inspect them with:

```sh
go tool pprof -top benchmarks/results/local/churn-REPLACE_TIMESTAMP/heap.pprof
go tool pprof -top benchmarks/results/local/churn-REPLACE_TIMESTAMP/goroutine.pprof
```

The pools and listener remain open during all samples. Heap includes the
combined API/client harness and is reported without a heap-leak assertion.
This is bounded evidence over 250 connections, not evidence of long-term absence
of leaks. It publishes no events and cannot establish replay correctness,
long-lived backpressure behavior or sustained resource stability.

## Actual TCP backpressure and disconnection

`TestSSEBackpressureEvidence` extends the earlier finite fanout sample with two
independent experiments. One keeps the operating system's default send/receive
buffers. The other requests a 4,096-byte server send buffer and 4,096-byte slow
client receive buffer; reported `getsockopt` values reflect Linux's actual
accounting, including its usual doubling. It does not reduce the production
128-event queue, 512-event history, 100 ms poll interval or five-second write
deadline. No artificial handler sleep or mock ResponseWriter provides the
backpressure.

```sh
FORGE_RUN_SUSTAINED_SSE=1 \
FORGE_TEST_DATABASE_URL='postgres://forge_admin:forge_dev_only@127.0.0.1:REPLACE_PORT/forge?sslmode=disable' \
GOCACHE=/tmp/forge-runtime-gocache \
go test ./benchmarks -run '^TestSSEBackpressureEvidence$' -count=1 -timeout=2m -v
```

Each mode has one synthetic run, four healthy clients and one client that stops
reading its HTTP body after headers. It publishes 1,000 actual PostgreSQL events
at 50 events/s over 20 seconds, with exactly 16 KiB of stored JSONB payload per
event. This deliberately heavier stream fills real transport buffers; it is
separate from the 200-client, 20-run, 1-KiB/5-Hz baseline above. Neither workload
may be presented as if it measured the other's exact configuration.

Acceptance requires all of the following:

- The slow HTTP handler returns and its real TCP connection reaches
  `http.StateClosed` before the harness cancels any clients.
- The production slow-subscriber metric increments. A network write timeout is
  classified as a slow subscriber; it is no longer mislabeled as a client cancel.
- At least 50 additional events are committed after slow TCP closure, while
  every healthy reader receives all 1,000 sequences exactly once and p95 latency
  remains below one second.
- PostgreSQL has all events, consecutive sequences and the exact payload size.
  After final cancellation, no test TCP connection or handler remains active.

Only after server-side closure is observed does the slow reader drain bytes that
were already buffered by the kernel. Its observed EOF/error and drained count are
retained. Client cancellation therefore cannot manufacture a passing disconnect.
Each mode saves its own report, actual socket sizes, raw healthy delivery and
publication samples, resources and final heap/goroutine profiles beneath
`results/local/sse-backpressure-<mode>-<timestamp>/`.

## Sustained publish/open/cancel cycles and heap plateau

`TestSSESustainedChurnEvidence` runs three full-size warm-up cycles, followed by
24 measured cycles of 100 actual TCP SSE connections. Each measured cycle lasts
at least five seconds, publishes ten 1-KiB durable events at 200 ms intervals,
checks every subscriber's consecutive sequences, holds connections open for
about four seconds, cancels and joins all readers, and confirms zero server TCP
connections/handlers before its post-GC measurement. The measured interval is
therefore at least two minutes and contains 2,400 connection lifecycles and
24,000 healthy event deliveries. Warm-up events and connections are reported
separately. The same real PostgreSQL/private-schema/nonowner RLS fixture is used.

```sh
FORGE_RUN_SUSTAINED_SSE=1 \
FORGE_TEST_DATABASE_URL='postgres://forge_admin:forge_dev_only@127.0.0.1:REPLACE_PORT/forge?sslmode=disable' \
GOCACHE=/tmp/forge-runtime-gocache \
go test ./benchmarks -run '^TestSSESustainedChurnEvidence$' -count=1 -timeout=4m -v
```

The warmed listener, database pools and telemetry remain open throughout. Each
cycle retains HeapAlloc, HeapInuse, FDs, goroutines, actual TCP/handler counts and
sequence totals. The harness preallocates result arrays and does not accumulate
per-delivery samples or socket records during the plateau window.

The acceptance thresholds are fixed before running the experiment:

- Every closed cycle has zero test TCP connections and active handlers, paired
  handler starts/returns, at most eight extra goroutines and four extra FDs above
  the warmed baseline.
- The mean post-GC HeapAlloc of the last six samples exceeds the first six by
  no more than 1 MiB.
- The ordinary-least-squares HeapAlloc slope over the final 12 samples is no
  greater than 8 KiB/s.

A pure oracle regression test rejects an explicitly growing synthetic series
and accepts bounded oscillation. A passing real sample constrains the measured
finite workload within these tolerances; it does not prove exactly zero retained
allocations or infinite process lifetime. RSS need not return to baseline because
the Go runtime may retain reusable memory. Both API and clients share this Go
process, so the measurements include the harness and runtime caches.

The timestamped output directory contains raw per-cycle data and heap/goroutine
profiles after warmup, round 6, round 12 and the final cycle. Compare them with:

```sh
go tool pprof -top -base results/local/sse-sustained-churn-TIMESTAMP/round-06-heap.pprof \
  results/local/sse-sustained-churn-TIMESTAMP/final-heap.pprof
```

Run timing experiments sequentially without `-race`; perform race validation
separately when needed. Both tests require explicit opt-in and use only their own
private schemas/roles. Source hashes, the base Git commit and dirty-tree status
are retained so working-tree samples are not mislabeled as pristine commit
measurements. These experiments make no model, effect or container calls and do
not themselves test retention deletion, database outage or a lost-notification
fault. Review raw outputs before copying them into versioned results.

## Six-minute steady-state diagnosis

The initial two-minute result had a positive heap slope close to its bound.
A sampled heap difference attributed the main new retained objects to
`pgx.(*Conn).getRows` called by `Store.Authenticate`. In pinned pgx 5.10.0,
`pgxpool.connResource` initially allocates 64 `poolRow` wrappers, then replaces
exhausted batches with 128 wrappers. Scanned wrappers retain the closed baseRows
until the current batch is replaced; baseRows.Close clears context, SQL, args,
values and scan-plan references. This suggests a bounded batch-warmup effect,
but sampled profiles alone do not establish steady memory.

`TestSSESteadyStateEvidence` therefore instruments only a per-PostgreSQL-backend
count of the fixed Authenticate query. It records no query arguments, tokens or
SQL strings. The real API role and 16-connection pool stay unchanged. Prewarm temporarily acquires all pool connections, releases one at a time and
performs 256 actual Authenticate calls through that one available connection.
This deterministically exercises every physical connection's Pool.QueryRow
batches without private-field access or changing pool limits. Three full-size
HTTP lifecycle cycles then warm the server/client paths. The measured window
starts only when the per-backend counters confirm that all 16 are warmed.

```sh
FORGE_RUN_SUSTAINED_SSE=1 \
FORGE_TEST_DATABASE_URL='postgres://forge_admin:forge_dev_only@127.0.0.1:REPLACE_PORT/forge?sslmode=disable' \
GOCACHE=/tmp/forge-runtime-gocache \
go test ./benchmarks -run '^TestSSESteadyStateEvidence$' -count=1 -timeout=10m -v
```

The measured workload is 72 five-second cycles, 7,200 new actual TCP connections
and 72,000 healthy event deliveries over at least six minutes. Every API
connection must perform at least 256 additional Authenticate queries during this
window, proving at least two full 128-row batch turnovers per connection. The
fixture refreshes its valid five-minute lease through the ordinary Heartbeat
method; it does not bypass the scheduler's duration bounds.

Before execution, the stricter oracle is fixed to:

- At most 256 KiB growth between the first and last 12-sample means.
- At most 1 KiB/s positive OLS slope over the last half of measured samples.
- At most 1.5 MiB between the smallest and largest post-GC HeapAlloc samples.
- The same exact healthy-sequence, TCP/handler cleanup and FD/goroutine bounds as
  the earlier experiment, plus the per-connection prewarm/turnover requirements.

The output directory `results/local/sse-steady-state-TIMESTAMP/` retains every
cycle, per-backend query counts, the actual raw-row structure size, source hashes,
and profiles after warmup and measured rounds 6, 12, 36, 60 and 72. Oracle tests
reject a sustained 2 KiB/s synthetic trend and inadequate batch turnover. The
first two-minute report and its original source snapshots remain unchanged;
this later experiment must be interpreted from its own raw outcome.

## Exact P02 isolation and missing optional hints

`TestSSEP02IsolationEvidence` preserves the exact P02 traffic shape: 20 simulated
active runs, 10 actual TCP subscriptions per run (9 healthy and 1 slow), and
5 durable events/run/second whose stored JSONB text is exactly 1,024 bytes.
All server socket buffers and production hub defaults remain unchanged
(100ms polling, 128-event subscriber queues, 512-event history and five-second
write deadlines). Healthy client sockets are also unchanged. Only the 20
intentionally non-reading clients request a 4,096-byte receive buffer before
connect; the actual kernel values are recorded. This is explicit client
behavior, separate from the earlier all-default client baseline.

```sh
FORGE_RUN_P02_SSE=1 \
GOCACHE=/tmp/forge-runtime-gocache GOPROXY=off \
go test ./benchmarks -run '^TestSSEP02IsolationEvidence$' -count=1 -timeout=28m -v
```

Set `FORGE_TEST_DATABASE_URL` to the disposable local PostgreSQL test service.
Each execution creates its own schema and non-owner RLS API role. It uses no
model credentials, executor, container or live deployment channel. Expected
runtime is roughly 8–15 minutes while normal server TCP buffers fill; the
maximum scheduled publication interval is 25 minutes. The workload stops only
after observing all 20 slow handler returns and actual server TCP closes,
followed by at least 50 additional committed events per run. If that does not
happen before 7,500 ticks/run, the report fails instead of calling a partially
buffered interval successful isolation. There are progress messages every
300 ticks (one minute).

The acceptance conditions are frozen in advance:

- Start with exactly 200 TCP connections/handlers. Observe 20 actual slow TCP
  closes and 20 `slow_subscriber` metrics before harness cancellation, while
  all 180 healthy clients reach every final committed cursor within ten seconds.
- Require exactly 20×ticks durable events, all stored payloads exactly 1 KiB,
  no durable sequence gaps, exactly 180×ticks unique healthy deliveries, and
  healthy p95 database-created-to-client-received latency below one second.
- Schedule every per-run publication at its absolute 200ms boundary; save each
  timestamp and append duration. Maximum scheduling lateness must remain below
  one second, and total publication duration cannot exceed the scheduled window
  by more than one second. No scheduled events are dropped to preserve a rate.
- Drain each slow response body only after all slow server TCP connections have
  closed. Then reconnect those 20 clients with normal receive windows and their
  original `Last-Event-ID`; each must replay all missed events through the final
  cursor within 30 seconds. Replay latency is separate from the live p95 metric.
- End with zero remaining TCP connections/handlers. Save every publication,
  healthy delivery and slow-client replay as compressed JSONL, alongside the
  report, observed socket sizes, source hashes and final profiles. This workload
  retains raw timing evidence and is not a heap-plateau experiment.

Current production `eventstream.Manager` uses durable polling without a LISTEN
consumer, and `AppendWorkerEvent` emits no notification. The experiment does
not invent such an optimization. To explicitly test missing optional hints,
a fixture-only private PostgreSQL channel first commits 200 unique NOTIFY hints;
a real listener must receive all 200. Every later event commits through the
ordinary `AppendWorkerEvent` transaction, then its optional hint is issued in a
separate transaction that is rolled back. The listener must receive none of
those rolled-back hints while healthy delivery and eventual replay remain
complete. Counts of committed, rolled-back and unexpectedly delivered hints are
reported separately. The channel belongs only to this test's private schema;
no shared `forge_wake` channel or production defaults are changed.

This closes only the matching finite traffic, slow-client recovery and absent
hint cases when the actual report passes. Concurrent retention deletion and a
PostgreSQL outage are separate fault boundaries, with separate evidence.
