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
