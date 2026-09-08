# Measured scheduler experiment

This opt-in test measures 1,000 real PostgreSQL submissions and 100 idempotent
replays using eight submitter goroutines, followed by 1,000 claims made by two
competing scheduler goroutines. Ten tenants and one synthetic runner registration
use the real persistence methods, SQL transactions, reducer, event log, snapshots,
capacity accounting, and pgx pool. It creates a private schema in the explicitly
selected loopback `/forge` database and drops that schema on completion.

```sh
FORGE_RUN_BENCHMARK=1 \
FORGE_TEST_DATABASE_URL='postgres://forge_admin:forge_dev_only@127.0.0.1:REPLACE_PORT/forge?sslmode=disable' \
GOCACHE=/tmp/forge-runtime-gocache \
go test ./benchmarks -run '^TestSchedulerCompetitionEvidence$' -count=1 -v
```

Use the port from your own `docker compose port postgres 5432`. The benchmark is
skipped unless explicitly enabled. It never calls a model, starts a container,
or dispatches an effect. It is safe to repeat against the dedicated development
database because fixture schemas are independent; no shared tables are truncated.

The assertions check that replay produces no extra logical run, each submitted
ID is claimed exactly once at epoch 1 by the two workers, each run has contiguous
created/claimed events and corresponding snapshots, and all 1,000 allocations
agree with tenant/runner counters. Runs finish this experiment in `running` and
are then removed with their schema. This is **not** evidence of terminal run
correctness, exactly-once effects, fake-model throughput, or real container repair
performance. Those require different tests.

Every successful invocation writes a timestamped raw JSON report under
`benchmarks/results/local/` (ignored by Git). It includes individual latencies,
wall time and rates for each separate phase, nearest-rank percentiles, machine
and Go details, PostgreSQL version/durability settings, pool configuration/stats,
code hashes, and explicit correctness counts and limitations. Review selected
reports before copying them into versioned `benchmarks/results/` as evidence.
Only report the measured sample, never extrapolate it into a production SLO.

The claim latency distribution includes successful calls. Aggregate claim wall
time also includes unsuccessful `SKIP LOCKED`/capacity polls and a 1 ms retry
pause. The raw report counts those polls separately. No HTTP endpoint is called,
so the submission rate is a persistence-method measurement, not API QPS.

## Recorded sample

The [2026-09-08 raw report](results/scheduler-20260908T074355.805892677Z.json)
contains a single non-race run on Linux amd64, Go 1.26.8, Intel i9-13900K
(32 logical CPUs), PostgreSQL 17.11 with `fsync=on`, `synchronous_commit=on`,
and pgx pool max 16/min 1. Other development work shared the machine.

| Phase | Samples | Measured rate | p50 | p95 | p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Submit + idempotent replay | 1,100 | 2,106.4 requests/s | 3.685 ms | 5.653 ms | 8.543 ms |
| Successful claims | 1,000 | 128.9 claims/s | 6.477 ms | 10.758 ms | 15.969 ms |

Workers claimed 482 and 518 distinct runs. All 1,000 runs were persisted and
claimed at epoch 1, with 2,000 contiguous events and 2,000 snapshots. The
allocation, tenant-active, and runner-slot totals all equaled 1,000. There were
zero model-attempt and effect rows, consistent with the restricted experiment.

The same sample had 795 capacity-or-runner-lock polls and 1,717 empty polls.
With one runner registration, concurrent `FOR UPDATE SKIP LOCKED` operations
can temporarily hide its row; current scheduling postpones the candidate.
These counts make contention visible and explain why adding submit concurrency
does not imply similar claim throughput. They are a useful starting point for
profiling or comparing a changed scheduler, not grounds to extrapolate scale.

## Terminal simulation with an effect ledger

`TestTerminalSimulationEvidence` extends the scheduling workload through a
terminal state. It uses a deliberately finite controller in the benchmark,
calling the actual PostgreSQL Store, quota and model-attempt APIs. Two worker
goroutines compete for tasks across ten tenants. Each run performs two real
`FakeProvider` stream assemblies, one simulated read effect and synthetic
regression verification before reaching `completed`. The fixture labels its
verification `regression_only`, not real code verification.

```sh
FORGE_RUN_TERMINAL_BENCHMARK=1 FORGE_TERMINAL_RUNS=1000 \
FORGE_TEST_DATABASE_URL='postgres://forge_admin:forge_dev_only@127.0.0.1:REPLACE_PORT/forge?sslmode=disable' \
GOCACHE=/tmp/forge-runtime-gocache GOPROXY=off \
go test ./benchmarks -run '^TestTerminalSimulationEvidence$' -count=1 -v -timeout=12m
```

Use `FORGE_TERMINAL_RUNS=10` for a smaller harness smoke check. The report always
records the actual count; a smoke result cannot satisfy the 1,000-task target.
The test uses its own schema and temporary local artifact directory. It starts
no container, command, network model request or public HTTP server.

Every effect is submitted to the in-memory fake executor twice. The fake's
idempotency map returns its original receipt and records one simulated
execution. After the first receipt is committed, the old-version confirmation
is submitted again to the real Store and must be rejected. Final assertions
join every persisted effect to the fake's operation/hash/receipt and to its
ready artifact. This checks the control-plane contract against an explicitly
idempotent fixture; it does not prove production runner execution idempotency.

Every model turn has a real persisted attempt, shared quota reservation,
dispatch marker, final usage and settlement. A repeated settlement must not
double the synthetic charge. The final checks require all runs completed,
one `run.finished` per run, one confirmed effect per unique simulated ID,
continuous event sequences, no missing ready effect receipt, all allocations
released, zero tenant/runner active slots, zero provider active/reserved
resources, and exactly the expected committed synthetic token/cost totals.
The committed consumption should remain nonzero; only outstanding reservations
and concurrency return to zero.

Successful runs write `results/local/terminal-simulation-<timestamp>.json`,
including per-run terminal state, version, event boundary, effect receipt,
duplicate confirmation/control results, measured duration, aggregate database
invariants and environment/source details. Rates describe this finite direct
Store controller. They do not describe `application.Driver`, native model
latency, process restart, real tools or code-repair throughput.
