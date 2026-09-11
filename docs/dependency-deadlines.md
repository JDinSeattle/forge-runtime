# Dependency operation deadlines

Database and artifact operations now have distinct default budgets. They inherit
any earlier caller deadline and do not replace the lifetime of a worker, model
request, runner operation or SSE connection.

| Boundary | Budget and lifetime |
| --- | --- |
| Store / quota operation | 3 seconds for pool acquisition, SQL and consuming results |
| SQL transaction | Same absolute deadline established at BeginTx; later calls cannot extend it |
| SQL rollback | Independent 1-second cleanup context, even if the query/client was canceled |
| Artifact Put / Stat / Delete | 10 seconds, with any earlier caller deadline retained |
| Artifact Open | 10 seconds including validation and consuming its returned reader; Close cancels the timer |
| Publication lock acquisition | 10 seconds; this does not apply a new deadline to a callback performing runner work |
| Object collection | 10 seconds per bounded collection call; PG and SQLite reference reads each have a 3-second budget |
| Native model call / runner RPC | Existing attempt deadline and runner RPC/reconciliation timeouts remain in force |
| SSE connection | Original request lifetime; each individual durable Store read gets its own SQL budget |

`internal/dependency` contains the shared defaults and transaction wrapper.
Production Store and quota methods cancel their operation contexts on return.
Rows and deferred QueryRow scans retain their child context until consumption
finishes. Commit uses the original deadline. Rollback uses a short detached
cleanup context; a failed pgx rollback closes the unusable connection. No timer
goroutine races a query by calling Rollback concurrently. Callers must still
defer Rollback and finish their SQL transaction before external I/O.

The `pgx.Tx` interface exposes raw `Conn` and `LargeObjects` escape hatches. The
application does not use them; this wrapper does not promise to intercept SQL
that future callers deliberately issue through those raw APIs. Direct diagnostic
and test pool calls similarly require their own context. The production Driver's
remaining raw database-clock query now goes through `Store.DatabaseTime`.

Artifact contexts are passed to generic stores at the Driver and API boundaries;
stores must honor the context while opening or writing. Local Open closes its
file on cancellation and propagates that deadline through reads. Cancellation
and normal Close share a `sync.Once`; a file is closed once even if those paths
race. Successful Put does not close its caller-owned input. On cancellation,
closable input is closed to unblock its Read, and Put joins the close callback
before returning. This callback join occurs after the publication lock is released;
staging writes and cleanup have already finished when publication unlocks.

An arbitrary nonclosable `io.Reader` cannot be forcibly interrupted without
inventing an unsafe goroutine lifecycle. Such readers must return from Read
promptly or implement their own cancellation. The same limitation applies to
uninterruptible kernel filesystem calls; context deadlines are not a kernel
storage watchdog. The local implementation checks context between operations and
closes the supported file/pipe readers on cancellation. It removes staging files
when an interrupted Put returns. A late durable unreferenced object is still
handled by the existing collector policy.

New regression coverage includes:

- Blocking `io.Pipe` input, cancellation during a blocked read, concurrent reader
  Close, staging cleanup and publication lock reacquisition.
- Earlier caller deadlines and separation from the parent worker/request context.
- Private-PG table-lock reads and exhausted worker pools reaching the default
  deadline, shorter quota lock waits, and transaction deadline/rollback cleanup.
- An actual TCP SSE connection delivering a newly committed event after the
  3-second database budget has passed.
- A Driver suspended inside a model or recording runner call for longer than
  3 seconds. Independent NOWAIT locks on run, tenant and quota rows, plus an idle
  application pool, check that SQL transactions are not held across the wait;
  the repair then continues normally.

The dependency and artifact package tests passed locally under `-race`, and the
affected packages compile and pass vet. All six new PostgreSQL/TCP/Driver tests
now pass under `-race` on the disposable loopback `/forge` fixture; the
[actual output](../benchmarks/results/dependency-deadlines-20260911/integration-repeat-pass.log)
is retained. Two external-wait subcases also pass. Database default bounds and
quota/transaction cancellation recover usable pools, and real SSE delivers an
event after its connection has exceeded the independent SQL budget.

The [initial failed output](../benchmarks/results/dependency-deadlines-20260911/initial-integration-failure.log)
and original test sources remain preserved. Two immediate pool-count assertions
raced pgx/puddle's asynchronous destruction of canceled connections. The bounded
one-second final-zero check observes completion in 3.154/3.576 ms. The runner
subcase had deliberately paused a signed proof for 3.15 seconds with a three-second
fixture lease, so rejection was correct; it now uses the production 30-second
lease while preserving the three-second SQL budget. No production authorization
or timeout was weakened to fix these tests. An independent read-only review
found no reachable P1/P2 in the deadline changes and checked these test corrections.

API errors now distinguish `503/dependency_timeout` from
`503/dependency_unavailable`. A real TCP endpoint which drops the database
handshake exercises the production pgx readiness path, and six classification
cases retain permanent SQL/auth configuration errors as `500/internal` without
exposing private diagnostics. [Actual race output](../benchmarks/results/api-dependency-errors-20260911/execution.log).
The original E27 outage ran older code and still records its actual 500 results.
`retryable` describes availability, not proof that a mutation failed to commit:
submission retries retain the same key, while other mutations require state and
version reconciliation. OpenAPI documents that contract. Independent review
found no P1/P2 in this classifier or the reserved priority fields.
