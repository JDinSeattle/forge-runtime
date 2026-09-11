# API process shutdown evidence (E45, partial S16.4)

The API now registers its SSE shutdown callback with `http.Server.Shutdown`.
The Go 1.26.8 server closes its listeners before invoking this callback. The
callback cancels the event stream manager, while ordinary accepted HTTP requests
retain their contexts and may finish within the existing ten-second drain bound.
A process signal does not issue a business cancellation.

## Actual process exercise

[Final raw evidence](../benchmarks/results/api-lifecycle-20260911T232421Z/results.json)
retains a freshly built race-instrumented API and review test binary identity,
source hashes before and after execution, command logs and a structured
[process report](../benchmarks/results/api-lifecycle-20260911T232421Z/api-lifecycle.json).
The binaries remain in the ignored local directory recorded by `identity.json`;
their bytes are not committed. The process test command passed in 3.722 seconds,
including race-runtime exit overhead; this is not a production latency SLO.
The affected-package vet check also passed.

The test performs these real operations:

1. Creates and migrates a unique review PostgreSQL schema, then starts an actual
   API subprocess on its own loopback TCP port. API queries use the helper's
   nonowner, non-superuser, RLS-constrained role through startup role selection.
   The connection login is still the review administrator; this does not test
   authentication as a separate LOGIN role.
2. Opens an authenticated SSE connection and reads its first complete event.
   Locks a project row in another transaction, submits a new run, and observes
   that API submission waiting on `FOR SHARE` in `pg_stat_activity`.
3. Sends SIGTERM to that exact fixture PID. New TCP connection attempts fail,
   SSE finishes with clean EOF, and the accepted submission remains pending
   while its database row lock is held. The API has not exited at that point.
4. Releases the row lock. The accepted request returns 202 with a new run ID,
   and the process exits successfully. A successor API process replays the same
   idempotency key and returns 202 with the same ID and `reused=true`.
5. Confirms exactly two project runs—the original SSE fixture and new
   submission—remain queued with epoch zero and no stop target. The successor
   exits successfully on SIGTERM; cleanup removes the private schema and role.

The recorded listener/EOF timestamps are observation times. Their order alone
does not prove atomic ordering; the server shutdown implementation supplies the
listener-before-callback guarantee.

## Harness correction and limitations

The [first attempt](../benchmarks/results/api-lifecycle-20260911T232045Z/results.json)
failed compilation because of a test import name collision and an incorrectly
qualified status constant. After those fixture corrections, the
[first process run](../benchmarks/results/api-lifecycle-20260911T232120Z/results.json)
passed. Independent review accepted the production behavior and raw result,
but identified a cleanup gap: Go's timeout panic can bypass `t.Cleanup`.

The [launcher](../tests/review/run_api_lifecycle.py) now starts each invocation
in a dedicated process group, bounds its wait, and cleans only that group on
success, failure or timeout. This catches orphan descendants even when the
test exits before its cleanup callbacks. A separate
[process-group exercise](../benchmarks/results/api-lifecycle-e45-cleanup/report.json)
checks all three paths with a child that deliberately ignores SIGTERM; no child
remained executing. The final API process test above includes this corrected
launcher. Earlier raw evidence and binaries remain preserved.

This is API lifecycle evidence only. No worker, runner, external operation,
Docker job, live service, workspace mount, or paid model was started or changed.
The full S16.4 acceptance remains incomplete until real worker/runner shutdown,
in-flight external-operation preservation and successor reconciliation are
exercised together. This evidence does not certify a future integrated commit
or live deployment.
