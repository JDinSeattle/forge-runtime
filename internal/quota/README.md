# Shared model quota and retry scheduling

This module uses a trusted PostgreSQL worker connection. Every mutation locks
the credential-group row before a reservation row, and admission checks use a
fresh database clock read after obtaining the lock. Different tenants, worker
instances, and connection pools therefore consume the same request/token/cost
capacity. Do not expose the service through a tenant API connection: sweeps and
provider configuration need visibility across all reservations for the group.

## Attempt protocol

1. Persist a model attempt and its pricing version. Reserve conservative input
   plus maximum output tokens and maximum cost, using the attempt ID as the
   reservation ID. Repeating identical reservation input returns the existing
   record; changing any bound input conflicts.
2. Commit `MarkDispatched` before opening the HTTP request. A duplicate marker
   returns `ErrAlreadyDispatched`; it is evidence for reconciliation, never
   permission to resend the same attempt.
3. On definitive complete usage, call `Settle` with measured total tokens and
   cost. Repeated identical settlement is a no-op; changing settled actuals
   conflicts. Actual overages are recorded and stop further admission as needed.
4. If any usage or cost is missing, call `MarkUnknown`. No token/cost reservation
   is refunded. Expiring the request deadline releases only its concurrency slot.
5. The only release without usage evidence is `AbandonBeforeDispatch`, which
   verifies no durable dispatch marker exists. This closes a request known never
   to have been sent.

Token and monetary capacities are configured per quota window. On rotation,
settled consumption counters reset, while outstanding conservative reservations
remain. Late actual usage is charged against the current window, which may
overcount that window but cannot silently discard the previously unknown cost.
The original reservation generation is retained for audit. These controls do
not infer or replace the provider's actual configured limits.

## Shared circuit and retries

`RecordOutcome` is independently idempotent per attempt. Transient failures count
toward the group circuit; permanent invalid input and caller cancellation use a
neutral outcome. After cooldown, exactly one reservation becomes the half-open
probe. Its success heals the circuit. Expiry recovers a vanished probe even if
the process settled usage before crashing prior to recording the outcome.

`RetryPolicy.Next` returns a `NotBefore` timestamp, never a blocking sleep. It
uses bounded exponential backoff, caller-provided full jitter, `Retry-After`,
attempt limits, and a total time budget/deadline. Persist the decision once and
release the worker's execution lease while waiting; retries require fresh
attempt identities and reservations. SDK retries remain disabled in provider.

Opt-in PostgreSQL tests (`FORGE_TEST_DATABASE_URL`, loopback `/forge` only) verify
shared admission with independent pools and tenants, competing requests, exact
settlement under concurrent replay, unknown billing across expiry/window changes,
and the half-open crash window. Fixtures are scoped by unique IDs and cleaned
without truncating shared tables. No paid provider calls are involved.
