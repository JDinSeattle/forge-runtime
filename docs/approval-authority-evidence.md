# E32 — Current membership authority at approval commit, 2026-09-11

Four targeted tests, containing five leaf cases, pass under `-race` against real
private PostgreSQL schemas and the production HTTP handler over loopback TCP.
The package elapsed time is **2.910 s**. They reproduce and close the S14.5 gap:
an authenticated developer whose membership changes while the approval request
waits for a database lock can no longer commit using the cached role.

The [original failure log](../benchmarks/results/approval-authority-20260911/before-fix-failure.log),
[original control implementation](../benchmarks/results/approval-authority-20260911/before-fix/control.go.txt),
[final race log](../benchmarks/results/approval-authority-20260911/postgres-http-race.log),
[derived report](../benchmarks/results/approval-authority-20260911/report.json) and
[source/log hashes](../benchmarks/results/approval-authority-20260911/manifest.json)
are retained. The original failure log SHA-256 is
`5f3e37995de9ef6a6eda9e60d15f05f34c9445f81a9b60dc7a1c94170541c22d`.
Relevant source snapshots were copied after execution; this is not a retained
test executable, full build-input attestation or historical database export.

## Reproduced failure and transaction boundary

The fixture first creates a waiting approval. A separate administrator
transaction holds the tenant accounting row. The real HTTP request authenticates
and enters `Store.Decide`, then waits for that row. The test observes that exact
blocker through `pg_stat_activity` and `pg_blocking_pids`; it does not use an
assumed sleep to claim that authentication has completed. Another connection
commits either a role downgrade to `viewer` or membership deletion, and then the
test releases the tenant lock.

Before the fix, **both cases return HTTP 200**, write one approval decision and
one `approval.decided` event, and change `waiting_approval` version 6 to `queued`
version 7. After the fix, both return **403**, retain the original state/version,
and write no decision or decision event. Restoring the membership permits one
approval; repeating the exact request returns the same version and changing
allow to deny conflicts.

`Decide` now takes locks in the order **tenant accounting → current membership
SHARE → run → approval**. It reads the current role after obtaining tenant
ownership, permits only `developer` or `admin`, and keeps the membership lock
through commit. A second test holds the run row so the request pauses after
acquiring its membership lock. A concurrent administrator role update cannot
finish during the bounded 80 ms probe; after the decision commits, that update
can complete and a new request is forbidden. The decision transaction retains
the existing three-second database budget and contains no external I/O.

Existing administrative membership-only updates and `BootstrapTenant` do not
take these locks in the reverse order. Any future administrative transaction
which locks both scheduling rows and membership rows must preserve that order.

## Narrow database authority

PostgreSQL requires UPDATE privilege to acquire a `FOR SHARE` row lock. Migration
10 therefore introduces the single fixed operation
`lock_current_membership(text,text)`. It is a `SECURITY DEFINER` function which:

- Resolves and quotes the memberships table's schema during the owner migration.
- Uses a fixed `pg_catalog, pg_temp` search path and a schema-qualified table.
- Rejects null identifiers and a tenant different from the transaction's
  `forge.tenant_id`; a missing membership returns NULL and `Decide` refuses it.
- Returns only the current role while locking that exact membership row.
- Performs no writes and accepts no SQL or schema parameter.
- Revokes PUBLIC execution and requires one explicit function grant.

The real test role has no INSERT, UPDATE or DELETE authority on memberships,
tokens or tenants. The checks confirm helper metadata, no PUBLIC execution,
rejection without tenant context or with the wrong tenant, NULL for a missing
membership, and that a forged temporary `memberships` table cannot change the
role returned by the helper. Removing the explicit EXECUTE grant makes the call
fail with PostgreSQL `42501`; there is no PUBLIC fallback. Direct membership
writes fail with that same permission error.

The existing review helper connects with administrator credentials and sets a
restricted nonowner `current_user` using `SET ROLE`; it is **not** a separate
restricted LOGIN test. Its production `CheckAPIRole` check passes. These tests
exercise the effective role's actual PostgreSQL permissions, while preserving
that distinction about the connection's `session_user`.

An admin denial case also verifies exact repeats before and after terminal
cancellation, one decision event, a stable terminal version, and released
accounting. Synthetic READY control metadata drives this fixture: no runner,
model, Docker container or filesystem effect is executed. It does not provide
new process-stop or model-quality evidence.

## Deployment and reproduction

**The running services and public schema were not updated by this acceptance.**
New `local-setup` instances grant exactly this function to the generated runtime
roles and retain read-only authentication-table permissions. Existing deployments
need migration 10 and an explicit function grant before serving approvals with
the new binary. Use the trusted migration administrator, not the API credential:

```sh
# FORGE_DATABASE_URL selects the intended deployment using its migration role.
bin/forge-admin migrate
```

Then, in that same database, grant only the function to the actual configured API
role. The following identifiers are placeholders to substitute from the reviewed
deployment configuration; the normal local setup uses schema `public`:

```sql
GRANT EXECUTE ON FUNCTION "deployment_schema".lock_current_membership(text,text)
    TO "configured_api_role";
```

A separately configured service role only needs the same narrow grant if it
actually calls `Decide`. Do not grant membership UPDATE or blanket function
execution. Migration revokes PUBLIC execution, so a missing grant fails closed;
the existing RLS startup check is not a substitute for this deployment step.
After migration/grant verification, start the new API binary and verify an
authorized approval against its actual role. No credentials are part of this
evidence bundle.

To reproduce the tests, set `FORGE_REVIEW_DATABASE_URL` to the dedicated local
review database and `FORGE_REVIEW_ALLOW_FIXTURES=1`, then run from the repository:

```sh
go test -race ./tests/review -run '^TestReviewApproval' -count=1 -v
```

The helper creates, migrates and drops only its unique private schemas and roles.
Without the explicit fixture environment the database tests skip. The retained
execution additionally set the local `GOCACHE` and `GOPROXY=off`; no helper forces
that machine-specific cache path. `internal/localsetup` and `internal/persistence`
also pass their available package tests. The first sandbox HTTP package check
could not open a loopback socket; the unchanged package passed when rerun with
that access. Those two logs are retained separately, and they are not represented
as another PostgreSQL acceptance or a whole-tree/CI result.
