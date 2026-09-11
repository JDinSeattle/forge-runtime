# E34 — Integrated admission, control and dependency checks

The frozen working tree based on `224a6b64ce0cc41c564ce2c9bfd2e74b4b9ccc0a`
passes build, ordinary tests, race tests, vet, generated-contract checks and all
five Python helper suites on 2026-09-11. Ordinary and race runs each have **368
passing test/subtest entries, zero failures and 27 explicit opt-in skips** across
21 passing packages. Twelve further packages have no tests. These are counted
entries, not 368 independent end-to-end scenarios.

[Final check results](../benchmarks/results/integration-checks-20260911T212636Z/results.json),
[ordinary JSON log](../benchmarks/results/integration-checks-20260911T212636Z/test.log),
[race JSON log](../benchmarks/results/integration-checks-20260911T212636Z/race.log),
[identity](../benchmarks/results/integration-checks-20260911T212636Z/identity.json)
and [manifest](../benchmarks/results/integration-checks-20260911T212636Z/manifest.json)
are retained. This local result is separate from GitHub Actions and from the
earlier revision's successful CI. It integrates E26–E33 implementation and test
changes; it does not rerun every opt-in container, load or model experiment.

The launcher builds actual command binaries into a fresh temporary directory,
then executes `go test -json -count=1 ./...`, its `-race` counterpart, `go vet`,
`make check-generated`, and isolated Python unittest discovery for volume,
runner, recovery, evaluation and fault helpers. The CLI Resume test receives
the freshly built executable. Private loopback PostgreSQL schema fixtures are
enabled explicitly; their setup/cleanup does not migrate the live public schema
or replace running services. No paid provider calls occur. Docker fault tests,
load scenarios, collector delivery and paired restoration retain their separate
execution gates and evidence.

The before/after maps contain **260 source/build-input files** with identical
hashes. They cover Go/module files, SQL, YAML, proto, Python, shell and Makefile
inputs, including the current CI workflow; documentation, raw evidence and local
runtime state are excluded. The exact [launcher](../benchmarks/results/integration-checks-20260911T212636Z/check-runner.py.txt)
and [built command hashes](../benchmarks/results/integration-checks-20260911T212636Z/binaries.json)
are retained. This is a working-tree check using installed Go caches, not a
fresh-machine deployment or an attestation of old workload binaries.

## Preserved first failure and correction

The [first full run](../benchmarks/results/integration-checks-20260911T212356Z/results.json)
has one ordinary-test failure:
`TestTransactionDeadlineCannotResetAndRollbackUsesCleanupContext` observes
`context canceled` after its 80 ms transaction deadline. Its race suite passes;
that does not invalidate the ordinary failure. All original logs and source
hashes remain unchanged.

`transaction.operation` created an operation timer at the transaction's absolute
deadline, but also forwarded every parent completion using `CancelFunc`. When
the transaction timer fired first, the callback could beat the operation timer
and replace the intended timeout classification with cancellation. The SQL
operation still stopped; its error cause was unstable.

The correction forwards only explicit `context.Canceled`, leaving deadline
classification to the operation's own timer at that same absolute deadline.
Earlier caller deadlines and explicit caller cancellation still apply. The
transaction's original deadline cannot be extended by passing Background; its
rollback retains the independent one-second cleanup context. A regression covers
100 competing timer pairs plus parent cancellation before/after operation
creation and caller cancellation. Independent review finds no P1/P2 issue and
passes a targeted race run; the final real-PG ordinary/race suites above also
pass. The correction does not change lease or fee accounting.

## Current additive upgrade coverage

Both final suites upgrade a unique PostgreSQL fixture from schema **6 to 10
twice**. They compare every original run column, effects, artifacts and immutable
snapshot bytes; the newly added priority is checked separately to default to
zero on existing runs. Legacy audit inputs remain NULL and the cleanup table
retains RLS. The [ordinary report](../benchmarks/results/integration-checks-20260911T212636Z/test-upgrade/postgres-upgrade.json)
and [race report](../benchmarks/results/integration-checks-20260911T212636Z/race-upgrade/postgres-upgrade.json)
are new observations; E19's historical 6→8 reports remain unchanged. Migration
10's approval authority helper also passes the E32 permission/race cases in
both suites. An existing deployment still needs migration 10 and its precise
function EXECUTE grant before using the new approval implementation.

This closes the current integration checks. Remaining runtime behavior,
operational trace/shutdown work, execution measurements, paired restoration and
budgeted model evaluation remain in the implementation map.
