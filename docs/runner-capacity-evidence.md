# E30 — Initial runner capacity rejection, 2026-09-11

The targeted application tests pass against **real private PostgreSQL schemas under `-race`**, with package elapsed time **5.557 s**. The main case passes in **3.35 s**. The fixture returns an explicit capacity refusal before doing runner work; its later successful path uses the production Driver, persistence transactions, actual SQLite/file engine and `sandbox.TestBackend`. This is not an experiment that fills real Docker or ext4 capacity, and it makes no paid-model quality claim.

Retained evidence: [original PostgreSQL/race log](../benchmarks/results/runner-capacity-20260911/postgres-race.log), [static race/vet log](../benchmarks/results/runner-capacity-20260911/static-race-vet.log), [derived report](../benchmarks/results/runner-capacity-20260911/report.json), and [source/log manifest](../benchmarks/results/runner-capacity-20260911/manifest.json). The manifest binds all seven task-owned source snapshots to the SHA-256 values frozen before execution. Those snapshots were copied after execution and verified byte for byte; the temporary `go test` executable was not retained. The report does not pretend to be a full historical PostgreSQL export or a clean-commit build attestation.

## Transaction and error boundary

Previously, a direct `PrepareWorkspace` capacity refusal reached the normal task-failure path. The new [Driver branch](../internal/application/driver.go) calls [Store.RequeueCapacity](../internal/persistence/capacity.go) only for that explicit refusal. A capacity result after fencing/adoption continues through the existing error path. Other errors keep their existing handling.

The capacity transaction locks tenant accounting before the run. It checks the current version, owner, epoch and unexpired database-clock lease; the sole pending command must initialize the workspace. The [pure transition](../internal/runtime/reducer.go) additionally requires an uninitialized running state, revision/step/model/tool counters zero and no pending work. Persistence checks for **all durable effects and model attempts**, because initial verification can already have a system-effect intent before the workspace-ready snapshot is committed.

The same transaction records `capacity_rejected`, advances the version, clears lease owner/expiry while retaining its epoch, empties commands, sets the original run to `queued`, writes `not_before = database event time + 1 s`, and releases its allocation and accounting. Runner/workspace identity is retained. `Advance` cannot bypass this transaction by accepting the capacity event directly. A repeated old-version requeue is fenced; a failed capacity transaction makes the Driver yield, without turning an uncertain commit into task failure or discarding an intent.

## Actual assertions

The main run is `run_TOBHCPBZA6J4GTSWTD5BM7BDWS`.

| Observation | Actual result |
| --- | --- |
| First rejection | Epoch 1, expected version 2; database time `2026-09-11T21:07:04.796129Z`, retry at `21:07:05.796129Z` |
| First release | Allocation released; tenant active and runner reserved slots both 0; no prepared workspace file, adoption or model call |
| Before retry | An immediate claim returns not found; an independent sentinel run can claim during the backoff |
| Duplicate release | Eight concurrent old-version requeues are fenced; the sentinel's active/slot counts remain 1/1 |
| Placement | A claim through a different runner cannot acquire the deferred run; the original runner claims the same run at epoch 2 |
| Second rejection | Expected version 4; database time `21:07:05.817280Z`, retry at `21:07:06.817280Z`; only the rejected run's allocation is released |
| Successful retry | Same run at epoch 3 completes; exactly three scripted FakeProvider calls and three total Prepare invocations, including two refusals |
| Durable history | 43 consecutive run events; exactly two capacity-rejection events |
| Final accounting | Sentinel is separately cancelled through the ordinary Driver; tenant active and runner reserved slots both 0 |

The remaining cases verify that an existing in-flight initial system effect makes requeue fail without changing the run/commands/effect or its 1/1 allocation; direct `Advance` is forbidden; an invalid Prepare still ends failed; a reconciliation-required Prepare retains the original running run/allocation; and a post-adoption capacity result does not enter this new requeue path. Four top-level tests contain five leaf cases. Their assertions and fixture boundaries are preserved in the [test snapshot](../benchmarks/results/runner-capacity-20260911/source-snapshot/internal/application/capacity_integration_test.go.txt).

## Reproduction and limits

Select the dedicated loopback `/forge` test database in `FORGE_TEST_DATABASE_URL`; the existing fixture helper creates, migrates and drops only a unique private schema for each case. With that variable set, run from the repository root:

```sh
GOCACHE=/tmp/forge-runtime-gocache GOPROXY=off \
  go test -race ./internal/application \
  -run 'TestPrepareCapacity|TestPrepareNonCapacity' \
  -count=1 -timeout=60s -v
```

Without the database variable, these integration cases explicitly skip. The retained static log records that skip and a passing pure state-machine race test; it is not counted as PostgreSQL execution. The actual PostgreSQL log SHA-256 is `75c872df0fc6e8e4e3615598af90dc85b22aad80d84704ca8f41585ba5a069e0`.

The source review also checked the real runner admission branch: no available configured slot returns `ErrCapacity` before inserting its volume lease; an existing prepared workspace takes its existing-result/fencing branch. The test's deterministic refusal verifies control-plane reaction, not the physical capacity verifier. Earlier actual resource-limit experiments remain separate. These elapsed times describe one acceptance execution, not throughput, latency percentiles or a whole-tree/CI result. The seven source files are frozen for the parent's final integration checks.
