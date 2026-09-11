# E31 — Successful Resume and duplicate capacity release, 2026-09-11

Three targeted tests passed using **real loopback HTTP, private PostgreSQL schemas and a race-instrumented test binary**. A separately executed `forge` CLI also successfully resumed the reviewed version. The cases add the missing successful-Resume evidence for S01.2 and repeated Resume/Finalize capacity checks for S08.6. [Raw test log](../benchmarks/results/resume-capacity-20260911T2110-root/test.log).

This is explicitly a **control transaction fixture**. No model, runner process, Docker command, real unknown external effect or trusted verification job is executed. Fixture states are constructed through ordinary Store transitions, with a real local artifact whose contents declare `no_runner_or_model_executed=true`. Declarative stop/verification events let the test reach finalization; they are not physical stopped-writer or grader evidence. Actual external-effect recovery remains covered separately by [E21](application-fault-evidence.md), [E25](application-continuation-evidence.md) and [E27](application-network-evidence.md).

## Observed results

| Test | Original run ID | Observation | Duration |
|---|---|---|---|
| HTTP Resume | `run_OUEGA4ROYE6U6RSJE6GH3XOCFJ` | 12 simultaneous requests for version 7 produce one 202/version 8 and eleven 409; another newly reviewed version succeeds at version 9 | 0.44 s |
| Duplicate Finalize | `run_HYOODT743ILNO6ZYWCURCDS2C5` | 12 simultaneous Finalize calls produce one commit at version 16 and eleven conflicts; one terminal event | 0.60 s |
| Real CLI Resume | `run_4L66XTOGCHSHKCEJXEQXF3CYGS` | `forge run resume … --version 7` exits 0 and prints the original run at version 8 | 0.37 s |

These are individual acceptance durations, not latency percentiles. The test HTTP handler runs in the test process on a real TCP listener. CLI PID **1256410** is a separate OS process, with its own production generated HTTP client. Its environment contains only PATH, locale/timezone and this fixture's API token; the PostgreSQL credential is not passed to it. [Exact CLI argv, PID and result](../benchmarks/results/resume-capacity-20260911T2110-root/cli-resume/cli.json), [original stdout bytes](../benchmarks/results/resume-capacity-20260911T2110-root/cli-resume/cli.stdout), [stderr](../benchmarks/results/resume-capacity-20260911T2110-root/cli-resume/cli.stderr).

### Resume preserves the unknown effect and its allocation

The fixture legally reaches `needs_reconciliation` via `EventEffectUncertain`. An initial HTTP Resume is rejected with 409 while the original worker lease remains valid. Ordinary `Store.Defer` then ends that lease; the fixture does not directly rewrite a run lease or wait for natural expiry. [Before Resume](../benchmarks/results/resume-capacity-20260911T2110-root/http-resume/before-resume-postgres.json), [active-owner rejection](../benchmarks/results/resume-capacity-20260911T2110-root/http-resume/active-owner-response.json), [eligible state](../benchmarks/results/resume-capacity-20260911T2110-root/http-resume/eligible-postgres.json).

The successful request clears the driver owner and advances the run version. It retains `needs_reconciliation`, the original `review_runner` placement, workspace revision 1, the exact operation ID, canonical arguments/hash, policy, expected revision and dispatch epoch **1**. It emits no execution command and creates no new logical run. The persistent effect remains unknown with no receipt ref. HTTP 202 acknowledges the control intent; it does not certify completed recovery. [Twelve actual responses](../benchmarks/results/resume-capacity-20260911T2110-root/http-resume/concurrent-resume-responses.json), [after concurrent Resume](../benchmarks/results/resume-capacity-20260911T2110-root/http-resume/after-concurrent-resume-postgres.json).

Repeating the old version is a CAS conflict. A request using the newly reviewed current version can record a second Resume intent; it also leaves the original effect and capacity untouched. The final state has exactly two `run.resume_requested` events, corresponding to the two accepted versions. [Reviewed repeat](../benchmarks/results/resume-capacity-20260911T2110-root/http-resume/reviewed-repeat-response.json), [final records](../benchmarks/results/resume-capacity-20260911T2110-root/http-resume/final-postgres.json).

### A still-active sentinel exposes double release

Each case claims a second, separate sentinel run on the same tenant and runner. Both counters and the sum of non-released allocation slots start at **2**. Resume, repeated Resume, and the replacement claim leave all three values at **2**. The sentinel's run row and allocation remain unchanged.

In the Finalize case, the replacement claim uses epoch **2**, while the original effect retains dispatch epoch **1**. Explicit control-fixture adoption/reconciliation/verification events bring the run to version **15**, ready to finalize. Exactly one of twelve concurrent `Store.Advance(EventFinalized)` calls commits version **16**. Tenant active count, runner reserved slots and non-released allocation sum become **1**, correctly retaining the sentinel. [Before finalization](../benchmarks/results/resume-capacity-20260911T2110-root/finalize/before-finalize-postgres.json), [after finalization](../benchmarks/results/resume-capacity-20260911T2110-root/finalize/after-finalize-postgres.json).

Subsequent stale-version and current-version Finalize calls are rejected, as are both corresponding HTTP Resume calls against the terminal run. The complete saved run/effect/allocation/counter/snapshot/event capture is identical before and after these terminal repeats. The sentinel still owns one slot and the target has one `run.finished` event. This checks for double decrement with a nonzero balance; an implementation that incorrectly clamps an underflow at zero would not evade the sentinel assertion. [After terminal repeats](../benchmarks/results/resume-capacity-20260911T2110-root/finalize/after-terminal-repeats-postgres.json).

## Audit and executed source

The [author recomputation](../benchmarks/results/resume-capacity-20260911T2110-root/author-audit.json) checks all **10** saved phase captures, original run/effect bindings, sentinel rows, allocation sums, contiguous event/snapshot versions, the exact CLI stdout digest and the explicit 74-byte control artifact in each case. The final target event counts are **11/18/10** for HTTP Resume/Finalize/CLI Resume; including each sentinel's two events, the three final captures contain **13/20/12**, or **45 events** total. The audit is a fresh read of saved raw records, not another execution or an independent implementation review. Concurrent Finalize error counts are Go assertions in the executed test; the saved PG records separately prove the single terminal commit.

Each phase capture uses **one SQL statement and one MVCC snapshot** to export its seven table groups. Different phases remain separate snapshots. The effective HTTP role passes production `CheckAPIRole` with NOBYPASSRLS and no superuser/ownership privileges. The existing review helper uses `SET ROLE` from an administrator session; this evidence does not claim a separate restricted LOGIN. Every case uses a unique migrated private schema, which the helper drops after recording results. The original application services, Docker daemon and fixed workspace pool are not used.

| Executable | SHA-256 |
|---|---|
| Race-instrumented `review.test` | `996e2029f51bfaf780edfe3acb12e6c4f1bb9e246f393afb79b6475a0edd8862` |
| Real `forge` CLI | `fe9ffe146c7b803ce2ee80b456baaf7e42f816551a38360207540b682bbaf1e7` |

The [source identity](../benchmarks/results/resume-capacity-20260911T2110-root/source-identity.json) records **177 Go source hashes**, unchanged between build start and the sandbox execution attempt. These are working-tree sources based on commit `224a6b64ce0cc41c564ce2c9bfd2e74b4b9ccc0a`, including uncommitted changes; the commit alone is not their complete identity. The [test source](../benchmarks/results/resume-capacity-20260911T2110-root/resume_capacity_test.go.txt) and matching helper/production source snapshots are archived. Go **1.26.8** and `-race=true` appear in the [test build information](../benchmarks/results/resume-capacity-20260911T2110-root/review.test-build-info.log). Subsequent approval changes and final whole-repository validation are separate.

The [launcher](../tests/review/run_resume_capacity.py) verifies both binary hashes before execution, reads an owner-only private environment file without recording its values, and runs only the three bounded tests. The [actual execution record](../benchmarks/results/resume-capacity-20260911T2110-root/execution.json) is exit **0**. Its copied build record retains the earlier sandbox failure code as historical provenance; it is not the successful execution's exit code. The [author manifest](../benchmarks/results/resume-capacity-20260911T2110-root/author-manifest.json) fingerprints these records and the two initial failures.

## Preserved initial failures and reproduction

The [first build](../benchmarks/results/resume-capacity-20260911T210034Z/build-tests.log) encountered the temporary parallel-development state where Driver referenced `RequeueCapacity` before its Store implementation existed. No test/schema began. The next build succeeded, but the [sandbox attempt](../benchmarks/results/resume-capacity-20260911T210708Z/test.log) was denied loopback sockets before any schema creation. Both records remain intact; neither is classified as a product Resume failure or an accepted run. The main agent executed those same hash-verified binaries in its existing socket-enabled context.

Build and reproduce against an explicitly permitted loopback review database, using fresh output paths:

```bash
review_build_dir="$(mktemp -d /tmp/forge-resume.XXXXXX)"
GOCACHE=/tmp/forge-runtime-gocache go build -o "$review_build_dir/forge" ./cmd/forge
GOCACHE=/tmp/forge-runtime-gocache go test -race -c \
  -o "$review_build_dir/review.test" ./tests/review
```

The [recorded build commands](../benchmarks/results/resume-capacity-20260911T210708Z/command.json) and [actual launch command](../benchmarks/results/resume-capacity-20260911T2110-root/command.json) identify the accepted binaries and test selection. Set `FORGE_REVIEW_DATABASE_URL` to the disposable loopback database, `FORGE_REVIEW_ALLOW_FIXTURES=1`, `FORGE_REVIEW_FORGE_BINARY` to the newly built CLI, and `FORGE_REVIEW_RESUME_EVIDENCE` to a fresh directory. Execute the test binary with the recorded test-name regex and `-test.timeout=2m`; never print or commit the private DSN/token. This record closes the specified control/transaction evidence gap without upgrading synthetic receipt metadata into real runner recovery evidence.
