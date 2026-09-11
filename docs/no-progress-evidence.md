# E42 — Closed-batch no-progress limit

New submissions freeze `max_no_progress_batches=3` unless a registered project
configuration provides another ceiling (1–20). HTTP budgets and
`forge submit --max-no-progress-batches N` can reduce that ceiling. The fourth
identical completed batch reaches the default limit: its first observation is
new evidence; the following three are repetitions. The worker requests a normal
authenticated workspace stop before publishing terminal `budget_exhausted` with
`failure_reason=repeated_no_progress`. It does not issue the next model call.

This is a narrow, deterministic repetition detector, not a judgment of semantic
task progress. It reads complete operation receipts and verification results;
streaming text/tool fragments, model prose length and revision counters cannot
advance or reset it.

## Decision contract

- `internal/application/progress.go` builds a `progress_report` only at the next
  context boundary after a closed tool batch or failed finish verification.
  Latest model attempt, step, tool ordinal, operation identity, canonical argument
  hash, workspace, dispatch epoch, policy and terminal receipt are bound before
  the report is published. A failed batch includes actual receipts through its
  first failure; its remaining planned, unexecuted intents are not evidence.
- The fingerprint keeps the operation kind, normalized arguments, result, error
  and terminal status. Successful `read_file` uses the actual returned path,
  file SHA, first line and content so omitted/default equivalent ranges are equal.
  Other arguments use their checked canonical SHA. Command job IDs and outer
  operation/revision identities are excluded. Commands, patches and verification
  also include before/after content hashes. Verification includes trusted
  baseline/target/regression outcomes.
- A previously unseen observation or resulting content tree resets the counter.
  Reading a different file/slice/content is evidence, even without an edit.
  Revisiting a known tree after A→B→A does not reset through revision increments.
  The first before-tree seeds the baseline without itself being progress.
- Receipt or context truncation, a running/truncated job, truncated search, or
  verification feedback exceeding its visible bound yields `indeterminate` and
  resets the repetition streak conservatively. It is not labeled new evidence.
- Histories are bounded FIFO first-observation sets: 512 fingerprints and 32 tree
  hashes. Eviction treats an old fact as new if seen again. Arbitrary timestamps,
  randomized output and semantically equivalent but differently phrased results
  are not stripped. Long/noisy loops can evade this detector; the existing model,
  tool, money and deadline budgets still bound execution.

## Atomicity, messages and stopping

`internal/persistence/progress.go` checks the exact READY report hash, same-scope
receipt set, latest completed attempt and absence of unknown/in-flight effects
inside the existing tenant→run transition transaction. It performs no artifact,
model or runner I/O while holding SQL locks. The pure reducer updates the bounded
counter in the same persisted snapshot as the stop decision; CAS/replay cannot
increment it twice.

Context and report share one consumed-message watermark. `AddMessage` and
`Advance` lock the same tenant/run rows. If a message commits before that context
transition, `ErrContextStale` reloads/rebuilds the same step without sampling a
model or consuming a round. The new watermark resets once when it is committed.
If the stop decision wins first, a new message is rejected with `ErrTransition`;
it cannot reopen stopping work. Same-key message retries remain idempotent.

The stop path retains effects, allocation and tenant capacity until its normal
stop receipt confirms no active operations. E42 exposed an existing `Store.Defer`
bug: `cancel_requested` at `build_context` could release capacity while stop was
still unknown. Release now additionally requires `StatusRunning` and an empty
stop target. The private-PG sentinel test observes 2 reserved slots while stop is
unknown, 1 after the main stop, still 1 after duplicate terminal settlement, and
0 after separately stopping its own sentinel.

Unknown model usage remains an independent ledger obligation after termination.
The positive-price fixture holds 4 × (8192+1024) = **36,864 microUSD**, with unknown
actual usage. Natural request deadlines release concurrency slots without
refunding that money. These are synthetic prices and fake provider turns, not
charges from a paid API.

## Snapshot compatibility

- New real submissions use snapshot v2 and freeze a positive limit. Default
  resolution happens after hashing/idempotency lookup, preserving the old
  omitted-field request hash. An identical omitted-field retry reuses its run.
- v1 snapshots without the new fields keep the old behavior and are not silently
  upgraded. Transition rejects v1 containing the protected progress fields.
- SQL and Go accept unambiguous exact numeric headers 1 or 2. Future v3, duplicate
  headers, case aliases and Unicode long-s aliases remain held/fenced under the
  existing compatibility protocol. No database migration is required here.
- This branch does not add provider fallback. The independent E41 integration
  must retain the same watermark and avoid emitting another `ContextBuilt` for a
  fallback attempt; its v1/fallback gate is separate integration work.

## Executed evidence and provenance

Evidence root: [`benchmarks/results/no-progress-20260911`](../benchmarks/results/no-progress-20260911).
`pre-freeze/` preserves the original cohorts, their binary SHA, source hashes,
source tarballs and diffs. `logs/` retains failures as well as passes. Source
archives are compressed so `go test ./...` does not discover copied Go packages.
Test executables remain locally in this worktree's ignored `var/e42-builds/`.
No credentials or process environment are exported to reports.

The frozen execution started from clean tracked commit
`7ee738b5b43a3044c4323e7467bef72d6721c14e` and ran on 2026-09-11 from
23:22:30.785128 to 23:23:08.233946 UTC. The retained Go 1.26.8 linux/amd64 race
binary has SHA-256
`7f03464132bc9e37fa2714238beca8828549ee9502edab732955ec165d7ed5d9`.
Its seven top-level `TestProgress` tests and five subtests passed in **37.448813 s**
of subprocess wall time, with nine private-PG JSON reports. All 274 recorded
code/build/script input hashes matched before and after; tracked status stayed
clean. See [`final/execution.json`](../benchmarks/results/no-progress-20260911/final/execution.json),
[`final/before-build.json`](../benchmarks/results/no-progress-20260911/final/before-build.json)
and the [raw test log](../benchmarks/results/no-progress-20260911/logs/frozen-app-race.log).
The shared host/cache were used; this is not an independent clean-clone build or
a latency benchmark. Source tarballs, module declarations and build settings are
retained, but dependency-cache/compiler supply-chain provenance is not attested.

| Frozen case | Actual observation |
| --- | --- |
| repeated read / failed finish / failed tool batch | Each makes four fake model calls, commits counters 0→1→2→3, confirms stop and keeps 36,864 unknown microUSD. Read additionally observes natural request deadlines release only request slots. |
| message wins the race | Step 4 context is rebuilt twice, watermark advances once, and seven total fake calls reach the later stop. The stale frame neither consumes a round nor increments progress. |
| return before / after progress commit | Persisted counters are respectively 2 / 3 at interruption. Natural expiry and epoch-2 adoption finish with four total calls each, without double-counting. |
| unknown stop with sentinel | Actual SQL capacity snapshots show 2→1→1→0, retaining both allocations until main stop is confirmed and preserving the sentinel through duplicate settlement. |
| legacy snapshot / omitted config retry | Same original key is reused; explicit old v1 snapshot completes in three calls without a progress state or implicit upgrade. |
| mismatched evidence | Seven forged/stale/unknown-ledger advances are rejected without changing snapshot version/progress; the actual repair still completes. |

The separate [Python audit](../benchmarks/results/no-progress-20260911/logs/frozen-independent-audit.json)
reconstructed **41 committed guarded context boundaries** from all nine records,
including zero for the deliberately unguarded v1 run. All four positive/negative
auditor regressions passed on the final records. `summary.json` is only a
convenience index; the original report bytes and logs remain the evidence.

Before this final bounded execution, the complete `internal/application` race
suite passed 30 top-level tests plus 38 subtests. The complete `tests/review` race
suite passed 38 top-level tests plus 36 subtests, with five explicitly opt-in
top-level tests skipped. These have their own pre-freeze source/binary identities
and are not relabeled as executions of the later frozen binary. Runtime/persistence
race, HTTP/CLI loopback race, all-package compilation, `go vet ./...`,
API/SQLC/protobuf generation checks and offline-audit regressions also passed.
See [`suite-counts.json`](../benchmarks/results/no-progress-20260911/suite-counts.json)
for exact counts and skipped names. `pre-freeze-manifest.json` describes the
intermediate archive at commit `7ee738b`; the final `manifest.json` hashes the
completed archive, including the refreshed `final/` records.

The existing opt-in terminal benchmark also manually issues `ContextBuilt`.
Its second context now publishes a bound v2 report using its already-completed
attempt and simulated receipt. This is an explicitly synthetic observation, not
Driver normalization, and does not disable the guard or downgrade snapshots.
A separate minimal race smoke passed **10 runs / 10 simulated effects / 20 fake
model calls**, split 5/5 across its two worker goroutines. All allocations were
released, active/request counters were zero, and 220 event rows had no gaps.
The test logged 2.99 s; this is a compatibility smoke, not a fresh 1000-run
performance result. See [`terminal-smoke/`](../benchmarks/results/no-progress-20260911/terminal-smoke)
and its [raw log](../benchmarks/results/no-progress-20260911/logs/terminal-smoke-race.log).
Its separate race binary SHA is
`4078ea3da5922fc94749cbe9560225d2959acb4430be96c31e706844d77e8903`;
the dirty-base source hashes, diff and source archive are retained. Earlier
E09/E39 benchmark measurements are unchanged and retain their original identities.

Failures retained, rather than relabeled:

| Cohort | Result and correction |
| --- | --- |
| first-offline / offline-02 | Old future-version expectation, restricted loopback, and an unused test import; corrected before actual private-PG execution. |
| pg-01 | Core closed-batch cases passed. Message fixture had only four request slots for seven deliberately unknown-usage turns. Crash fixture shortened lease to the signer's skew, leaving no usable grant lifetime. Both fixture assumptions were fixed; the failed records remain. |
| pg-02 | Corrected message and both recovery boundaries passed. Its earlier fingerprint algorithm/source is preserved under its own binary identity. |
| pg-03 | Actual unconfirmed-stop capacity assertion failed; the production `Defer` condition was fixed. Legacy-v1 completion passed. |
| pg-04 | Sentinel and positive unknown-money tests passed after that fix. |
| final-review | Three old manually scripted context fixtures lacked the new consumed-message/report contract. They now publish bound synthetic reports/attempts while retaining their original assertions. They are control-protocol fixtures, not Driver semantic-progress evidence. |
| final-review-02 | Complete review race suite passed; raw v3/ambiguous-header compatibility reports retained. Optional real CLI/operator/recovery cases without their opt-in inputs remain skipped. |

The Driver integration uses a real private PostgreSQL schema, real SQLite runner,
real file tools and `sandbox.TestBackend` with a scripted provider. It does not
prove Docker execution, process-kill recovery, remote provider behavior or paid
model quality. Recovery injection is a controlled Driver return before/after the
commit followed by natural lease expiry and a new claim. Final whole-repository
tests and independent source review after merging other feature branches remain
the parent task's responsibility.

## Reproduce the bounded checks

Provide an authorized disposable `FORGE_TEST_DATABASE_URL`; the test helper
creates/drops only unique private schemas. Do not point it at production.

```sh
go test -race ./internal/runtime ./internal/persistence ./internal/httpapi ./cmd/forge -count=1
FORGE_PROGRESS_EVIDENCE_DIR="$PWD/var/progress-evidence" \
  go test -race ./internal/application -run '^TestProgress' -count=1 -v
go test -race ./tests/review -count=1 -v
python3 scripts/progress/audit.py benchmarks/results/no-progress-20260911/final
python3 -m unittest discover -s scripts/progress -v
make check-generated
```

The standalone Python auditor re-normalizes retained receipt bytes, validates
artifact hashes/ledger bindings and replays only committed context boundaries.
Its negative tests alter a counter, receipt or ledger argument. It verifies saved
fixture records; it is not an independent clean-build or live-database attestation.
