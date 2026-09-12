# E49 — integrated logging and lifecycle checks

The frozen main-branch commit `0c796c744b90bd08a26513a3586a475387ba775d`
passes the local whole-tree checks on 2026-09-12 UTC (2026-09-11 locally).
Ordinary and race suites each report **563 passing test/subtest entries,
zero failures and 31 explicit skips**, across 23 passing packages; ten other
packages have no tests. These counts are test entries, not independent
end-to-end scenarios. Build, vet, generated contracts and all six Python helper
groups also pass.

[Results](../benchmarks/results/integration-e49-20260912T002550Z/results.json),
[ordinary log](../benchmarks/results/integration-e49-20260912T002550Z/test.log),
[race log](../benchmarks/results/integration-e49-20260912T002550Z/race.log),
[identity](../benchmarks/results/integration-e49-20260912T002550Z/identity.json),
[binary hashes](../benchmarks/results/integration-e49-20260912T002550Z/binaries.json)
and [manifest](../benchmarks/results/integration-e49-20260912T002550Z/manifest.json)
retain the exact execution. All **318 source/build inputs** have identical
before/after hashes. The launcher builds the actual command binaries and uses
the current API binary for the explicitly enabled API lifecycle fixture.
Private PostgreSQL fixtures use the local test server. The run does not migrate
the public schema or replace running demonstration services.

This integrates E44 strict logging and immutable receipt-first log publication,
E47 runner admission closure before RPC drain, E48's corrected worker lifecycle
fixture, and the v4/v5 retention helper gate. The E43 telemetry setup and E47
shutdown ordering are both retained in `cmd/forge-runner`; logging does not
replace either. Six Python groups cover workspace volumes, runner setup,
recovery, evaluation, fault helpers and progress auditing. The source-controlled
CI workflow includes the progress group.

Independent review [recomputed](../benchmarks/results/e49-independent-reviews/forge-e49-and-publication-independent-review.json)
all 38 manifest entries, both 563/31 counts and all 14 native handoff records.
The [worker review](../benchmarks/results/e49-independent-reviews/forge-e48-integrated-independent-review.json)
separately checks 81 files, 318 inputs against Git blobs, actual process identities,
lease timing, original operation/receipt bindings and released capacity.

The actual worker SIGTERM fixture has a **separate** explicitly enabled run:
[E48](worker-sigterm-evidence.md). Its integrated executable and race test
binary use this same commit, but its 47.24-second process observation is not
counted as an opt-in execution inside the ordinary/race suites above. Likewise,
E44's independent real-PG publication tests and Docker capture observations
retain their own source and execution identities. The full suite does not
implicitly repeat Docker floods, model evaluation, load tests, an external
Collector experiment or paired restoration.

The first E48 process experiment failed its final confirmation assertion because
the fixture counted only `effect_completed`, whereas takeover records
`reconciled`. Its failed log remains unchanged. The corrected assertion checks
exactly one terminal confirmation, bound to the successor owner/current epoch
and original receipt dispatch epoch. The integrated repeat passes. This was a
fixture correction, not a change to production settlement semantics.

The evidence remains a local frozen-tree result using installed caches, not a
fresh-machine deployment or proof of the remaining compound requirements.
S12.9's combined fixed-volume log/disk experiment, S16.4's actual runner/Docker
shutdown, S16.6 paired restoration, paid native-model evaluation and the final
mandatory audit remain open. The implementation map therefore keeps
**121 V, 5 I, 1 U**, plus 13 optional X rows.

## Clean-clone continuation

A separate clean checkout of `12126e480d41dd303790e3f9d93d560788866106`
includes the subsequent no-mount test fixture and worker evidence document;
production behavior is unchanged from the full-suite commit above. It passes
command builds, the race-instrumented capture-test build, four actual Docker
capture cases, ordinary whole-tree tests (**568 pass, zero fail, 32 explicit
skips**) and generated contracts. Its **319 inputs** remain unchanged and Git
status is empty before and after. Command build metadata reports
`vcs.modified=false`. This local clone reuses installed Go caches and the
private test PostgreSQL server; it is not a fresh-machine reproduction.

[Clone results](../benchmarks/results/clean-clone-e49-20260912/results.json),
[identity](../benchmarks/results/clean-clone-e49-20260912/identity.json),
[final state](../benchmarks/results/clean-clone-e49-20260912/final-state.json)
and [manifest](../benchmarks/results/clean-clone-e49-20260912/manifest.json)
retain this later execution. The extra five ordinary passing entries and one
skip come from the new no-mount fixture and its local sink/program tests. The
actual Docker invocation is a separate explicitly enabled command, not an
implicit container run inside the ordinary suite. The earlier full race suite
remains the separately identified 563-entry observation.

The first successful Docker capture executable was built in the author worktree.
Nineteen of its twenty linked local inputs match later main; the remaining
domain file adds `ErrContextStale`. The [comparison](../benchmarks/results/e49-independent-reviews/nomount-linked-inputs-main-comparison.json)
preserves that difference rather than treating the first executable as a main
build. The clean-clone repeat uses its own new executable and captures its exact
SHA, source commit and retained streams. Both observations remain separate.

The [independent clone review](../benchmarks/results/e49-independent-reviews/forge-clean-clone-e49-independent-review.json)
recomputes 41 manifest files, 319 inputs against the commit and clone, six binary
identities, both-stream hashes and arithmetic in all four Docker cases, and
the 568/32 ordinary test counts. It confirms the current clone remains clean.
