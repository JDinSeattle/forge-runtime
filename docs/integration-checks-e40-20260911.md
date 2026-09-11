# E40 — Snapshot compatibility and execution evidence integration

The final frozen-source local check passes build, ordinary tests, race tests,
vet, generated contracts and five Python suites. Ordinary and race each record
**448 passing test/subtest entries, zero failures and 28 explicit opt-in skips**;
23 packages pass and 10 have no tests. All 272 recorded Go/SQL/module/protocol/
configuration/script inputs remain unchanged during the final run. The scope is
the a0b3a5d working tree plus the reviewed E37 compatibility hold, E36 harness
and the isolated Git patch test fixture correction.

[Final results](../benchmarks/results/integration-checks-20260911T222418Z/results.json),
[source comparison](../benchmarks/results/integration-checks-20260911T222418Z/source-comparison.json),
[current command binary hashes](../benchmarks/results/integration-checks-20260911T222418Z/binaries.json),
[launcher](../benchmarks/results/integration-checks-20260911T222418Z/check-runner.py.txt)
and [raw manifest](../benchmarks/results/integration-checks-20260911T222418Z/manifest.json)
are retained. This is local working-tree evidence, not an inferred clean-clone
or current-commit CI result.

The launcher explicitly supplies the newly built `forge` and `forge-admin`
executables. Unique private PostgreSQL schemas exercise the actual API and
operator CLI, including two separate 6→11 additive upgrade runs. The public
database and running services are not migrated or redeployed. Real runner
timings remain the separate [E36 experiment](execution-evidence.md); native
interrupted-stream outcomes remain [E35](text-batching-evidence.md). Their opt-in
skips in this broad suite do not replace those actual executions or count as new
paid-provider or Docker acceptance.

## Preserved failures and correction

The [first run](../benchmarks/results/integration-checks-20260911T221741Z/results.json)
passes build and all non-Go-test groups, but ordinary/race each have five failed
entries. The launcher's long workspace `TMPDIR` exceeds Unix socket pathname
capacity in the socket tests (four failed test/subtest entries). Separately,
the Git patch fixture's temporary directory is inside the main checkout, so
`git apply` discovers the ancestor repository and skips paths outside the current
prefix while returning success. The unchanged file-content assertion correctly
detects that no patch was applied.

Restoring `TMPDIR=/tmp` fixes the socket failures. The
[second run](../benchmarks/results/integration-checks-20260911T222041Z/results.json)
still has the Git fixture failure, with 447 passing and one failing entry per
ordinary/race run. Inspection of the installed Go 1.26.8 source explains why:
`testing.T.TempDir` uses `GOTMPDIR` when set. Build intermediates intentionally
remain on the workspace filesystem to avoid the host's `/tmp` quota.

The patch test now initializes its own repository using explicit `--git-dir`
and `--work-tree` paths inside its temporary directory. Every original patch
check, content, deletion and executable-mode assertion remains unchanged.
Independent review mechanically reconstructs the original source after removing
this setup block. The [targeted race check](../benchmarks/results/integration-checks-20260911T222041Z/git-apply-isolated-after.log)
passes with the same problematic workspace `GOTMPDIR`; the final whole-tree
run then passes with only this test file changed. No production behavior is
altered to satisfy these fixture failures.

Two independently reviewed feature sets retain their own original failures:
E37's ambiguous snapshot headers and E36's interface-name probe assumption.
Those failures, the first two broad checks and the separately scoped final
success are not merged into a single passing historical run.

## Temporary storage and evidence boundaries

The host's `/tmp` quota also prevents one tool invocation from starting. After
checking every hash, four superseded and never-executed E35 build-01 executables
are removed, freeing 367,925,975 bytes. Their recorded source, hashes and logs
remain. The four actual E35 build-02 executables, both E36 experiment binaries
and the exact running runner executable copies remain available. The
[cleanup record](../benchmarks/results/completed-build-cleanup-20260911/text-batch-build-01.json)
lists precisely what was removed; no raw report, service, mount or workspace
was deleted by this cleanup.

The map now contains 115 V, 11 I and 1 U mandatory rows plus 13 optional X rows.
No-progress control, provider handoff/fallback, aggregate logs, remaining
telemetry/lifecycle work, paired recovery, paid model evaluation and the final
compound delivery audit remain open.
