# E46 — Integrated routing, progress, telemetry and API shutdown

E41, E42, E43 and E45 are now integrated. The runtime preserves one-way model
routing across prepared-attempt recovery, assesses repetition only at committed
closed-batch boundaries, exports actual component trace relationships and
drains accepted API requests during SIGTERM. E44's cumulative logging work and
the remaining worker/runner lifecycle exercise are still separate work.

## Frozen whole-tree checks

The [whole-tree run](../benchmarks/results/integration-checks-20260911T233322Z/results.json)
started from `47743e8` plus the recorded v1/fallback guard change. All **303
source/build inputs** remained identical before and after execution. Build,
ordinary tests, race, vet, API/SQLC/protobuf generation, and six Python helper
groups passed. Ordinary and race each contain **515 passing test/subtest entries,
zero failures and 29 explicit opt-in skips**. Ordinary/race command wall times
were 88.341/104.670 seconds; these are suite timings, not runtime benchmarks.
Skipped workloads do not become verified through this run.

The launcher uses the private loopback review database, explicit local Go
1.26.8/offline module settings and the shared disk-backed build cache. Tests
create and remove their own schemas. It retains CLI binary hashes, upgrade
reports, actual API process-shutdown reports and both cohorts of native handoff
records. Commands run in bounded, dedicated process groups so an abnormal test
exit cannot depend solely on Go cleanup callbacks. This is a local working-tree
check, not a fresh machine, clean-clone build, current CI or live deployment.

## Integration decisions and independent review

- The E43 step wrapper preserves E42's `ErrContextStale` reload and its fault
  barrier after a committed progress transition. E41's barrier remains after
  every successful Advance; its same-epoch inactive-heartbeat exception is
  preserved. Neither a stale message watermark nor a terminal heartbeat fence
  invents a model retry or business cancellation.
- Model preparation inserts the selected route and persisted traceparent
  together. The locked durable snapshot governs the v1 gate, including replay
  of an already prepared attempt; a stale in-memory v2 copy cannot bypass it.
  The Driver rejects that contradictory v1/fallback configuration before model
  dispatch. The new private-PG test observes one retained prepared attempt,
  zero reservations and zero provider calls. Unmodified v1 runs remain supported.
- E42's stop-unknown path retains both the run's allocation and an independent
  sentinel; E43's explicit `lease_yielded` field distinguishes its defer from
  an expired worker. Unknown money remains in the ledger after slot release.
- Independent review accepted the production merge boundaries and found six
  audit omissions across E41/E42. Original reports passed the additional
  cross-checks, but their old auditors accepted substituted wire/cost/receipt
  bindings or inconsistent progress chains. The old records and counterexamples
  remain retained. The revised auditors reject them. See the
  [handoff hardening](provider-handoff-evidence.md) and
  [progress hardening record](../benchmarks/results/no-progress-audit-hardening-20260911/README.md).

The [post-suite checks](../benchmarks/results/integration-final-e46-20260911/results.json)
recompute all **14 native/guard handoff records** from the ordinary and race
cohorts using the strengthened byte, quote, receipt and actual-wire audit.
This does not turn native HTTP fixtures into paid inference or TestBackend
verification into container execution. Those checks also pass the revised
progress auditor's ten regression methods.

## Collector and final default-port correction

The [integrated Collector exercise](../benchmarks/results/integrated-telemetry-20260911T233752Z/report.json)
runs the actual TCP API, private PG, Driver, gRPC transport and SQLite Engine
with a scripted provider and TestBackend. All **215 recorded source inputs**
remain unchanged. The race test passes in 5.930 seconds and observes four claims,
four Provider.Stream invocations, six journal operations, 23 committed state
transitions and **20,749 READY artifact bytes**. New progress reports explain why
this byte total differs from isolated E43's 19,404. The graph is produced by the
independent official Collector and bound to actual PG and journal identities.
Its control-RPC and process-restart limits remain those stated in
[E43](telemetry-chain-evidence.md).

After the whole-tree run, review found that E43 initially gave runner metrics
the API's default port, 8097. The runner now defaults to **8099**, while the API
uses 8097 and worker metrics use 8098. Explicit `FORGE_METRICS_LISTEN` overrides
are unchanged. This final one-line production change has its own build,
runner-package race and vet checks in the post-suite record; it is not relabeled
as the source of the earlier whole-tree run. No running service was restarted.

## Remaining acceptance

The implementation map advances six scoped rows: S09.6, S10.3, S10.8 and
S16.1–S16.3. It now contains **121 V, 5 I and 1 U** mandatory rows, plus the
unchanged 13 optional X rows. S12.9 logging, full S16.4 lifecycle, S16.6 paired
restoration, S17.5 native paid execution, P07 paid quality and S20.6 final global
delivery remain open. A–H compound gates are not promoted from these subset
results. Live services and the public schema retain their previously recorded
versions; this work does not claim a deployment or paid evaluation.
