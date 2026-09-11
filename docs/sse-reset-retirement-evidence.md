# E38 — Retire an SSE hub before announcing reset

GitHub Actions for `a9208e79879d29373eb3964e098eff286aa6de82` exposes a real
reconnection race: `TestReviewSSEReconnectAfterGapStartsFreshHub` receives
`context canceled` on its replacement subscription. The [failed CI record](../benchmarks/results/github-actions-a9208e7/run.json)
and [original log](../benchmarks/results/github-actions-a9208e7/execution.log)
remain failures. The local full suite passed before this CI execution; that does
not supersede the observed failure. A subsequent 200-run local repetition of
the old test also passes, illustrating why uncontrolled repetition alone is
insufficient to rule out this interleaving.

The old hub publishes `ErrReset` while it is still present in the manager's
admission map. A client can immediately subscribe to that dying generation
before deferred cleanup removes it. Cleanup then cancels the new subscription.
The fix performs retirement under the existing **manager → hub** lock order:
remove this generation from the map, cancel its poller, then notify and close its
subscribers. Both a missing sequence and an explicit source reset use that same
boundary. Deferred cleanup and late old-handler Close still compare generation
identity, so neither removes a replacement hub.

## Controlled reproduction and validation

A new regression holds the manager's admission lock, then releases a gated
source response. A reset cannot be published while that lock prevents removal
of the dying generation. The [original code fails](../benchmarks/results/sse-reset-race-20260911/controlled-before-race.log)
both variants—missing sequence and source `ErrReset`—and both replacement
subscriptions are canceled. This uses the real Manager with a controlled
in-memory Source, not TCP or a database. Each fixed case also reconnects before
closing the old handler and requires delivery of the next sequence.

In an independent local checkout of a9208e7, only three source files change:
`internal/eventstream/hub.go`, its new `hub_test.go`, and the existing review
reconnect test's late-Close assertion. The final checks pass:

- 200 repetitions of the reset-detachment test (both variants), plus 200
  repetitions of each existing SSE review scenario, with race detection.
- The complete eventstream, HTTP API and review packages under race detection
  with explicit real private PostgreSQL/HTTP fixtures enabled.

[Repeated results](../benchmarks/results/sse-reset-race-20260911/repeat-results.json),
[repeat log](../benchmarks/results/sse-reset-race-20260911/repeat-after-race.log),
[PG/HTTP log](../benchmarks/results/sse-reset-race-20260911/repeat-postgres-http-race.log),
before/after source snapshots and hashes are retained. The package checks take
14.907 s and 18.957 s respectively; these are test durations, not latency
measurements. They do not repeat the sustained SSE load or certify unrelated
concurrent feature work.

The first attempts to compile the fixed code hit the host's `/tmp` disk quota;
their failed build logs are preserved and are not product-test outcomes.
Ten hash-verified command binaries from already completed E34 checks were
removed from `/tmp`, retaining their hashes, source identities and raw results.
Go build temporaries then use an ignored workspace directory. The exact same
fixed sources pass the repeat; no test assertion or race setting is relaxed.
[Cleanup record](../benchmarks/results/sse-reset-race-20260911/completed-temporary-binary-cleanup.json),
[manifest](../benchmarks/results/sse-reset-race-20260911/manifest.json).

An independent implementation review finds no P1/P2 issue in lock ordering,
reset visibility, repeated retirement or late cleanup of an older generation.

The exact correction commit `7b7504711538aa8fb9a9ad3b3124d0043e545bb2`
passes [GitHub Actions 34651379712](https://github.com/JDinSeattle/forge-runtime/actions/runs/34651379712):
build, ordinary/race tests, vet, generation and five Python suites. The
[run record](../benchmarks/results/github-actions-7b75047/run.json),
[original execution log](../benchmarks/results/github-actions-7b75047/execution.log)
and hash manifest retain that identity separately from the failed a9208e7 run.
Current services are not redeployed by this fix. Concurrent text batching,
snapshot compatibility and later execution evidence are not included in this
successful CI revision.
