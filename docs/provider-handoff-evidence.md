# E41 — Frozen provider fallback and closed tool boundaries

The implementation adds one operator-selected fallback to a registered run
configuration. Submission freezes both exact provider/model identities; an API
caller still selects a registered `config_id`, not arbitrary providers or keys.
The fallback requires a finite positive run cost limit. An omitted field keeps
the previous request encoding, including idempotency hashes.

```json
"fallback": { "provider": "anthropic", "model": "<registered exact model ID>" }
```

This is a single, one-way route. Its decision is the committed destination
`model_attempts` row, with a `model.handoff_prepared` event in the same transaction.
It does not change the submission snapshot, model round, step, or retry budget.
All subsequent requests remain on that route; configuration removal or a later
destination failure cannot silently return to the primary. Existing prepared
attempts keep their request, price, ID and deadline across recovery.

## Admission and context

Only a persisted, retryable `rate_limited`, `unavailable`, or
`stream_interrupted` failure permits switching. The original persisted
`not_before`, three-attempt limit, two-minute per-step retry window and run
deadline still apply. Cancellation, authentication, protocol/consumer errors,
local capacity refusal and an unconfirmed previous dispatch do not authorize a
new destination. Missing or incompatible fallback registration leaves the
ordinary bounded primary retry in place.

The destination must support tool calling with known context/output limits. Its
portable request must fit the conservative input bound, context window and
512 KiB envelope limit. Its immutable price quote is reserved against the
remaining run budget, including all unresolved prior model obligations.
Insufficient money stops the run before a fallback attempt is inserted. Normal
quota admission and shared circuit-breaker checks then govern dispatch.

Every native continuation also retains the portable public messages from the
same accepted-message watermark: the original task/system instructions, the
existing bounded factual summary, the latest four completed turns, paired tool
results and verification feedback. A handoff uses that frozen representation;
it does not consume new messages or emit a second ContextBuilt transition.
Opaque reasoning, signatures, native response IDs and adapter state are removed.
Public tool-call IDs are deterministically rebound to `handoff_N`, preserving
pairing while avoiding a foreign vendor's ID syntax.

The run lock, current version/lease, StageModel, pending effect/approval fields
and the SQL effect ledger are checked before route preparation. Every completed
effect requires a READY receipt. A remaining planned tool is treated as skipped
only in an earlier step with a preceding failed/cancelled tool and READY receipt.
Unknown operations, missing receipts and open/ambiguous public tool batches
block the change. Provisional tool deltas from an interrupted model response
never become executable effects.

## Retained execution

The latest targeted source run is
[20260911T230147Z](../benchmarks/results/provider-handoff-20260911T230147Z/results.json):
278 unchanged source/build inputs, 37 test/subtest passing entries, no failures
or skips, `-race`, affected-package vet and generated API consistency. The race
command took 36.825 seconds; this is test execution time, not a performance SLO.

Both OpenAI→Anthropic and Anthropic→OpenAI execute the production SDK against
local HTTP/SSE fixtures, a private PostgreSQL schema, production Driver and a
real SQLite runner journal. The sandbox TestBackend checks file contents; it
does not execute Python or demonstrate model quality. Each run performs one
committed read, discards a partial patch stream, prepares its fallback, interrupts
Driver at a fault hook before reservation, reclaims that same attempt after a
fixture-controlled lease expiry, applies one real file patch,
and completes the fixture's trusted-verification protocol. This simulates a
worker interruption; it is not an OS-process crash or runner restart. Each records four
attempts, three model rounds, one handoff and exactly two model tool effects.

The failed source attempt retains 66,560 synthetic microdollars of unknown
obligation after completion; total ledger exposure is 67,110 in each run. These
are test prices, not vendor invoices. The destination's first request carries
no source opaque marker, its next request retains its own native marker, and
its actual HTTP timestamp follows the persisted retry time. A price change
between preparation and recovery leaves the prepared attempt unchanged; the
next step uses the new quote while keeping the selected destination.

Five additional private-PG cases cover destination retry exhaustion, unsupported
tools, insufficient context, removed model registration and an unknown prior
cost that leaves too little budget. The last stops after one attempt with its
9,216 synthetic-microdollar reservation intact. Separate cases reject a forged
in-memory route, an old epoch, unknown effects and missing READY receipts.

Seven structured run exports retain 71 exact artifact byte copies, their PG
metadata, attempts, fees, events and wire requests. The standalone
[recompute script](../benchmarks/results/provider-handoff-e41-audit/recompute.py)
verifies hashes/lengths, context reconstruction, paired IDs, route/attempt
ordering, backoff and ledger exposure. Its
[report](../benchmarks/results/provider-handoff-e41-audit/report.json) also rejects
four deliberate corruptions: foreign opaque leakage, retry reset, refund of an
unknown fee, and artifact byte tampering. This is an independently recomputable
oracle written by the implementer, not an independent reviewer claim.

Independent review found three omissions in that original audit: a modified
target wire model/history, changed settled cost, or substituted effect receipt
could pass. The retained originals were independently checked and were internally
consistent; the three copy-only counterexamples are preserved in the
[v2 audit evidence](../benchmarks/results/provider-handoff-e41-audit-v2/forge-e41-independent-audit-negatives.json).
The [current audit](../scripts/provider-handoff/audit.py) additionally binds the
actual destination model, output cap, system/history and tool definitions to its
hashed portable request; recomputes all 21 frozen reservation quotes and known
zero-cache charges; and checks 17 effect receipts against their SQL request
identity, epoch, revision, status and canonical argument bytes. It fails closed
for cache usage outside these retained fixtures. The
[v2 report](../benchmarks/results/provider-handoff-e41-audit-v2/report.json) passes
all seven original records and rejects 22 copy-only corruptions. Independent
review reran the audit and separately confirmed that its original three
counterexamples are now rejected. The v1 script, reports and original raw
records remain unchanged.

## Preserved terminal-heartbeat failure

The first three scoped runs passed. The
[fourth run](../benchmarks/results/provider-handoff-20260911T225718Z/results.json)
then exposed a real existing race: PG had committed `completed`, but the
heartbeat's expected fencing error cancelled Drive before its final reload.
Drive returned `context.Canceled` despite the durable terminal state.

The [controlled pre-fix run](../benchmarks/results/provider-handoff-20260911T225953Z/results.json)
holds execution after a committed transition for two heartbeat ticks. Both
`completed` and `waiting_approval` reproduce the error. The fix permits an
authoritative same-epoch inactive state to end the heartbeat without cancelling
Drive; unknown/mismatched state, lease loss and parent cancellation retain their
normal cancellation behavior. Both controlled cases pass in the latest run.
The original intermittent failure, controlled failures and original manifests
remain unchanged.

## Integration boundary

This working-tree evidence is anchored at `5de6f0f`, whose separate
[GitHub CI](../benchmarks/results/github-actions-5de6f0f/run.json) passed before
E41. It does not certify a future commit or a live deployment. Integration with
E42's explicit snapshot-v2 admission/old-reader guard remains required before
promoting S10.3/S10.8; existing v1 runs keep their original configuration.
No paid provider call, live database migration, runner replacement or mount
change was performed by E41.
