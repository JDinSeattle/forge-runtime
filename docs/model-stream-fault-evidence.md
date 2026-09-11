# E26 / F03 — Native stream interruption with the application ledger

The OpenAI Responses and Anthropic Messages cases both passed real loopback
HTTP → native SDK/adapter → production `application.Driver` → private PostgreSQL
acceptance under the Go race detector on 2026-09-11. Each case emitted provisional
text, one fully ended tool item and another unfinished tool-argument fragment,
then cut the TCP response before the complete turn and final usage. Neither
fragment became an effect. The dispatched request's unknown fee/token obligation
survived natural worker lease expiry and subsequent request-slot expiry.

This is a local protocol/ledger experiment with synthetic provider bytes and
prices. The runner records calls and allows only workspace preparation; no
Docker, command, file edit, real model inference or paid service is involved.
The existing production implementation required no F03 change.

## Retained runs

| Execution | OpenAI | Anthropic | Total |
| --- | --- | --- | --- |
| First actual `go test -race` | 8.40 s | 8.37 s | 16.77 s |
| Repeat with a retained binary identity | 8.41 s | 8.46 s | 16.87 s |

The first execution remains separately identified:
[OpenAI raw](../benchmarks/results/model-stream-interrupted-openai-20260911T202343.883942111Z/report.json)
and [Anthropic raw](../benchmarks/results/model-stream-interrupted-anthropic-20260911T202352.283005259Z/report.json).
Its selected source snapshots are retained, but the temporary `go test` binary's
identity was not captured. The repeat added binary provenance without changing
the workload or acceptance assertions. No failed experiment has been discarded
or relabeled; all four actual cases passed.

The repeat's primary records are:

- OpenAI: [raw report](../benchmarks/results/model-stream-interrupted-openai-20260911T202817.952133210Z/report.json),
  [strengthened audit](../benchmarks/results/model-stream-interrupted-openai-20260911T202817.952133210Z/strengthened-audit.json),
  [source and compilation identity](../benchmarks/results/model-stream-interrupted-openai-20260911T202817.952133210Z/compilation-provenance.json).
- Anthropic: [raw report](../benchmarks/results/model-stream-interrupted-anthropic-20260911T202826.363552405Z/report.json),
  [strengthened audit](../benchmarks/results/model-stream-interrupted-anthropic-20260911T202826.363552405Z/strengthened-audit.json),
  [source and compilation identity](../benchmarks/results/model-stream-interrupted-anthropic-20260911T202826.363552405Z/compilation-provenance.json).
- Both cases: [actual execution log](../benchmarks/results/model-stream-interrupted-openai-20260911T202817.952133210Z/execution.log).

All reports are working-tree executions anchored at
`224a6b64ce0cc41c564ce2c9bfd2e74b4b9ccc0a`, not exact-commit CI results. The repeat
uses a race-instrumented binary with SHA-256
`041f99555f829dfb91b6403d40783a4319f5cab9bc6f2738f9fe2fe99fbeecc8`.
Twenty selected source hashes were stable before/after compilation and match
both reports' pre-execution snapshots. The build ID is retained beside the raw
reports. Executable bytes remain local at `/tmp/forge-f03-native-stream-evidence.test`
and are not checked into the repository.

## What the combined path demonstrates

The [test](../benchmarks/model_stream_fault_test.go) submits and claims one run,
then calls the production Driver. Prepare and BuildContext publish actual local
artifact objects and PostgreSQL metadata. The Driver commits its priced attempt
and quota dispatch marker before making the native HTTP request. The adapters'
SDK retries are disabled by production code; both the server records and
transparent provider observer count exactly one request per case.

Each server writes native SSE events and flushes them, then hijacks and closes
the chunked HTTP connection before its terminating chunk. This is an actual
transport truncation. The stream contains one ended `read_file` item and a second
`apply_patch` item whose JSON ends mid-string, plus the retained provisional
text. Anthropic additionally starts with nonfinal usage information; the adapter
retains an observed input lower bound of 12 while `usage.final` remains false.
Those counters do not become a final bill.

“Incomplete attempt” is represented by persisted `status=failed` and
`error_code=stream_interrupted`; the database has no separate `incomplete` enum.
`raw_ref` and final usage are null. The adapter returns no executable tools or
native continuation and emits no completed marker. PostgreSQL retains the text
as `text.delta` with `provisional=true`, then records one `model.attempt_failed`
and the durable retry delay. There is no `model_response` artifact, no effect
row and no runner operation other than one Prepare. The model's permitted retry
is not dispatched during this experiment.

The frozen synthetic price reserves 8,192 input-bound tokens plus 1,024 output
tokens, and 10,240 microUSD. Every one of five PostgreSQL snapshots retains the
same attempt and bound `unknown` reservation with null actual usage/cost and no
settlement. No balance is treated as a real invoice.

After the recorded retry delay, the test makes a second Claim and deliberately
starts no Driver or heartbeat. That epoch-2 lease expires naturally; it is a
lease observation, not a second model sample. Database-clock waits and a failed
LeaseProof establish expiry before the independent provider-request deadline.
Calling request-slot expiry at that point returns zero and preserves both the
active provider slot and its monetary/token reservation. At the natural request
deadline, slot expiry returns one, then zero on replay. The provider slot becomes
free while 9,216 tokens and 10,240 microUSD remain reserved. Attempting zero-cost
`AbandonBeforeDispatch` returns `ErrAlreadyDispatched` and changes no obligation.

| Repeat case | Natural epoch-2 lease expiry (UTC) | Request deadline (UTC) | Request-slot expiry counts: early / due / replay |
| --- | --- | --- | --- |
| OpenAI | 20:28:21.610106 | 20:28:26.295974 | 0 / 1 / 0 |
| Anthropic | 20:28:29.943998 | 20:28:34.753048 | 0 / 1 / 0 |

The helper's migrations and cleanup affect only each newly created private
schema in the selected loopback PostgreSQL 17.11 fixture. `fsync` and
`synchronous_commit` are on. The privileged test role models a worker's Store
access; this experiment makes no API/RLS authorization claim. Temporary artifacts
and the private schema are removed after observation, without pretending the
unknown obligation was reconciled or refunded.

## Independent audit and reproduction

The [offline auditor](../benchmarks/audit_model_stream_fault.py) checks all five
snapshots per case, native requests/frames, adapter sequence and identity,
provisional text persistence, exact reservation identity, natural timestamp
windows, unchanged attempts, all effect counts and 20 source snapshots. Durable
events are contiguous: 10 after Driver deferral, then 11 after the second Claim,
unchanged through the final three snapshots. The auditor also rejects deliberately
altered copies that zero the reserved expense, invent a second model call, or
insert an effect. All four reports passed this independent audit.

Independent review found three groups of missing audit bindings: native
provider/attempt identity, wire text/tool arguments versus observed content,
and reported lease expiry versus actual PostgreSQL and reducer state. Sixteen
altered report subtests were incorrectly accepted before correction. The
[review bundle](../benchmarks/results/model-stream-audit-review-20260911/manifest.json)
retains that failed negative-test output, corrected auditor/test snapshots,
passing regressions and all four recomputed results. The original raw reports
and earlier `independent-audit.json` files remain unchanged; the corrected
results are separately named `strengthened-audit.json`. Nine F03 regression
tests now pass, alongside the six existing application audit tests. The CLI
prints to stdout by default; explicit output paths must not already exist.

The network-free negative ledger oracle also passed in the same race-enabled
binary; its [raw output](../benchmarks/results/model-stream-oracle-race-20260911.log)
is retained. `go vet ./benchmarks` passed separately. Commands, prerequisites and
the frozen protocol are in [MODEL_STREAM_FAULTS.md](../benchmarks/MODEL_STREAM_FAULTS.md).

These observations close the chosen F03 stream/ledger boundary. They do not
establish crash recovery, a successful retried repair, provider billing
reconciliation, container isolation, or real-model quality.
