# F03: interrupted native streaming and durable expense

This opt-in acceptance joins the production native SDK adapters, application
Driver, artifacts, PostgreSQL attempts/events and quota ledger. It sends no
requests to a paid service. Two cases use real loopback HTTP: OpenAI Responses
and Anthropic Messages, each served by a local native-protocol fixture.

```sh
FORGE_RUN_MODEL_STREAM_FAULT=1 \
GOCACHE=/tmp/forge-runtime-gocache GOPROXY=off \
go test -race ./benchmarks -run '^TestNativeStreamInterruptedLedgerEvidence$' -count=1 -timeout=90s -v
```

`FORGE_TEST_DATABASE_URL` must select the disposable `127.0.0.1` `/forge`
database. The existing helper creates and drops only its own private schema.
Do not point it at any shared production service. The expected two-case runtime
is about 20 seconds, including natural database-clock deadline waits. No worker
environment, provider secret, live runner or Docker service is needed.

Each fixture stream emits provisional text, a fully ended `read_file` item and
another `apply_patch` item whose JSON arguments stop mid-string. It then closes
the actual TCP socket after flushing those events, without the final HTTP chunk,
native turn completion or final usage. Anthropic's initial usage is partial
evidence only. A transparent provider observer records and forwards the adapter's
events and returned result without changing the Driver's input or behavior.

The Driver starts from a real Submit/Claim and performs Prepare/BuildContext,
reserves the frozen synthetic price and dispatches through the native SDK. The
runner fixture allows only Prepare; every other runner operation is recorded and
rejected. `HasTarget=false` intentionally avoids an initial verification effect.
There are no successful simulated command/file operations, and this experiment
does not establish Docker isolation or the runner's permission checks.

The frozen assertions require:

- One native HTTP request and one incomplete attempt. PostgreSQL persists status
  `failed` and `error_code=stream_interrupted`, with null completed `raw_ref` and
  usage. “Incomplete” describes the absence of a complete model turn, not a
  separate database enum.
- Provisional adapter text/tool deltas and one failed event; no completed event,
  returned executable tool calls, native continuation, final usage, published
  `model_response` or effects. Runner calls consist only of one Prepare.
- One dispatched `unknown` reservation retaining 9,216 tokens and 10,240 microUSD
  under `synthetic-f03-v1`. Those are conservative synthetic reservations, not
  measured usage or external invoices. Actual usage/cost and settlement remain
  absent.
- After the Driver durably defers for its recorded retry time, the fixture waits
  using PostgreSQL time and makes a second Claim. It does **not** start another
  Driver, heartbeat or model call. That epoch-2 lease expires naturally before
  the request deadline. LeaseProof rejects it; request-slot expiry returns zero;
  tokens/cost and the active provider slot remain reserved.
- At the attempt's natural eight-second request deadline, request-slot expiry
  returns one and then zero on replay. Only request concurrency becomes zero;
  the unknown token/cost obligation remains. A zero-cost pre-dispatch abandonment
  is rejected because the request was already dispatched.

Five raw ledger snapshots retain the single attempt, reservation, provider quota,
run, events, artifacts and zero effects. The report also retains synthetic native
request bodies, every sent event, TCP close timestamps, observed adapter output,
runner counts, natural lease/deadline timestamps, PostgreSQL settings, machine
details and pre-execution source hashes/snapshots. No bearer headers or DSNs are
retained. Reports are written even when a test fails, and failed reports must not
be overwritten by a later successful repeat.

The fixture keeps the model call count at one to isolate retention. It does not
claim a worker crash, resumed repair or eventual billing reconciliation. Cleanup
drops only the private schema and temporary artifact directory; the unknown
obligation is not made to disappear by pretending it was settled.

The network-free negative oracle checks that releasing unknown monetary/token
obligations is rejected even after the concurrency slot legitimately expires:

```sh
GOCACHE=/tmp/forge-runtime-gocache GOPROXY=off \
go test -race ./benchmarks -run '^TestInterruptedLedgerOracle' -count=1 -v
```
