# HTTP acceptance fault experiments

## F01: response lost after successful submission commit

Run only against the disposable loopback PostgreSQL test service:

```sh
FORGE_RUN_API_FAULT=1 \
GOCACHE=/tmp/forge-runtime-gocache GOPROXY=off \
go test ./benchmarks -run '^TestSubmissionResponseLossEvidence$' -count=1 -timeout=1m -v
```

`FORGE_TEST_DATABASE_URL` must identify the explicitly selected local `/forge`
database. The normal test helper creates an isolated schema; the experiment
creates a non-owner, NOSUPERUSER, NOBYPASSRLS, NOINHERIT API role whose CRUD grants
are limited to that schema. No shared tenant data, runner or model is used.
Expected execution is a few seconds.

The original request goes through two actual loopback HTTP/TCP legs: client →
fixture reverse proxy → the current production HTTP API handler. The proxy
matches one exact route and idempotency key. After receiving the upstream's
202 acceptance response, it extracts the run ID and independently queries
PostgreSQL. It must observe exactly one queued run, one `run.created` event,
one bound idempotency record and no model/effect/runner-allocation rows before
closing the client TCP connection. The proxy neither writes response headers nor
flushes an HTTP body to the client. The upstream response is retained as a
fixture observation, not falsely described as a response received by the client.

The initial client must observe a transport error with no HTTP response. Its
explicit retry sends the original body and key, without using the proxy's hidden
run ID. Both proxy and client transports use fresh connections to prevent hidden
replay on a previously used broken connection. Per-hop counters must show exactly
one upstream request before the client observes the failure. The retry must
return 202, `reused:true`, the original run ID and the same run Location.

The frozen acceptance also checks that a different body with the same key yields
409 and that the same token cannot access an unrelated tenant (403). Snapshots
after the first client failure, after retry and after those negative controls
must still show exactly one run/event/key with unchanged bindings and no execution
records. The report retains timestamps, synthetic request body/hash/key, responses
or their absence, hidden proxy observations and raw PG creation/idempotency rows.
Bearer tokens and database connection strings are excluded. No production
interface, proxy setting or retry policy is changed.

Raw results and matching source snapshots are written to
`benchmarks/results/local/api-submit-response-loss-<timestamp>/`. A failed report
must be retained and cannot be relabeled as successful because one subset passed.

Two network-free `TestSubmissionDropFixture*` regressions use `net.Pipe` to check
that the injector is key-scoped, happens once, writes zero HTTP bytes, and never
manufactures the unknown transport outcome when durable proof fails:

```sh
GOCACHE=/tmp/forge-runtime-gocache GOPROXY=off \
go test -race ./benchmarks -run '^TestSubmissionDropFixture' -count=1 -v
```
