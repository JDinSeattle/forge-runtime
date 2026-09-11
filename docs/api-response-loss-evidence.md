# E24 — Acceptance response lost after commit

On 2026-09-11, the real HTTP handler and PostgreSQL submission transaction passed
one explicit after-commit response-loss experiment. The first client received
EOF and no HTTP response. Its explicit retry of the same body and idempotency key
returned `202`, `reused:true`, the original run ID and the original Location.
All four database snapshots contained the same single queued run, single
`run.created` event and single bound idempotency record.

The [unaltered raw report](../benchmarks/results/api-submit-response-loss-20260911T200419.808266740Z/report.json),
[execution log](../benchmarks/results/api-submit-response-loss-20260911T200419.808266740Z/execution.log)
and [independent audit](../benchmarks/results/api-submit-response-loss-20260911T200419.808266740Z/independent-audit.json)
are retained together. The test completed in 0.26 seconds. Its base commit was
`0a2cb15e1de83de509aaf0afabd90bb669c3cf45` with an explicitly dirty working tree;
the report identifies nine source files by SHA-256 and retains matching
[source snapshots](../benchmarks/results/api-submit-response-loss-20260911T200419.808266740Z/source-snapshot).
This execution is not a clean-commit or CI claim.

## Injection and authority boundaries

The [opt-in test](../benchmarks/api_fault_test.go) uses two real loopback TCP
connections: client → fixture reverse proxy → the current production HTTP
handler. A disposable private PostgreSQL schema contains synthetic tenants,
principals and the project. The handler uses a non-owner API role with
`NOSUPERUSER`, `NOBYPASSRLS`, `NOINHERIT` and grants limited to that schema.
PostgreSQL 17.11 reports `fsync=on` and `synchronous_commit=on`; the API and owner
connection pools each allow 16 connections.

The proxy injects the cut once, for one exact submission route and key. It first
receives the upstream `202 reused:false` response, extracts the new run ID and
independently checks its committed PostgreSQL rows. Only after that proof does it
hijack and close the client connection without writing HTTP response bytes.
The hidden upstream response and proof remain fixture observations; the client
does not receive either. The retry uses its original body and key, without the
hidden run ID. Fresh client and proxy connections prevent hidden replay on a
previously used broken connection. Counters record exactly one upstream call
before the initial failure and four calls across the four explicit requests.

No production API, proxy configuration or retry policy was changed. No worker,
runner, executor or model was started. The run remains queued; model-attempt,
effect and runner-allocation counts are zero. This establishes F01's selected
after-commit/before-response boundary, not general packet-loss rates or worker
recovery. Bearer tokens and database connection strings are excluded from the
retained report.

## Observed sequence and independent checks

| Observation | UTC time or result |
| --- | --- |
| Initial client request started | 20:04:19.978756348 |
| Proxy observed upstream acceptance | 20:04:20.001184658 |
| Independent PG snapshot captured | 20:04:20.012292 |
| Proof returned; proxy recorded connection close | 20:04:20.012832254; 20:04:20.012899423 |
| Initial client observed EOF | 20:04:20.012966845; no HTTP response |
| Explicit original-body/key retry | 20:04:20.013395350–20:04:20.015860936; 202, reused, same run |
| Same key with changed body | 409; no extra creation |
| Original token with unrelated tenant | 403; no extra creation |
| Final PG snapshot captured | 20:04:20.019090 |

The independently executed [offline auditor](../benchmarks/audit_api_fault.py)
recomputed request hashes, checked all 15 timestamp-order constraints and
compared all four raw PostgreSQL snapshots: before the socket cut, after the
client failure, after retry, and after the two negative controls. Their run,
event and key records are identical. The run is queued at version 1, cursor 1,
epoch 0 with no runner; its sole event is sequence 1 `run.created`. The key keeps
the same tenant, principal, route, resource and request-hash binding.

The audit also checked the absent initial response, the retry's run ID and
Location, the 409/403 controls, the call counters, all nine source snapshot
hashes, and zero model/effect/allocation rows. The raw report SHA-256 is
`8251d5c0a9e441b1643231fa8fad4a4546aa3279589aa9b755c02149756259b2`.
The initial failure took 34.210497 ms and the retry took 2.465586 ms; these are
single observations, not latency SLO measurements.

Two network-free injector regressions additionally passed under `-race`: they
check exact key scoping, at-most-once injection, zero response bytes before EOF,
and refusal to manufacture response loss when durable proof fails. Their result
was observed in agent tool output; no separate raw unit-test log was retained.
The actual network experiment has its own retained execution log above.

Reproduction commands and prerequisites are in
[the HTTP fault experiment protocol](../benchmarks/API_FAULTS.md). The retained
report can be re-audited without network or database access:

```sh
python3 benchmarks/audit_api_fault.py benchmarks/results/api-submit-response-loss-20260911T200419.808266740Z
```
