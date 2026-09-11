# k6 metadata acceptance

[`metadata.js`](metadata.js) measures authenticated metadata reads and run admission through actual TCP and a real private PostgreSQL schema. Run it through [`TestK6MetadataEvidence`](../k6_test.go), which provisions its own API fixture, nonowner RLS role, seed run and temporary token. It never invokes a worker, model, runner or task container. The [E33 record](../../docs/k6-metadata-evidence.md) preserves the first count failure, corrected PASS and independent audit.

## Reproduce

Requirements: the repository's pinned Go toolchain, **k6 v2.2.0 on PATH**, and `FORGE_TEST_DATABASE_URL` already set to the dedicated loopback PostgreSQL `/forge` database. The test helper rejects other hosts/databases and creates/migrates a uniquely named schema. Its fixture administrator needs permission to create that schema and a temporary nonlogin role; API queries then use the nonowner role. Do not use the running platform's worker credentials.

From the repository root:

```sh
FORGE_RUN_K6_BENCHMARK=1 \
GOCACHE=/tmp/forge-runtime-gocache GOPROXY=off \
  go test ./benchmarks -run '^TestK6MetadataEvidence$' \
  -count=1 -timeout=60s -v
```

The exact test selection matters: its wrapper enables and calls the HTTP fixture once. Without the explicit opt-in flag or selected database, the corresponding integration test skips. The harness checks the k6 version before dispatch. It uses built-in k6 modules, an empty configuration file, no usage reporting and a constructed child environment containing only the fixture connection values and basic process settings. No secret belongs in JavaScript, shell arguments or published reports.

By default, the new directory is `benchmarks/results/k6-metadata-<UTC timestamp>`. To select another location, set `FORGE_K6_EVIDENCE_DIR` to a **new absolute directory** whose parent already exists. The harness creates the directory itself and refuses an existing one, preserving prior evidence. Do not pre-create or reuse the leaf directory. Source snapshots, `report.json`, `summary.json`, `metrics.jsonl`, `execution.log`, the empty config and tool version are saved there even when the final workload assertions fail. A setup failure before result capture can leave an incomplete directory; it is not a completed measurement.

## Fixed workload and acceptance

The Go fixture performs 100 warm-up reads outside measurement. k6 uses `constant-arrival-rate`, 50 iteration starts per second for 20 seconds, 16 preallocated/maximum VUs, five seconds of completion grace and three-second HTTP timeouts. The executor schedules starts independently of response completion; script logic determines the HTTP count. [Official constant-arrival-rate documentation](https://grafana.com/docs/k6/latest/using-k6/scenarios/executors/constant-arrival-rate/).

Only iteration indices 0–999 dispatch HTTP: multiples of ten admit a run with a distinct idempotency key, and the other indices read the seed. This gives exactly 100 writes and 900 reads without randomized selection. Any extra boundary iteration returns before HTTP and remains visible in `iterations`; it is not a dropped request or a discarded raw sample.

Passing requires exactly 1,000 HTTP requests, 900 reads, 100 admissions, expected statuses 200/202, zero request/check failures and zero dropped iterations. `http_req_duration` must have p95 <200 ms and p99 <1,000 ms. PostgreSQL must contain exactly 101 runs, idempotency keys and created events including the seed, with no model attempts or effects.

Use `http_reqs`, not `iterations`, as the HTTP denominator. The k6 duration metric excludes initial connection/blocked time; it is not model or task latency. Its exported percentile values use interpolation, and legacy summary threshold booleans identify **failure**, so `false` is not a failed threshold. Preserve every JSONL point and the actual exit/count assertions when assessing a run. E33 explains the measured values and the limitations of the retained build/database provenance.
