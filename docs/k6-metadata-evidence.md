# E33 — Actual k6 metadata workload, 2026-09-11

The corrected k6 workload passes **1,000 actual HTTP requests at a configured arrival rate of 50/s over 20 seconds**: 900 reads return 200, 100 admissions return 202, all checks pass, and no iterations are dropped. The independently recomputed `http_req_duration` values are **p95 6.74039745 ms** and **p99 11.37764899 ms**. This is one local metadata experiment with real TCP, the authenticated API handler and a private PostgreSQL schema. No worker, runner, model or task container executes.

The earlier execution failed its write-count requirement and remains unchanged. Both raw executions and the [independent audit](../benchmarks/results/k6-metadata-20260911T211355Z/independent-audit.json) are retained; the later PASS does not reclassify the first result.

## Workload and measured boundary

The [Go harness](../benchmarks/k6_test.go) reuses the [HTTP fixture](../benchmarks/http_test.go): one seeded run, a temporary token, real authentication and a nonowner PostgreSQL role with RLS. The API pool has at most 16 connections. The handler runs in `httptest` without telemetry, so this does not measure a deployed API process with every operational middleware enabled.

There are 100 successful warm-up reads before starting k6; they are excluded from its measured samples. The [script](../benchmarks/k6/metadata.js) uses 16 preallocated/maximum VUs, a 20-second scenario, a five-second completion grace and a three-second request timeout. The index determines the mix: every tenth request is an admission with its own idempotency key. All others read the seeded run. This is deterministic request selection; no randomized data/seed is used.

Grafana defines `constant-arrival-rate` in terms of **iteration starts**, scheduled independently of response completion while VUs are available. It does not make an iteration equivalent to an HTTP request; that depends on the script. The configured duration also excludes the completion grace. [Official executor documentation, checked 2026-09-11](https://grafana.com/docs/k6/latest/using-k6/scenarios/executors/constant-arrival-rate/).

## Preserved failure and correction

| Observation | First execution — failed | Corrected execution — passed |
| --- | --- | --- |
| Raw directory | [21:12:32 UTC](../benchmarks/results/k6-metadata-20260911T211232Z/report.json) | [21:13:55 UTC](../benchmarks/results/k6-metadata-20260911T211355Z/report.json) |
| k6 iterations | 1,001 | 1,001 |
| Actual HTTP requests | 1,001 | 1,000 |
| Reads / admissions | 900 / 101 | 900 / 100 |
| Expected HTTP statuses | 1,001 / 1,001 | 1,000 / 1,000 |
| Failed requests / checks / dropped iterations | 0 / 0 / 0 | 0 / 0 / 0 |
| PG runs / keys / created events, including seed | 102 / 102 / 102 | 101 / 101 / 101 |
| Model attempts / effects | 0 / 0 | 0 / 0 |
| HTTP p95 | 6.657663 ms | 6.74039745 ms |
| HTTP p99 | 13.222153 ms | 11.37764899 ms |
| Harness elapsed around k6 and result capture | 20.137580099 s | 20.123616459 s |
| Failed threshold | `metadata_admissions: count==100` | None |

The first script sent a request for the extra boundary iteration, index 1,000. Because it is divisible by ten, this produced the 101st admission. Its [execution log](../benchmarks/results/k6-metadata-20260911T211232Z/execution.log), [summary](../benchmarks/results/k6-metadata-20260911T211232Z/summary.json), [every raw point](../benchmarks/results/k6-metadata-20260911T211232Z/metrics.jsonl) and [original script](../benchmarks/results/k6-metadata-20260911T211232Z/metadata.js.txt) show the failed workload count. This is an observed fixture boundary behavior, not a claim that every k6 scenario always emits an extra iteration.

The correction limits dispatch before HTTP: `index >= 1000` returns immediately. It also adds the exact `http_reqs: count==1000` threshold. The rate, duration, VUs, 90:10 mix and latency/error targets remain fixed. The extra iteration is still visible in the corrected [raw points](../benchmarks/results/k6-metadata-20260911T211355Z/metrics.jsonl); it simply has no HTTP sample. No samples were deleted to obtain the passing count.

The [corrected summary](../benchmarks/results/k6-metadata-20260911T211355Z/summary.json) uses the legacy k6 threshold representation: `false` means that threshold **did not fail**. The first summary contains `true` for the admission-count threshold and did not yet define the later HTTP-count threshold. The readable logs and actual sample counts agree with these meanings. The successful run satisfies p95 <200 ms and p99 <1,000 ms for the metric defined below.

## Timing definition and independent checks

`http_req_duration` covers sending the request, waiting for the response and receiving its body; initial DNS, connection and blocked time are outside that metric. Raw HTTP timestamps mark request completion. These values therefore must not be described as connection-inclusive client latency, whole-iteration duration or end-to-end task completion. [Official built-in metric definitions](https://grafana.com/docs/k6/latest/using-k6/metrics/reference/).

The audit reads every JSONL point, matches the 1,000 request and duration identities/timestamps, verifies the 900 GET/200 and 100 POST/202 tag combinations, and checks every failure/check value. It recomputes percentiles by linear interpolation at sorted zero-based position `(N-1) × percentile`, matching the k6 summary. Successful-run minimum, median and maximum are respectively **0.553928**, **1.3388165** and **20.873848 ms**. This interpolation differs from nearest-rank reports elsewhere in the repository.

The independent review also checks the script hash, the captured Go harness/HTTP source against their current bytes, reported production source hashes and the installed k6 executable hash. It finds no P1/P2 issue. A credential-pattern scan finds no literal platform token, bearer credential or credentialed PostgreSQL URL in either retained directory. The actual token is supplied only through a deliberately constructed child environment, not argv or script output; the review does not claim universal secret detection.

## Environment and provenance limits

| Item | Recorded identity |
| --- | --- |
| Load tool | `k6 v2.2.0 (commit/00a9a1b7f5, go1.26.5, linux/amd64)` |
| k6 executable SHA-256 | `9fced9ff140d73bf3d69b3cde2493adce3807b35ab210a5cb0771045d0ad5a29` |
| Corrected JavaScript SHA-256 | `409c531abc5718984e0756919bfd3c5b165827f867de817a8237da6a32eb6bd1` |
| Recorded Go test executable SHA-256 | `8b43339f0e3bc649350a9a39875ce9dffbd76c67dfd5039e637460a08c075b7b` |
| Go / host | Go 1.26.8; Linux amd64; Intel i9-13900K; 32 logical CPUs; GOMAXPROCS 32; reported host memory `129209084 kB` |
| API database boundary | Private schema; authenticated handler; nonowner RLS; pool max 16 |

The successful directory preserves [k6 harness source](../benchmarks/results/k6-metadata-20260911T211355Z/k6_test.go.txt), [HTTP fixture source](../benchmarks/results/k6-metadata-20260911T211355Z/http_test.go.txt), [script source](../benchmarks/results/k6-metadata-20260911T211355Z/metadata.js.txt), exact k6 arguments and hashes. These snapshots were copied after execution and matched during review. The temporary Go executable itself is unavailable for independent reconstruction, and the first execution has less Go build provenance. **Neither record supplies a complete clean-build attestation.**

PG totals are retained outputs of actual fixture queries/assertions, not full historical table exports. This experiment does not include a complete PostgreSQL configuration inventory, GC/pprof capture, an otherwise idle-host guarantee, sustained-load distribution or release-wide performance certification. It supplements the earlier E08 metadata measurement with actual k6 evidence; it does not replace that historical report or close every S17.6 provenance requirement.

The [reproduction instructions](../benchmarks/k6/README.md) use the private fixture and require a fresh output directory. No production files, tests, original measurements or public acceptance rows were changed while writing this record.
