# E39 — Fixed-commit control and simulated workload repeats

Three bounded experiments pass from an independent local checkout of
`a9208e79879d29373eb3964e098eff286aa6de82`: scheduler competition, authenticated
HTTP metadata, and 1,000 simulated terminal tasks. They use one explicitly built
ordinary Go test executable. Its SHA-256 is
`1a63a717cbef52ca2b5608adba9f425323070d845fa760abbe891c2cb06a8a8a`.
These new observations supply the missing build identity for the corresponding
older E05/E08/E09 workload classes. Historical results keep their original
numbers, limited source maps and execution identities.

[Build/checkout identity](../benchmarks/results/stamped-benchmarks-20260911T213726Z/identity.json),
[exact invocations](../benchmarks/results/stamped-benchmarks-20260911T213726Z/executions.json),
[unchanged-source result](../benchmarks/results/stamped-benchmarks-20260911T213726Z/completion.json),
[owner recomputation](../benchmarks/results/stamped-benchmarks-20260911T213726Z/recompute.json)
and [manifest](../benchmarks/results/stamped-benchmarks-20260911T213726Z/manifest.json)
are retained. No current working-tree feature is included in this executable.
The a9208e7 SSE CI failure and its later fix are recorded independently in E38;
these workloads do not exercise SSE reset retirement.

| Workload | Observed result | Measured latency |
| --- | --- | --- |
| 1,000 submissions plus 100 idempotent repeats; 10 tenants, 8 submitters | Exactly 1,000 accepted runs | 1,100 samples: p95 6.170 ms; p99 9.602 ms |
| Two competing scheduler goroutines | 1,000 first-epoch claims, worker counts 516/484; no event gaps | Claim-call p95 10.757 ms; p99 13.586 ms |
| Warm metadata at 50 requests/s for 20 s | 900 GET/200 and 100 POST/202; zero errors; 101 stored runs including seed | 1,000 client request samples: p95 4.755 ms; p99 8.582 ms |
| Two simulated controllers, 1,000 terminal tasks | 1,000 unique simulated effects, 2,000 FakeProvider calls, 1,000 duplicate confirmations rejected, all tasks complete | Per-task simulated execution p95 195.923 ms; p99 216.382 ms |

The [scheduler report](../benchmarks/results/stamped-benchmarks-20260911T213726Z/raw/scheduler-20260911T213730.861721840Z.json),
[HTTP report](../benchmarks/results/stamped-benchmarks-20260911T213726Z/raw/http-20260911T213759Z.json)
and [terminal report](../benchmarks/results/stamped-benchmarks-20260911T213726Z/raw/terminal-simulation-20260911T213759.470860792Z.json)
retain every sample. Percentiles use nearest rank, `ceil(q*N)-1` in a sorted
zero-based array. The owner recomputation checks these numbers directly; this
differs from E33 k6's interpolated metric. The HTTP timer covers the client
request through response-body consumption and excludes waiting for its offered
arrival. It is not full task execution.

Scheduler claims intentionally retain 1,000 slots until their private schema is
dropped; this experiment does not reach task terminal states. The separate
terminal workload records 21,000 events, 12,000 snapshots, 1,000 released
allocations, no unsettled reservations, and zero final tenant/runner/provider
active or reserved counters. Its 6,000 microUSD and 96,000 tokens are **synthetic
ledger values**. The controller and executor are explicit fixtures; no
application Driver, real command, Docker task or paid model is benchmarked.

An [independent audit](../benchmarks/results/stamped-benchmarks-20260911T213726Z/independent-audit.json)
checks every source hash against the exact commit and clean clone, the retained
binary against all three executions, raw counts/quantiles and every original
manifest entry. It does not independently rebuild the executable. HTTP database
settings are corroborated by the adjacent workloads using the same launcher and
database; the HTTP report itself has no separate settings snapshot.

## Reproducibility and limits

The local clone checks out the exact commit with an initially clean working
tree. All 262 recorded source/build inputs remain byte-identical from build
through all three executions, and tracked Git status remains clean. The full
Git commit additionally fixes input files outside the selected hash map. Test
binary hashes match before and after execution. The [retained launcher](../benchmarks/results/stamped-benchmarks-20260911T213726Z/launcher.py.txt)
shows the clone, compile and execution steps without storing database secrets.
Private schemas are created and dropped in the explicitly selected loopback
database; live business tables and services are not modified.

The host is Linux/amd64 on an i9-13900K with 32 logical CPUs and approximately
129,209,084 kB of host memory. Go 1.26.8 runs with **GOMAXPROCS=2, GOGC=100**;
PostgreSQL 17.11 reports 128 MB shared buffers, fsync and synchronous commit on,
with a 16-connection fixture pool. Workload shapes and request mix are
deterministic; random values are opaque identities, so there is no workload
random seed. This is a shared development machine and database: unrelated
checks may run concurrently. It is not a dedicated-host capacity study or SLO.

Each class records a CPU profile, a heap profile and GC trace output. All six
profiles have been symbolized with the retained executable. The generated heap
top listings use pprof's default allocation view; they are not an in-use heap
plateau or a long-term leak test. E16 retains that separate finite stability
experiment. Cached Go dependencies are used, so this is not a fresh-machine
installation test.

The [first clone attempt](../benchmarks/results/stamped-benchmarks-20260911T213659Z/failure.json)
fails before build or database access because `/tmp` cannot hardlink Git objects
across filesystems. The corrected invocation uses `--no-hardlinks`, preserving
the original failure and leaving all measurement parameters unchanged.

P06 real execution measurements and P07 paid model outcomes remain separate.
An exact build identity makes these finite observations inspectable; it does
not turn simulated work or earlier incomplete evidence into production results.
