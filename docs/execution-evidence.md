# E36 — Real runner command measurements

Status: the second real runner attempt passed all 20 process commands on 2026-09-11. Ten normal commands succeeded, ten commands with child processes were cancelled, and both owned workspaces were stopped, snapshotted and released. Independent Python recomputation verified the retained observations, 40 typed operation records and all 44 artifact hashes. The first attempt and its network inventory assertion failure remain preserved below.

The [harness](../internal/runnerclient/execution_benchmark_test.go) uses the existing operator-provisioned mapped runner through its typed gRPC Unix socket. It allocates exactly two uniquely named workspaces in the existing four-slot pool, with the existing `clamp` source and digest-pinned `python-clamp` profile. At least two slots must be free. It does not start or stop a service, mount or unmount storage, change configuration, issue PostgreSQL requests, invoke a model, or call a paid API.

## Fixed workload and acceptance

The workload has five normal rounds followed by five cancellation rounds. Each round starts two commands concurrently, one per workspace. There are 10 normal and 10 cancelled process commands, plus one typed `read_file` operation after each process command. The Python program is identical across commands; only the workspace-specific marker and mode differ. Both workspaces write the same relative filename, `p06-shared-name.txt`, and compare its contents to their own marker every 20 ms. Normal commands run these checks for one second and exit. Cancellation commands create a child process, have both parent and child ignore SIGTERM, and run for up to 45 seconds unless cancelled first. Each operation has a 55-second deadline; direct RPC waits have a 15-second bound. The experiment context is bounded to four minutes, followed by at most 30 seconds of cleanup per owned workspace.

For every round the evidence must establish distinct writable mount sources and overlapping actual container lifetimes. In cancellation rounds, A is cancelled only after both processes and their children have produced readiness output. The harness verifies A's container is stopped and its dedicated cgroup has no remaining tasks, observes B still running, and then cancels B. Each final `read_file` result must contain its own marker and SHA-256. An isolation-failure marker, missing log boundary, unknown operation, unexpected termination, or unmatched artifact proof fails the experiment. A passing run must have all 20 admitted commands, all final statuses as expected, and both owned workspaces safely released.

The fixed program reads actual UID/GID, capabilities, no-new-privileges, cgroup memory/PID/CPU limits, root filesystem read-only flag, bounded tmpfs size, network interfaces and routes, and Docker-socket absence. On the tested Linux platform, bounded `SIOCGIFCONF` and `SIOCGIFFLAGS` reads collect every IPv4 address, including aliases, and flags for each interface; sysfs supplies operational state. The original IPv4/IPv6 route tables and their parsed entries are retained. At least one enabled loopback interface is required. Every other interface must be down, have `IFF_UP` clear and have no IPv4 address. An active, non-reject route must be restricted to a loopback destination on a loopback interface with an unspecified gateway/next hop. Device names are not an allowlist, and Docker `NetworkMode=none` remains a separate required readback. The harness also retains whitelisted actual Docker settings and validates the pinned image, resource settings and independent writable mounts. This readback does not repeat the earlier PID exhaustion, OOM and ENOSPC stress tests, and does not establish CPU fairness or arbitrary hostile-code isolation. The earlier [real Docker limit evidence](../benchmarks/results/real-docker-limits-20260908.log) remains separately identified.

## Timing definitions

Every timing point has an RFC3339 UTC timestamp and a duration since the harness's monotonic start. Latency distributions use those monotonic durations; UTC timestamps support correlation. Docker `StartedAt`/`FinishedAt` are separate daemon clock observations used to check process-lifetime overlap. They are not subtracted from harness timestamps.

| Reported measure | Exact interval and limitation |
| --- | --- |
| Direct Start acknowledgement | Immediately before generated typed `StartOperation` RPC to its successful response; calls `c.rpc` directly, avoiding the domain client's possible post-error Inspect fallback. This confirms durable acceptance, not container startup. |
| First observed running | Start request to the end of the first Docker inspection reporting running; includes CLI overhead and polling delay. |
| First active daemon log | Start request to the first read-only `docker logs` call returning the READY line, bracketed by running container inspections. Child readiness is tracked separately. This is an operator observation, not a platform log-stream RPC. |
| First typed Inspect log | Start request to the first typed `InspectOperation` response containing the READY line in a terminal operation result. The current protocol has no live log RPC. This includes command execution and receipt publication; cancelled samples also include deliberate observation and cancellation wait. |
| Cancel RPC acknowledgement | Immediately before `CancelOperation` to its returned terminal cancelled result. |
| First observed stopped after cancel | Cancel request to the first subsequent Docker observation of a stopped container; an upper bound including the RPC, typed Inspect and CLI overhead. Separate cgroup proof records zero remaining controlled tasks. |

Observers pause 50 ms between polling rounds; this is **not** a promise of 50 ms measurement resolution, because RPC and CLI execution add time. Samples are split by normal/cancel mode. Min, p50, p95 and max use linear interpolation at `q × (n−1)` over sorted raw durations. Each mode contains only 10 process samples. No control-plane latency target is borrowed, and neither the test package duration nor a terminal-log duration is represented as launch latency.

## Evidence and cleanup boundaries

The new output leaf contains `report.json`, the exact `command.py`, selected source snapshots with hashes, the retained test executable, the actual running runner executable, whitelisted Docker version/settings, per-command observations, redacted typed operations, original operation/stop receipt bytes, and original snapshot bytes. Start, Inspect and Cancel replies must match every immutable field of the actual submitted request. The comparison ignores only the bearer grant and uses `time.Equal` for the deadline, accepting the same instant expressed in a different timezone. Each final operation receipt must bind its current sample's tenant/run, not merely either workspace owned by the experiment. The harness also compares receipt bytes with the full typed operation after removing the artifact's self-reference, checks stop-proof binding, and recomputes snapshot file hashes and the manifest tree hash. All copied artifacts must match their advertised byte count and SHA-256.

The operator supplies the actual runner PID, excluding the rootlesskit parent. `/proc/<pid>/exe`, start ticks and `-config` argument identify that process before and after the experiment; `go version -m` records its build metadata. The actual gRPC dialer samples `SO_PEERCRED` on each Unix connection, retains its timestamp/PID/UID/GID, and rejects a peer PID different from that process. This includes reconnections and is a connection-creation observation, not cryptographic session attestation. The actual runner executable and current selected source snapshots are recorded separately: a source snapshot alone does not establish that the running service was built from those bytes. A configuration hash and matching command argument identify the supplied file; they are not an attestation of every value read at service startup. The harness never reads process environments or writes signing-key bytes or bearer grants to evidence.

Cleanup targets only the two freshly generated workspace IDs. An explicitly unknown operation retains its workspace for inspection. Other cleanup requires a valid no-active Stop receipt and a readable, hash-verified sealed snapshot before Release. Both Stop and Snapshot must match the original tenant, run, workspace ID and epoch before their proofs can authorize release. Failed cleanup retains the workspace and makes the run fail; it does not delete containers, unrelated workspaces, images, journal rows, artifacts, or mounts. The recorded Release replies establish those two releases; the harness does not claim a full global SQLite/pool audit. Executable copies can be large and remain local evidence unless deliberately archived; publishing a summary must preserve their hashes and distinguish locally retained from versioned files.

## Explicit execution entry point

From the repository root, in the operator context that can access the existing runner socket, signing-key file, artifact root, Docker socket and its `/proc` entries, set the actual `FORGE_REAL_RUNNER_PID` and run:

```sh
FORGE_RUN_EXECUTION_EVIDENCE=1 \
FORGE_REAL_RUNNER_CONFIG="$PWD/var/local/runner.json" \
FORGE_REAL_RUNNER_PID="$FORGE_REAL_RUNNER_PID" \
GOCACHE=/tmp/forge-runtime-gocache GOPROXY=off \
  go test ./internal/runnerclient \
  -run '^TestRealRunnerExecutionEvidence$' -count=1 -timeout=6m -v
```

The default output is a new `benchmarks/results/local/runner-execution-<UTC>` directory; its parent must already exist. An absolute `FORGE_EXECUTION_EVIDENCE_DIR` may select another new leaf whose parent exists. An existing leaf is rejected. A normal run is expected to take roughly one to three minutes, without making that estimate an acceptance threshold. Run without `-race` for these observed timing samples; the offline oracle regression is separately checked with `-race`:

```sh
GOCACHE=/tmp/forge-runtime-gocache GOPROXY=off \
  go test -race ./internal/runnerclient \
  -run '^Test(P06|RealRunnerExecutionEvidence)' \
  -count=1 -v
```

With the opt-in variable unset, the real runner case explicitly skips. The four offline cases reject altered no-active/epoch/active-operation stop proofs, altered content/executable/workspace snapshot proofs, all immutable request-field changes, receipt tenant/run substitutions, another internally consistent sample's result, incorrect original cleanup bindings, and usable external network interfaces/routes. A positive case accepts a UTC-converted deadline for the same instant. These checks pass under `-race`, and `go vet` passes. The offline verification explicitly selects only `^TestP06`; it does not invoke the real runner case. These offline checks do not count as P06 execution evidence. The cancellation program installs the parent's SIGTERM ignore handler before `fork`, so the child inherits it before emitting readiness.

## Preserved first attempt and correction — 2026-09-11

The [first real report](../benchmarks/results/runner-execution-20260911/attempt-01/report.json) records `passed=false` with `actual process isolation/resource facts failed`. It reached two normal process samples and then stopped at the old assertion requiring precisely one network device named `lo`. The probe reported `lo` and `tunl0`, while Docker reported `NetworkMode=none`. That old probe did not record per-interface state or routes, so this failure is neither a demonstrated external network escape nor evidence that the additional interface was safe. Its exact [program](../benchmarks/results/runner-execution-20260911/attempt-01/command.py), source snapshots, original artifact bytes and [local executable identities](../benchmarks/results/runner-execution-20260911/attempt-01/retained-local-executables.json) remain retained unchanged. Both fresh workspace IDs (`p06-3ce8ae2a909f3101-0` and `p06-3ce8ae2a909f3101-1`) have successful no-active Stop, sealed Snapshot and Release replies in that report.

The correction replaces that name/count assertion with the flags, state, address and route rules above. Its fixed Linux interface buffer rejects truncated or malformed inventory. The offline positive case deliberately uses an arbitrary name for a down/addressless non-loopback device; 14 network negative cases cover enabled/addressed external interfaces, IPv4 aliases, external/default/gateway routes in IPv4 and IPv6, unreported or duplicate interfaces and missing loopback. Original cleanup tenant/run/ID/epoch substitutions are also rejected.

The revised [checks](../benchmarks/results/p06-network-oracle-20260911/checks.json), [race log](../benchmarks/results/p06-network-oracle-20260911/race.log), source snapshot and program are retained separately from the failed attempt. Four top-level offline tests and 29 named subcases passed under `-race`; `go vet ./internal/runnerclient` exited zero, and Python `ast.parse`/`compile` accepted the extracted program without running it. Frozen harness SHA-256: `eb820cece4bc79dda4c5fd259b69c42fd10f2c1e9a15a68b4c256d346e4fabf2`; program SHA-256: `1147a2e19ee55e58759f4ea5d459ece1741a0ad689521a6b1e8f7a7ceefd1502`. The subsequent attempt below supplies the real container observations. The offline oracle alone does not establish those facts.

## Passing second attempt — 2026-09-11

The [original report](../benchmarks/results/runner-execution-20260911/attempt-02/report.json) covers `2026-09-11T22:16:20.426917349Z` through `22:16:36.779771905Z`, a 16.352855-second harness interval. The separate [process wrapper](../benchmarks/results/runner-execution-build-20260911-attempt2/execution.json) exited zero in 16.392388 seconds. Neither duration is a command-start latency. This is one host execution with ten samples per mode, using the actual rootless Docker Engine 29.8.0 runner and the pinned Python image recorded above.

All ten normal commands exited zero and each made 50 checks of its own marker at the shared relative filename. All ten cancellation commands exited 137 after the harness observed both Python parent and child plus `docker-init` in the dedicated cgroup. Their actual Docker state reported `OOMKilled=false`; exit 137 is interpreted here as the observed cancellation outcome, not an OOM measurement. Each cgroup was absent after cancellation, with no remaining tasks. All 20 post-command `read_file` operations returned the correct workspace-specific marker and hash. There were 20 distinct container IDs and 40 distinct operation IDs.

Each of the ten paired rounds used distinct writable workspace mount sources. Recomputing `min(FinishedAt) − max(StartedAt)` from the final Docker observations gives positive daemon-clock overlap for every pair: 1.155409–1.213087 seconds for normal rounds and 0.315117–0.395071 seconds for cancellation rounds. In each cancellation round, A's stopped-container/cgroup-empty observations precede a read-only observation of B still running; B's cancellation begins after that observation ends. The [recomputed pair records](../benchmarks/results/runner-execution-20260911/attempt-02/audit.json) retain those separate monotonic timestamps. This proves the recorded order at observation boundaries, rather than an unobserved continuous isolation claim.

Every process reported the same network inventory: `lo` had flags 73 and IPv4 `127.0.0.1`; `tunl0` had flags 128, operational state `down` and no IPv4 address. IPv4 had no route entries. IPv6 had one active loopback `::1/128` route with unspecified next hop, plus two rejected default entries; there was no usable external route. The independent audit reparses the original route-table strings and compares them with the emitted records before applying the address/state/route rules. Docker observations for all 20 containers also retained `NetworkMode=none`. Actual UID/GID were 1000, capabilities zero, no-new-privileges enabled, memory limit 268435456 bytes, PID limit 64, CPU quota `100000 100000`, read-only root filesystem and 67108864-byte `/tmp`. These are configuration/readback results; the earlier exhaustion tests remain separate.

### Recomputed observed timings

Values below are milliseconds, rounded to three decimals. Every row has **n=10**; p50/p95 use the interpolation defined above and were independently recomputed from raw monotonic stamps. Active daemon logs and terminal typed Inspect logs are distinct measurements. The cancellation samples include the explicit parent/child and peer-survival checks before cancellation.

| Mode | Measure | Min | p50 | p95 | Max |
| --- | --- | ---: | ---: | ---: | ---: |
| normal | Direct Start acknowledgement | 40.463 | 52.919 | 61.858 | 67.743 |
| normal | Start → first observed running, upper bound | 415.586 | 465.254 | 501.536 | 506.004 |
| normal | Start → active daemon log | 506.247 | 591.573 | 632.044 | 635.922 |
| normal | Start → terminal typed Inspect log | 1664.477 | 1747.322 | 1848.840 | 1855.735 |
| cancel | Direct Start acknowledgement | 39.755 | 55.245 | 67.627 | 68.558 |
| cancel | Start → first observed running, upper bound | 420.022 | 487.275 | 538.001 | 548.019 |
| cancel | Start → active daemon log | 509.587 | 588.659 | 641.029 | 641.927 |
| cancel | Start → terminal typed Inspect log | 772.071 | 951.685 | 1158.521 | 1160.356 |
| cancel | Cancel RPC acknowledgement | 183.902 | 210.675 | 234.428 | 235.474 |
| cancel | Cancel → first observed stopped, upper bound | 197.260 | 227.110 | 252.296 | 255.553 |

### Execution identity and audit limits

The [fixed build](../benchmarks/results/runner-execution-build-20260911-attempt2/identity.json) used commit `a0b3a5da4c09181cf600e7e4f54d83df93387282` plus only the reviewed `execution_benchmark_test.go` SHA-256 `eb820cece4bc79dda4c5fd259b69c42fd10f2c1e9a15a68b4c256d346e4fabf2`. All 267 recorded build inputs matched before and after; the independent auditor reconstructs the other 266 inputs from that Git commit. The actual test executable SHA-256 is `d58fa279d9cb66d527813538d5f0961181b5708d56435b2e4768712d3b65ab4c`.

The existing executed runner had PID 969928, start ticks `44361042`, and executable SHA-256 `6921c94212899fada5fabc730dcea7e4944d86c9ae2baccd5656e26dcbe969a0`. The actual gRPC Unix connection recorded `SO_PEERCRED` PID 969928/UID 1000/GID 1000. The two retained executable files were rehashed independently; their [local paths, lengths and hashes](../benchmarks/results/runner-execution-20260911/attempt-02/retained-local-executables.json) are versioned, while the large executables remain local. The runner executable is identified separately from the test build and current source snapshots; this experiment does not claim the runner was rebuilt from the harness checkout.

The [independent recomputation script](../benchmarks/results/runner-execution-20260911/recompute.py) validates exact original process intent against final typed/Cancel/receipt records, canonical argument hashes, receipt binding and bytes, 20 read results, all 44 artifact lengths/digests, two sealed snapshot file/tree hashes, original cleanup tenant/run/workspace/epoch, all overlap and cancellation ordering observations, interface/route facts, ten timing distributions, source identities and executable hashes. It verifies the [99-file content manifest](../benchmarks/results/runner-execution-20260911/attempt-02/manifest.json), including the report, program, 11 source snapshots, 40 typed records, 44 objects, local executable metadata and its audit result. This is an independent calculation over one execution's retained observations; it is not a second OS experiment or an independent implementation review.

Both original workspaces `p06-e3d1756b0907532f-0` and `p06-e3d1756b0907532f-1` have valid no-active Stop receipts, revision-11 sealed snapshots and successful Release replies. The frozen code performs those calls in that order. Separate cleanup RPC timestamps are not retained, so the audit establishes the recorded proofs, final replies and source order rather than independently timing each cleanup call. Full original Start replies are also not separately saved: the direct call and immutable-intent guard are source-reviewed, while retained final/Cancel/receipt intents are independently recomputed. The process identity before/after check is enforced by the frozen harness; its report retains final process identity and connection peer samples rather than two independent process snapshots. No PostgreSQL leases, capacity accounting, model, paid API, global pool census or packet-transmission probe is part of this measurement.

Recompute without contacting any service:

```sh
python3 benchmarks/results/runner-execution-20260911/recompute.py
```

The script requires the recorded local executables and Git commit for full identity verification, writes nothing during a normal rerun, and fails if a recorded file, artifact or measurement differs.
