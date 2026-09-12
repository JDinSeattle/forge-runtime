# E44 — bounded process logs (S12.9)

Status on 2026-09-12 UTC: strict logging is integrated into main at `4846457`; filesystem/SQLite tests and the [E49 integrated ordinary/race suites](integration-checks-e49-20260912.md) pass. Immutable receipt-first log publication also passes independent real-PG/FS checks. The pre-start Docker attach protocol has a real successful probe. The fixed-volume **backend-only** acceptance harness is frozen but its actual execution is **pending authorization or an independently owned mounted volume**. That harness does not open an Engine or a SQLite journal. Its result cannot by itself prove the complete Engine + Docker + PostgreSQL publication path.

The author implementation was developed from detached `5de6f0f` and frozen at `5248cc1`; its fixed backend binary retains that separate build identity. Main integrates the feature with E43 telemetry and E47 admission ordering. It has not been deployed to the demonstration runner. Existing v4 journal ownership, pool owner markers and services remain intact. Automatic approval review rejected the planned shared-volume/stop-services execution before CreateProcess because of interruption, destructive-state and restoration risk without explicit user authorization; no demonstration service was stopped and no fixed-volume backend-test container was started. The explicit authorization question remains pending. This fixture must not be executed indirectly to bypass that decision.

## Protocol and policy

The executable selects strict capture by default. New process operations use `--log-driver none`; the runner creates the container, persists its immutable container ID, obtains an HTTP 101 attach acknowledgement, durably records start intent, then invokes Docker start. Only an explicit Unix Docker endpoint is accepted. The reader decodes both non-TTY streams using the documented eight-byte multiplex header and bounded 16 KiB reads. The API version is deliberately pinned to v1.51. [Moby Engine API v1.51](https://raw.githubusercontent.com/moby/moby/v28.3.3/docs/api/v1.51.yaml)

`none` leaves no retained daemon log available through `docker logs`. This avoids relying on the physical behavior of rotated/compressed local-driver files for a run budget. Existing containers keep their original logging configuration; this change does not retrofit or delete their logs. [Docker logging-driver documentation](https://docs.docker.com/engine/logging/configure/)

| Boundary | Default and hard ceiling | Exact interpretation |
| --- | ---: | --- |
| Entry | 16 KiB | Includes the 32-byte spool frame header, not a line or a Unicode string. |
| Operation | 512 KiB | Total persisted framed spool bytes; oversized output is drained and discarded while the job is stopped. |
| Run | 16 MiB | Cumulative reservations keyed by tenant/run, not current filesystem occupancy. |
| Process operations | 32 | `run_command` and `verify` both consume one reservation, including failed/cancelled attempts. |
| Receipt preview | 64 KiB | Raw binary preview before JSON/base64 encoding; bounded so full receipt metadata survives RPC mapping. |

Zero configuration fields take these defaults; negative values and values exceeding these ceilings are rejected. Smaller consistent limits are allowed. A run's normalized policy is immutable. The operation reservation and operation insert commit in the same SQLite transaction. Replaying a stable operation ID does not reserve again. Reservations are not refunded after a receipt or container deletion: this is a cumulative output/work budget. A new strict operation in a run with legacy process history is rejected instead of assigning unmeasured old logs a fictional budget.

The hard run number describes **framed log payload**, not all physical copies. A bounded metadata sidecar (read cap 128 KiB per operation), the bounded receipt preview, and the artifact copy use additional storage. The workspace, baseline, candidate and spool share the fixed filesystem's separately enforced capacity. Full artifact storage remains subject to its existing storage/retention policy. No claim is made that 16 MiB covers daemon metadata, all artifact copies or unrelated host writes.

## Durable representation and failure behavior

Each trusted sibling log directory is outside the writable task checkout mount. Absolute directory components are opened with `openat(O_DIRECTORY|O_NOFOLLOW)` and pinned before creating `os.Root`; files use `O_NOFOLLOW`, `O_EXCL`, mode 0600. The directory is mode 0700. Directory symlinks, substituted files and oversized reads are rejected.

A frame contains magic `FLG1`, stream ID, monotonic sequence, payload length, CRC32 and reserved zero fields. The sidecar binds schema version, policy, operation ID, the complete immutable operation request excluding its bearer grant, retained bytes/hash, bounded preview and capture facts. Seal ordering is spool fsync → sidecar temp fsync → rename → parent fsync. Recovery validates the entire sealed spool or returns only an authenticated frame prefix marked incomplete. It never appends to or silently restarts a crashed operation. The sidecar's stream counts, dropped-byte knowledge, completeness and termination flags must be self-consistent.

`stdout_seen` and `stderr_seen` count bytes actually observed by this attach reader. An incomplete capture cannot claim the total output of the process. `dropped_known` is false for capture gaps; retained-prefix bytes remain downloadable. A completed capture can report the exact observed dropped bytes. A log overflow or spool write failure requests a separate policy stop; it cannot masquerade as user cancellation. Capture EOF does not prove the process stopped: Docker state must confirm it. A policy kill or an incomplete/truncated capture is rejected as trusted target/regression verification evidence, including the initially failing target.

Runner shutdown closes the capture connection and marks `runner_shutdown`/incomplete, leaving an executing process alive and its operation unknown. A successor observes the original operation/container and does not restart it; explicit cancellation/adoption can later stop it. Ordinary business cancellation stops the container while retaining the drain through EOF, and its effect records cancellation separately from output policy. The integrated executable invokes `BeginShutdown` before RPC drain, then closes the Engine while retaining journal/pool ownership until requests finish.

SQLite migration **v5** adds `log_runs` and `operation_logs` (policy, immutable container ID and retained/removing/removed cleanup state). It preserves v4's journal identity, pins and existing operation receipts. Existing v4 operations without a log row keep legacy behavior. The collector accepts v4 and v5 because its authoritative artifact pin queries are unchanged. An old v4-only runner must not be restarted against an upgraded v5 journal.

Publication ordering is checked spool → durable `operation_log` object + journal pin → durable operation receipt + pin → SQLite terminal receipt. Under the existing shared publication lock, failure cannot expose an unpinned successful receipt. The application validates the receipt's original bytes against the operation and exact result, validates the log tenant/run/kind/size/digest, publishes its idempotent PG READY row, then publishes/settles the receipt. This makes complete retained logs available through the existing authenticated artifact listing/download API. Ref substitution or missing bytes fails closed.

Workspace release removes only stopped strict containers identified by their persisted original ID and exact name. Removal progress is journaled; retries do not infer a refund. Unknown operations retain their container/workspace. Legacy local-driver containers are not silently deleted by the strict cleanup path.

## Current evidence and preserved failures

- `benchmarks/results/log-limits-e44/preflight-01/report.json`: root executed the standalone no-mount probe against the dedicated rootless daemon. HTTP 101 acknowledgement preceded start; binary stdout `early-out\0` and stderr `early-err\xff` matched exactly; driver was `none`; process exited zero; the sole owned container was deleted. This proves the protocol, not fixed-volume quota or end-to-end publication.
- `unit-first.log`, `affected-race-02.log`: initial capture/reservation/migration/shutdown/receipt crash tests and five affected packages passed with the race detector.
- `strict-race-05.log`, `strict-race-07.log` and `artifact-race-08.log`: directory/metadata negative cases, RPC binding roundtrip, binary framing, two-stream drain, partial headers, handshake cancellation/header bounds, real SQLite durable reservations, legacy migration and lost-receipt recovery passed. The real Docker harness is opt-in and skipped in these runs.
- `affected-race-06.log`: runner, sandbox, application, retention and executable packages passed. Existing runnerclient UDS/TCP tests failed on sandbox socket `EPERM`; this was an execution-environment limitation, not a claim that those transport tests passed here.
- Preserved intermediate failures: an initial helper-name collision; a huge-frame truncated body reported EOF instead of unexpected EOF (corrected); the first RPC fixture omitted its required tool kind (corrected); `strict-race-03.log` recorded a transient unused import during editing; `strict-race-04.log` exposed the net.Pipe fixture closing before deadline reset (corrected by explicit handshake synchronization). Do not relabel these as passing original versions.
- `compile-all-02.log` and `vet-01.log`: every repository package compiled; six affected packages passed vet. `compile-all.log` preserves the first failed attempt, when `.go` evidence copies were inadvertently discoverable as incomplete packages; archive copies now use `.go.txt` and `.py.txt`.
- Independent review found two publication gaps while this feature was under construction: the application initially did not create a READY row for `operation_log`; the first fix trusted absent RPC metadata when classifying a legacy operation. The corrected classifier reads the immutable receipt first. [Controlled prior-function failures](../benchmarks/results/log-publication-e44-controlled-20260912/manifest.json) preserve omitted publication and substituted references. Missing RPC metadata has its own before/after regression in the final evidence directory. The prior function was overlaid on a controlled current Driver, not represented as an old whole-tree build. [Final real-PG/FS tests](../benchmarks/results/log-publication-e44-final-20260912/result.json) pass with `log_artifacts.go` SHA-256 `c15c82e5bf791ff5012444dab6637256ae3e0886153afa8ca206cb75bef0c8a8`: one idempotent READY row, authenticated artifact metadata/bytes, foreign run/kind/hash/size rejection, and stripped strict metadata rejection while genuine legacy receipts remain compatible. This is not an HTTP or Docker observation.
- [The first publication capture](../benchmarks/results/log-publication-e44-before-20260912/capture-correction.json) overlapped an author edit and contained an invalid zero-retained-bytes test fixture. Its directory name is historical; it is not valid old-source before evidence. Its original failure is retained alongside the correction rather than relabeled.
- A subagent's first privileged probe invocation was aborted after waiting for approval; it produced no Docker result. Root subsequently executed the reviewed standalone script successfully. No live runner or journal was changed for this probe.

Production source and binary identity for the backend executable are archived in the accompanying `source-snapshot` / manifest. The fixed binary is a local artifact, not committed as a large executable.

## Operator-only backend execution

Build (from this isolated worktree, with the documented offline Go environment):

```sh
go test -c -o var/local/e44-runner.test ./internal/runner
```

Pass `FORGE_E44_BACKEND_CONFIG` naming a private JSON file with:

```json
{
  "volume": { "id": "operator-selected-slot", "mount_path": "complete existing VolumeSpec required" },
  "docker_host": "unix:///run/user/1000/forge-runtime-docker.sock",
  "image": "python@sha256:229a2c5bfa27522db7815ea81f9bed70af17ccb9de9fc7ad142b1877b5830d36",
  "output_dir": "/absolute/existing/private/evidence-directory",
  "exclusive_handoff": true
}
```

The abbreviated `volume` above is illustrative: use the full already-verified VolumeSpec, including image inode/device/size, loop device, filesystem UUID and inode ceiling. The fixture never mounts, resizes, rebinds ownership or reads credentials. Root must first confirm that the borrowed slot is released and stop its original runner/worker ownership. The harness itself obtains the existing `.forge-pool.lock` with a nonblocking exclusive flock on a read-only fd. A parent must not concurrently hold the same lock.

```sh
env FORGE_E44_BACKEND_CONFIG=/absolute/reviewed-config.json \
  /absolute/logs-worktree/var/local/e44-runner.test \
  -test.run '^TestStrictLogDockerBackendAcceptance$' \
  -test.v -test.count=1 -test.timeout=120s
```

The fixture exclusively creates `output_dir/backend` and one random `forge-e44-*` sibling on the selected volume. Four commands cover binary early output, concurrent stdout/stderr flood, cancellation with a child, and shutdown-context detach with a child still running. This is an actual Docker attach disconnect caused by the same context signal used by Engine shutdown; it is not an OS SIGTERM/SIGKILL of a runner process. The announced bound is 20 fixture filesystem entries and 4 MiB including sidecars; it does not fill the whole volume. Each intent is saved before create; original container ID is saved immediately on acknowledgement. Every case records actual `none`, empty `LogPath`, labels/ID, both-stream counts, spool digest, completion and policy termination, and unchanged Docker `StartedAt`/create/start-intent counts on same-ID retry. Cleanup requires complete name/ID/label ownership. Unresolved cleanup retains its descriptor and directory.

This backend fixture uses real Docker and fixed-volume spool I/O, but no Engine journal. Engine SQLite crash/reservation/artifact ordering has separate actual SQLite/filesystem tests. An all-in-one Engine + Docker process-crash/publication acceptance still requires a separately owned mounted volume pool or a reviewed compatible deployment; the backend test alone must not close that wider evidence claim.

The author recomputation is `scripts/logs/audit.py`. `author-audit-preflight.json` independently recalculates binary stream payloads and the 255,658 ns attach-ack-before-start interval from saved protocol evidence, checks 36 locally linked source/module inputs, and checks the exact test-binary digest. It is a recomputation of the same observation, not a second live Docker observation.
