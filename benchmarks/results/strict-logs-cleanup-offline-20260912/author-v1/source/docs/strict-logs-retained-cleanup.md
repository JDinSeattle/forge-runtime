# Retained strict-log fixture cleanup

This is a narrowly scoped **operator fixture cleanup**, with actual execution still pending at source freeze. It cannot establish terminal cancellation of a PostgreSQL run, restore a model request, or count as a successful S12.9 logs acceptance.

The failed `logs-01` suite reached the fourth L3 byte-budget operation. Its 512 KiB multiplexed log retained only stderr while both stdout and stderr seen counters were positive. The corrected oracle permits either retained prefix; it still validates exact loss, framing, bounds and completed termination. Original failed evidence is preserved.

## Fixed authority

`TestStrictLogsRetainedCleanup` is disabled unless `FORGE_RUN_STRICT_LOGS_CLEANUP=1`. The reviewed launcher supplies `FORGE_STRICT_LOGS_ACCEPTANCE=<scope>/acceptance-logs-cleanup-01.json`; `EvidenceDir` remains the historical `evidence/sigterm-02`, while the declared three executables are from a new frozen version directory. Integration uses the distinct `logs-cleanup` phase. No database environment or credential is needed.

Only these identities are admitted:

- Scope `lr20260912_a`; tenant `strict-log-fixture`; workspace/run `sl-L3-bytes-46djebzw3ifph2ui7nesemhkz6`; epoch 1; revision 1; slot `slot-001`.
- SQLite journal UUID `b010dd7440a54c58601967342f397faf686b7e306061af9f49809ee342b0ec8e`, version 5.
- The original 44 terminal operations, including exactly four unchanged target operations, and the other three released workspaces. Every request, result, receipt, container identity and lifetime log reservation must remain unchanged.
- The original failed manifest SHA `3ff0f3548c756214e9bdd41032c749d1b873d5322e680baac3a5850a125cf15d` and original `acceptance-02.json` SHA `f41acf98de6b4c3a0a36ac1e025d9ec6434c0cb51daa00228ac663566bd3a76b`.

The original suite has **no `acceptance-input.json`**. Its manifest covers `preflight.json.acceptance`, which must equal the original `acceptance-02.json`. The failed acceptance has eight immutable execution inputs, not nine. Its manifest has 524 members. The first read-only check used the mistaken count of nine and failed before any execution; that failure log is retained.

The entry point shares `runtime/.sigterm-control.lock` with the lifecycle fixtures. It requires the original host-launch terminal result, no live UDS peer, exact existing owner markers and persisted volume specs, production `VerifyVolume` for all four volumes, and availability of their existing pool locks. It starts only its own new runner with the existing byteguard config. The runner performs the same lifetime ownership checks again. No old service is stopped or restarted.

The target was created by the original direct-RPC fixture; there is no corresponding PostgreSQL task or scheduler lease. Each RPC receives fresh, 20-second, tuple-scoped **fixture authority** with only `inspect`, `cancel`, `snapshot`, and `release` permissions. This is neither a renewed old execution token nor a database-backed maintenance lease. The test never calls prepare, execute, verify or adopt, never starts a worker, and never invokes Docker removal directly.

## Durable sequence and failure boundary

1. Validate every original manifest member and frozen input. Record a new immutable intent and a read-only journal observation in `evidence/logs-01-cleanup`.
2. Inspect the four exact terminal operation IDs through the new runner. Compare returned operations, actual published receipt bytes and log bytes with the original archive; verify log framing/loss and durable artifact pins.
3. Call production `StopWorkspace`. Validate the exact stopped workspace tuple, `NoActiveOperations`, immutable stop bytes and object identity.
4. Call production `SealSnapshot`. Read the actual snapshot object, validate its workspace tuple and the complete original `app.py` bytes, executable bit and independently recomputed source-tree hash. Fsync the archived bytes and metadata, including their parent directory, and verify the two SQLite pins.
5. Persist a release intent binding both artifact hashes. Only then call production `ReleaseWorkspace`. Production code removes only the four known terminal containers and that workspace's storage. The test does not edit SQLite, delete directories, refund log reservations, rebind owners or reformat volumes.
6. Confirm both released flags, four `removed` log-container states, absent exact workspace storage, unchanged operation/other-work rows, and retained snapshot/stop objects. Stop only the new child runner. Write the successful report and a manifest covering all cleanup evidence.

Each execution has a new `invocation-*` directory with its own runner logs, identity, read-only journal observation and success/failure report. Failed or partial files are retained. Fixed stage files use exclusive creation and fsync; an existing different or partial stage is a hard failure.

A restart after Stop can repeat the same Stop and Seal while the workspace still exists. If any container cleanup or released flag has advanced, previously archived snapshot/stop bytes, pins and the exact release intent are mandatory. When the workspace is already marked released, the entry point never repeats Stop or Seal. It may repeat only the same Release to finish pending durable storage removal, or confirm an already completed release from the journal and preserved proofs. Missing proof fails closed. An already finalized manifest prevents another cleanup execution.

The final `report.json` is created only after successful confirmation. A failure has an invocation report and no successful final manifest; the launcher must not infer success from a directory's existence or an earlier partial stage. A process killed before its deferred failure writer runs can leave only its intent/log/stage files; this remains incomplete evidence.

## Next full logs execution

`slCleanupPrerequisite(a, c, j)` checks the completed manifest, all required report and stage fields, original failed inputs, the cleanup execution acceptance and complete input map, current immutable object bytes/pins, and the current journal against the released observation. It returns all proof paths and hashes for the new suite's final recheck. It runs before that suite creates a schema or starts a runner. New logs evidence and private settings use distinct execution-02 paths; the old logs-01 archive and historical E50 binary identities are retained.

## Offline validation

The helper tests cover accepted crash-stage shapes, changed UUID/epoch/revision/request/result/receipt/container/volume/budget rejection, snapshot identity and source-byte corruption, minimal grant permissions/expiry, immutable stage and complete manifest handling, and missing archive bytes/pins. These use synthetic journal values and temporary regular files, never a live Engine or Docker backend.

An optional `TestStrictLogsCleanupArchivedInputs` with `FORGE_STRICT_LOGS_READONLY_SCOPE` reads the retained fixture files and four published receipt objects. It does not read the signing key, open SQLite, connect to a socket or start a process. On the retained originals it verified 535 distinct input paths, including all 524 manifest members and the original eight execution inputs after deduplication.

No actual Stop, Seal, Release, volume cleanup or new full logs acceptance is claimed by these offline results.
