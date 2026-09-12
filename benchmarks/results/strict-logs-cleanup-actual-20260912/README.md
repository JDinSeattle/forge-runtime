# Actual retained-log cleanup: release completed; test exited1

This archive preserves the 2026-09-12 cleanup of the retained `logs-01` workspace in the independent lifecycle pool. The actual test used revision `c99a285c85d782d5a3d22a84447371db445c69d1` and **failed after material cleanup**, at its final read-only comparison of `acceptance-input.json`. Go reported3.27s; the host launcher reported exit1 and3.575698666s. The stage report's original `passed:true` describes completed Stop/archive/Release. It does not convert the overall failed execution to PASS.

The initial independent audit passed74 checks. It verified the original19-member cleanup manifest, unchanged full rows for all44 terminal operations, unchanged95 artifact pins plus the two new stop/snapshot pins, and all4 workspaces/leases released. The target's four `operation_logs` rows changed only `cleanup_state` from retained to removed. The complete journal has42 log rows, not44; two operations have no log row.

The464-byte stop object and531-byte snapshot match their content addresses and durable pins. The snapshot contains only the fixture's original `app.py`, with exact bytes/hash/mode checked. Actual stored immutable artifact bytes were read and matched during the first audit. The archived copies allow subsequent offline verification without accessing runtime storage.

Own runner PID1990464 was bound to the UDS peer and recorded executable hash, received SIGTERM and exited0. Production `RemoveOwned` acknowledgements were persisted before typed Release returned. **No additional post-removal Docker inspect or events query was archived or performed by the independent reviewer.** These are durable cleanup acknowledgements, not a separate live-daemon absence probe.

The failure was reproduced from the executed predicate: JSON decoded into a map marshaled in sorted key order, while the expected struct retained declaration order. Both represented the same object. Root's two-file fix at `0a5f0f47100941858ae12b07ecd9bb094bdca833` canonicalizes both sides while retaining exact JSON numbers. Independent c99 plus those two frozen files passed41 raced tests with2 opt-in skips; vet exited0. Changed fields, distinct large integers and concatenated JSON remain rejected. No cleanup was repeated for this review.

Also retained is the subsequent revision of the **then unexecuted** logs02 preparation: an exclusive original copy plus intent/result changed exactly six binary path/hash fields. Its first attempt stopped at the nonsecret build-report file-mode guard; that observation exists only in the executor's tool output, not an invented raw log. The reviewed correction and successful second attempt are recorded separately. `executed:false` in that revision action describes preparation only. Later logs02 execution and its outcome are outside this archive.

`raw-evidence.tar.gz` contains30 byte-identical selected raw files. `archive-manifest.json` maps each source hash to its archived hash. No runtime configuration, credential content or digest, key, database file, binary or full Docker stdout was copied. Empty persisted grant fields remain empty. Source copies use `.go.txt`/`.py.txt` to prevent accidental compilation or execution. The historical audit report remains unchanged, including its timestamped observation that logs02 had not yet run.

Run the portable check from any checkout:

```sh
python3 benchmarks/results/strict-logs-cleanup-actual-20260912/verify.py
```

It verifies112 conditions against only the archive. This evidence does not close the combined S12 acceptance, change a checklist status, or establish any paid-model result.
