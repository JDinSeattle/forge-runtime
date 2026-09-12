# Third strict-log attempt: preserved failure and retained L4 diagnosis

The actual `TestStrictLogsCombinedAcceptance` finished **FAIL in 77.14 s** on
2026-09-12. Its aggregate remains `passed: false`. The producer recorded five
passed cases: **L1, L2-L3-default, L3-bytes, L3-count, and L5**. L4 timed out at
`strict_logs_combined_test.go:1105`, waiting for an actual spool write failure.
This archive does not promote the overall S12.9 acceptance status.

`raw-evidence.tar.gz` preserves the entire 602-member `logs-03` manifest and
its manifest file, three `host-logs-03` files, and the later six-member
`logs-03-retained-observation` manifest plus its manifest file: **613 files**.
Their original total is 505,212,850 bytes; the compressed archive is 9,290,594
bytes. All original manifest members were independently hashed before copying.
`archive-manifest.json` maps every source path/hash to its archived path/hash.

611 files are byte-identical. Only `L1/fixture.json` and `L4/fixture.json` have
an explicit `_archive_projection`: each omits `worker_config_sha256`, the
digest of a private worker configuration. No private worker configuration,
credential, key, DSN, or its digest was read or copied by the archivist.
The enclosing original fixture file hashes remain recorded, and the original
manifest bytes remain unchanged. The structured and credential-pattern scan is
in `secret-scan.json`; all 6,000 retained `grant` fields are empty. Docker
environment entries contain only the five documented public image variables.
Four raw log artifacts have a `.json` suffix but contain binary log frames;
their original bytes were retained. This finite scan is not a proof about every
possible secret encoding.

## What the raw observation establishes

The exact L4 run is `run_DKT2OOLEVCKYXHW5NGNS7BKBHJ`, operation
`run_DKT2OOLEVCKYXHW5NGNS7BKBHJ_step_1_op_0`, tenant
`sl-L4-xsra53ckdacsus345hmddtehsj`, epoch 2. The retained pressure file is
inode 26/device 1802 in its own slot-001 workspace. The same journal UUID is
`b010dd7440a54c58601967342f397faf686b7e306061af9f49809ee342b0ec8e`.

The pressure loop stopped after a **1 MiB write** returned ENOSPC. At
18:24:17.664428967Z its own `statfs` still recorded **430,080 available bytes**.
The finite command emitted an initial 11 bytes per stream, slept eight seconds,
then attempted 300 pairs of 128-byte writes, with a 50 ms delay per pair. Its
planned payload was only 76,822 bytes. First failure of a large pressure write
did not establish failure of these smaller spool writes.

The later read-only sample is dated **18:33:29.232059Z**, not a current live
assertion. It records the same pressure file still allocated, with **364,544
bytes available**. Independently decoding the retained spool yields **184
complete CRC-checked frames, 29,206 encoded bytes, and 23,318 payload bytes**:
11,659 bytes per stream, representing the initial prefix and 91 loop writes
per stream. There is no torn frame tail. The preview, frame counters, request
binding hash, and spool SHA all match the metadata.

That metadata says **`reason: runner_shutdown`**, `complete: false`, and
`termination_requested: false`, `termination_observed: false`. The original
container `483c36964c0d87a9b94af7ae44f7a3ac4a1d54191afba1deddb2e6d1515c7bf4`
ran for 23.181205 s, naturally exited 0 without OOM at
18:24:40.311957499Z, and ended **10.474809 s after** the runner exited. This is
consistent with the eight-second sleep plus the approximately fifteen-second
loop. It does not establish an ENOSPC-triggered log-policy stop.

The worker pause lasted 12.380968 s, did not fire its 18-second watchdog, and
ended with 17.042176 s remaining on the captured lease. Thus the pause watchdog
or an expired lease during that pause does not explain this failure. The runner
shutdown came from failure cleanup; it detached capture without cancelling the
actual command. The observer itself invoked no runner RPC, Cancel, Start,
release, or file removal.

## Authority and cleanup limits

Across the captured SQLite tables, after normalizing JSON member order and
omitting the already-empty grant, the only changes are the target operation's
`running` → `unknown` status and empty error → `log_capture_incomplete`.
Its request, job ID, dispatch/start intent, empty receipt/result, and
`cancel_requested=0` are unchanged. The workspace remains unsealed and
unreleased, the original log container remains retained, and the slot lease
remains allocated.

The PostgreSQL sample is the exact private schema
`appfault_strictlogs_zg4ub4tnaruvpaojcj5dmmeqpc`, OID **852843**, in `/forge`.
The L4 run is still `running`, version 9, epoch 2, with the original in-flight
effect and immutable command. Its lease and deadline have expired **by the
later sample**, while active-run, runner allocation, and unsettled-effect
counts remain one each. Model requests and unsettled model reservations are
zero. Committed fees total 570 synthetic micro-USD across the two fixture API
runs; L4 accounts for 150. There were no paid model calls.

These are current run snapshots and aggregate ledger counts at the recorded
time, **not complete individual effect/attempt/allocation/event histories**.
The same unknown operation needs truthful reconciliation, durable incomplete
log publication and receipt, followed by Stop, snapshot sealing, and release.
Docker exit 0 or an expired SQL lease cannot by itself authorize treating the
effect as successful or releasing its allocation. A cleanup must capture and
compare its exact ledger rows and preserve already committed synthetic usage.

## Source identity and reproduction

The actual binaries were built under the frozen revision
`bdff9b1b511ae1c38327b8441ac973bb4db262e2`. The archived preflight pins their
paths and hashes; the aggregate before/after input hashes are equal:

| Binary | Recorded SHA-256 |
| --- | --- |
| `application-faults.test` | `fc4937bf5ad4e7a00cb55f16272e47bb9fd11f6223a0cced896982c3cab3c041` |
| `forge-runner` | `29a58dddf3a00aa063a1a7f2d7b07735f63edbf1cf0600f29c2d3d2943f94013` |
| `forge-worker` | `e7e02e06be5687d4c76e6c650bcd75446b61609d091f437b7ecce4b088c2f3e4` |

The diagnostic production/test source copies came from that Git revision.
`initial-static-review.json` and `initial-audit.py.txt` preserve the initial
diagnosis unchanged, including its then-unresolved read-only observation
requests. `observation-review.json` supplies the later raw-supported answer.
The exact observer script and `isolated_state.py` match the source hashes
recorded during observation. The latter was recovered from commit
`b837b4e40f396af3a3851f6c401598acb007512e`, since that worktree subsequently
changed branches. `isolated_checks.py` is included from the same commit as
dependency context; it was not independently hashed by the historical observer.
This archive does not add a new clean-build or process-session attestation.

Run only the portable verifier to recompute the archive bindings and diagnosis:

```sh
python3 benchmarks/results/strict-logs-third-failure-20260912/verify.py
```

It reads this archive only; it neither extracts files nor accesses live
PostgreSQL, Docker, processes, runtime configuration, credentials, or models.
The original 602-member failure remains unchanged even when this archive audit
passes. `seal.json` hashes the tracked archive files, excluding itself.

The original SDE plan's S12.9 paragraph and fault-evidence requirements do not
require all six cases in one invocation. A later **complete targeted L4** may
be transparently composed with these five case records if production policy,
build/configuration compatibility, pool/journal identity, old-work settlement,
and full L4 publication/release/health oracles are verified. The original suite
must remain failed. No such targeted execution or cleanup is claimed here.
