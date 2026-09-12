# Targeted L4 correction: offline tests and independent review

This is **offline implementation/review evidence**, not an actual ENOSPC,
recovery, or targeted L4 execution. The real logs03 suite remains failed and
its five recorded passing cases retain their original evidence. No acceptance
status is promoted by this archive.

The final seven root-owned files were committed as
`1b4e0b482fe14aaf834da8e6968b15e4afab0d9a`. The archivist independently matched
all seven Git blobs to `freeze-v2.json`. `source-v1/` and `source-v2/` retain
the complete source sets as `.txt` data, with the two original freeze manifests.
`archive-manifest.json` maps every preserved source to its byte-identical
archived file. There are **26 preserved files**, with no redactions or projections.
The archive selects only source, review reports, test output and nonsecret
manifests; it reads no runtime configuration, key, environment or credential file.

The changes replace the insufficient 1 MiB pressure test with bounded
1 MiB → 4 KiB → 1 byte writes, syncing each ENOSPC stage and requiring zero
available filesystem blocks. The complete targeted L4 fixture additionally
retains raw spool bytes and metadata, requires the original prefix below the
operation-byte ceiling, and observes a nonzero, non-OOM early stop before the
immutable deadline while the actual worker remains paused. It retains the
publication/download/cleanup/four-slot verification/health steps. Preparation,
opt-in, test name, output directory and final-result checks distinguish this
single L4 run from the unchanged six-case combined gate.

## Preserved outcomes

| Evidence | Result and scope |
| --- | --- |
| First author Python run | 35 tests, **one error**: the offline test pre-created a recovery directory and correctly hit the exclusive preparation guard. Original traceback retained. |
| Corrected author Python run | **35 passed** after fixing that test setup. |
| Author initial Go race | **104 passed, 4 opt-in skipped**. |
| Author guard delta race | **1 passed**. This was before the final SQL literal changed from two named unresolved states to all nonterminal states. |
| Author vet | Empty raw log retained; exit 0 was reported by the author. The empty file alone is not exit-status proof. |
| Independent affected Go race | **8 passed** against freeze v2, including compilation of the final SQL literal. |
| Independent new Python cases | **2 passed**, exercising phase/result isolation and exclusive preparation. |

No existing full suite was rerun by the archivist. JSONL pass counts include
Go subtests and exclude the package-level pass event. The initial Python run
predates freeze v1; its exact pre-fix source snapshot was not available. Its
original traceback/result is preserved without attributing it to a later source
version or reconstructing an invented historical file.

The first independent review reported two P2 gaps:

1. The continuation read live SQL by schema name without verifying the original
   namespace OID, allowing a same-name replacement to be confused with the old
   recovered authority.
2. The idle query omitted unsettled effects, so a terminal run and zero counters
   alone did not establish that the old effect ledger was closed.

The v2 correction binds recovery tenant, schema and **OID 852843**, verifies the
live namespace OID as the first query before business-table reads, requires
effects to be `succeeded`, `failed`, or `cancelled`, requires settled/released
model request slots, and requires released workspace-cleanup rows. It also fixes
the stale documented payload size to **2,457,622 bytes**. The independent v2
report found **no remaining P1/P2 in these seven files**. Both reviews and all
earlier source/test results remain unchanged in the archive.

The live SQL clauses were source-reviewed and compiled, **not exercised against
PostgreSQL** in this review. The recovery producer was still separate WIP; only
its report-field contract was provisionally checked. Its final source must be
included in the eventual combined frozen build. The 18-second pause and bounded
observation windows can still fail honestly on a slow host; offline tests do
not establish runtime latency or actual ENOSPC behavior.

Recompute this archive without running tests or accessing live resources:

```sh
python3 benchmarks/results/strict-logs-targeted-l4-offline-20260912/verify.py
```

`archive-audit.json` is the resulting portable check. `seal.json` hashes every
archive file except itself. Source/hash matching is evidence integrity, not a
new clean-build attestation or actual policy-stop acceptance.
