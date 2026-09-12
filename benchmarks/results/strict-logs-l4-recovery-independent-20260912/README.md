# L4 recovery: preserved independent offline review

This archive preserves Boyle's independent review of the proposed recovery for
the failed logs03 L4 operation. **It is offline evidence. Actual recovery and
the new targeted ENOSPC acceptance had not run when this review was produced.**
The original failed logs03 result remains unchanged.

The original **28-member** `independent-manifest.json` is unchanged, with SHA-256
`607d1d7b6c56212f32b8590d76578fcde09a6258dc44f65d258e71fe739a671d`.
Every listed member was independently rehashed before copying. The v2 report,
also supplied separately, is byte-identical to `raw/delta-v2/review-v2.json`:
`fd2374794f93c386ea5dd0fdb94dea2e562cefa304dcac5fa83c101d4b139ef0`.

`archive-manifest.json` maps **31 byte-identical preserved files**: the original
28 members plus their manifest, the separately supplied report, and one exact
previously archived `isolated_state.py` source helper. There are no projections
or redactions. No runtime/private configuration, credential, key, or its digest
was opened. The helper matches the original observer's recorded source hash;
it supplies source context without reopening the changed worktree or executing
the historical observer. The finite credential-pattern scan passed.

## Results and retained failures

| Evidence | Result |
| --- | --- |
| v1 ordinary tests | 38 passed; actual recovery opt-in skipped |
| v1 race tests | 38 passed; actual recovery opt-in skipped |
| Independent saved-lease counterexample before fix | **Failed**, with its original output and overlay source retained |
| v2 ordinary tests | 58 passed; actual recovery opt-in skipped |
| v2 race tests | 58 passed; actual recovery opt-in skipped |
| Same saved-lease counterexample after fix | **Passed** under race instrumentation |
| v1/v2 vet | Recorded exit 0 in command JSON; original empty output logs retained |

Go pass counts include subtests and exclude package-level pass events. This
archivist parsed the saved results and **did not rerun any of these tests**.
The ordinary suites passed even before the independent counterexample revealed
the missing behavior; both facts are retained.

The first review found two P2 issues:

1. It confused the total number of cleanup rows with the number still requiring
   cleanup. The original L1 cleanup row already existed in `released` state,
   so expecting total counts of zero before and one after L4 was incorrect.
2. It compared a full `GetRun` state directly to an older JSON snapshot without
   allowing the SQL-authoritative heartbeat lease overlay. The saved snapshot's
   lease expiry was `2026-09-12T18:24:46.778095Z`; SQL legitimately held
   `2026-09-12T18:24:59.737432Z`, **12.959337 seconds later**. The independent
   test used that saved real observation, without querying PostgreSQL again.

The v2 fix protects the complete original released L1 row, scopes target cleanup
counts, rejects unresolved cleanup, and binds both the raw snapshot and exact
observed SQL lease to the original tenant/run/schema/OID. Its local comparison
normalizes only the lease overlay on a copy. It does not mutate the database
lease, operation deadline, original snapshot or failure record. The same saved
lease case then passed, with additional negative cases for altered authority
and snapshot fields.

The preserved review moved from **two confirmed P2 findings** at
`e9793ea654d4c2e43d410eca8fab3c1a65e4fe98` to **no remaining confirmed P1/P2
within this scope** at `0cc916cdeb9fd6def5ce5dca198924152cf37ce5`. Both complete
sets of three source files, overlays, command environments, logs and reports
are preserved verbatim. Upstream author-archive checks, the 614 saved input
checks and production-diff assertions remain attributed to that review;
this archive does not claim to rerun those external checks or add a clean-build
attestation.

## Portable verification and scope

```sh
python3 benchmarks/results/strict-logs-l4-recovery-independent-20260912/verify.py
```

The verifier reads only relative archive paths, checks all file hashes, source
and command/log bindings, parses test counts, and independently compares the
saved SQL/snapshot lease timestamps. It performs no tests, subprocesses, live
database/SQLite/Docker/service/volume/credential/provider operations. Absolute
paths inside the original command and overlay records are historical data and
are intentionally unchanged; source `.txt` files are not execution entry points.

`archive-audit.json` stores the portable result; `seal.json` hashes all archive
files except itself. Successful actual recovery would only close the old
retained work. It cannot turn the original ENOSPC failure into a pass or by
itself complete S12.9 or authorize a paid evaluation.
