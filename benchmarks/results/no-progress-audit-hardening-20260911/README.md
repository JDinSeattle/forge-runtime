# E42 offline auditor binding correction

Independent review found three accepted counterexamples in the original auditor:
an SQL effect changed to unknown while its receipt still says succeeded; a middle
context's input/output counters changed together without changing its committed
predecessor; and a current run marked completed while its last snapshot remains
budget_exhausted. The original successful records have not shown those mutations.

This directory preserves copy-only mutated records, the independent findings,
the exact before/after auditor sources, and regression results. The three named
counterexample JSON files are deliberately corrupted copies, **not new execution
results**. `before-identity.json` identifies the untouched source report and old
archive manifest. No file inside `no-progress-20260911/` was changed.

The fixed auditor requires matching terminal SQL/receipt status and scope. It
requires a complete consecutive snapshot chain from version 1, matching each
input state to its preceding committed body. Only a guarded `context_built`
transition may change progress; its output is independently recomputed as before.
The current run must equal the last snapshot. Only `lease.until` is excluded from
state equality because SQL heartbeat refreshes that field independently; owner,
epoch, status, progress and every other field are compared.

The same 10 regression methods produced 11 failing negative subcases before the
fix and passed after it. This includes all nine untouched final E42 records,
three final-projection mutations, terminal-status variations, the original middle
counter attack, a non-context reset with linked adjacent inputs, and the boundary
between permitted heartbeat deadline differences and forbidden owner/epoch changes.
The three review counterexample files are accepted by the retained old auditor
and rejected by the new one; `counterexample-results.json` records both outcomes.

The updated auditor still validates all nine final reports and reconstructs the
same 41 committed guarded context boundaries. All 117 files referenced by the old
archive manifest and the manifest bytes themselves remain unchanged. This is an
offline correction of evidence validation, not another PostgreSQL run, independent
build, external receipt signature verification, or a general reducer reimplementation.
Exports missing earlier snapshots are now rejected rather than treated as a
complete progress proof.

```sh
python3 -m unittest discover -s scripts/progress -v
python3 scripts/progress/audit.py benchmarks/results/no-progress-20260911/final
```

`manifest.json` hashes only this new correction directory. It does not replace or
revise the prior evidence archive's manifest.
