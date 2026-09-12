# Independent isolated-evaluation preparation and launcher review

This archive preserves two independent code-review results. The local preparation script `01c55705` and launcher delta `b837b4e` passed their bounded offline reviews. This does not record a build, provisioning, service launch or model evaluation.

The build review includes the earlier three blockers and the narrow corrections: Git-bound main-source permissions, helper identity, the exact original UDS, and readable trusted corpus files inside the newly created private evaluation tree. The temporary filesystem probes touched no original file permissions.

The launcher review independently ran 58 Python tests, recomputed its author archive and 577 retained nonsecret identities, and verified every one of the 595 inputs in the failed logs03 acceptance. Ten mutations of copied original PostgreSQL JSON were rejected with the SQL boundary mocked. The old E50-input omission is retained as a before-negative and is closed by the pinned material graph.

At review time logs03 retained five passed cases and no passed L4; its acceptance remained false and its final-journal file was absent. The launcher correctly rejected that evidence. No paid evaluation or successful six-case acceptance is inferred. Any later L4 recovery/composite contract is outside this archive.

`file-map.json` preserves each original path and exact bytes. `manifest.json` covers all archive files except itself. Files ending `.py.txt` contain reviewed/recomputation source only; the original reports have not been rewritten to point to new locations. The copied nested manifests refer to their original source filenames; the outer file map and manifest account explicitly for the `.txt` archive suffixes.
