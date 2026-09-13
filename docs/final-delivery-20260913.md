# Delivery verification — 2026-09-13

The public repository is [JDinSeattle/forge-runtime](https://github.com/JDinSeattle/forge-runtime).
Code commit `3be62da746b4bc4e6e01b6aabe7cc97346f46f7a` passes
[GitHub Actions 34774974520](https://github.com/JDinSeattle/forge-runtime/actions/runs/34774974520).
The [retained CI record](../benchmarks/results/final-ci-20260913/run.json) includes
build, all Go tests, race, vet, generated contracts and seven Python helper groups.
CI runs in a fresh checkout with isolated PostgreSQL. Earlier failed/cancelled
runs remain visible; no previous result was relabeled.

Actual fixed-volume log-pressure acceptance and paired PostgreSQL/SQLite/ext4/
artifact recovery passed. [Real DeepSeek measurement](deepseek-live-evidence.md)
collected all four tasks but yielded 0/4 verified repairs and $0.196610 unresolved
conservative exposure. Exact vendor billing and a live run of the subsequent
slot-release fix remain unverified. Those limitations are not hidden by the green
CI: protocol/implementation validation and model quality are distinct.

Prior scoped independent reviews, the 140-row implementation map, actual runtime
and recovery evidence, and this final CI constitute the bounded delivery audit.
The 13 optional extension rows remain deferred. This is a portfolio engineering
baseline, not a claim of production adoption or universal recovery guarantees.

The operator authorized making the repository public. A scan of 3,174 reachable
Git blobs found no matched live provider key/private CLI token or generic private-key
pattern. The only known-credential matches were the documented development
PostgreSQL default already shipped in sample configuration. This is a bounded
credential check, not a guarantee about every possible secret format.

Six project services stopped normally with inactive/dead state, PID 0 and success:
API, both original workers, original runner and the two dedicated rootless daemons.
The final host-only step is `scripts/workspace-volumes/close-project-volumes.py`,
run by the operator with sudo and Python isolated mode. It parks exactly the four
recorded pools (16 volumes), retaining images/files/logical leases and refusing
busy or foreign namespace use. At this document boundary, operator execution and
confirmation of those unmounts are still pending; services stopped is not the
same claim as volumes unmounted.

This record is a documentation-only follow-up to the tested code commit.
