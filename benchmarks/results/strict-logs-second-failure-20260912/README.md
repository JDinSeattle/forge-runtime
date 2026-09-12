# Second strict-log attempt: failure before runner start

The actual `logs-02` attempt at revision `0a5f0f47100941858ae12b07ecd9bb094bdca833` **failed after 2.84 seconds**, after creating a new private PostgreSQL schema and submitting one run. Its fixed `strict-log-publication.json` filename already belonged to `logs-01`; exclusive creation failed before the L1 runner or worker launch. Only L5 completed. The failed outcome and all original evidence remain unchanged.

The first independent audit verified 30 conditions, including every member of the 15-member original manifest. The preflight and L5 journal representations exactly match the earlier cleanup's released journal: 44 terminal operations, 42 removed log records and four released workspaces/leases. Source ordering and the precise recorded failure place the error after Submit and before any own runner/worker start. That first audit explicitly left post-failure live state unverified.

A separate executor observation at **2026-09-12 06:10:29 UTC** filled that gap. Its two PostgreSQL repeatable-read/read-only transactions show the same full run snapshot:

- Run `run_VZZ7CZCKDG65KNL3BW7ZH7F2O4`, tenant `sl-L1-d7wtgoelviiujwjs3rrqiragqp`.
- Private schema `appfault_strictlogs_ggl4ub23utvo6qnlzlkgroms5n`, observed OID `852430`.
- Queued, version 1, epoch 0; lease owner, runner and workspace unset.
- Exactly one `run.created` event and one initial snapshot; no steps, effects, attempts, artifacts, allocation or reservation rows, or quota use.
- Exact original service recorded absent, inactive, MainPID 0.

The later independent review passed 43 conditions against those retained results and the exact observer sources. These are historical observations at the recorded time, not a new current-state query. The reviewer did not open a database, call Docker/systemctl, read credentials or invoke a model.

The observer compared every listed live journal table in one SQLite read transaction, normalizing row/JSON order and omitting transient `request_json.grant`. Only after equality did it save the corresponding original canonical rows. The archived `journal.json` is therefore an explicitly projected observation, not a raw live-byte dump or proof that transient capability bytes were identical. PostgreSQL and SQLite observations are sequential, not an atomic cross-system snapshot.

No Cancel, workspace cleanup, or retry in the failed schema occurred. Its original deadline and queued task remain retained. A future third attempt must use a new exclusive evidence directory, private configuration and schema, never route this run to a worker, use a per-run fault filename, and recheck the old failed/observed authority and original cleanup before starting new work. This archive does not establish that attempt's execution or success.

`raw-evidence.tar.gz` contains 27 selected raw members. Twenty-six are byte-identical; `L1/fixture.json` explicitly omits `worker_config_sha256`, a private configuration digest, and carries a projection marker with its original file hash. The original manifest is retained verbatim; `archive-manifest.json` maps both source and archived hashes. No private runtime configuration, credential content or digest, signing-key bytes or database file was copied. Nonsecret signing-key **paths** and empty persisted grant fields remain as recorded.

The observer's `isolated_state.py` was copied and hash-checked while it still matched the observation's recorded SHA `05044e79c1d1eb3ca42be3a0f6547172501eed0bf6b149f0b096bfd396a3b680`. Subsequent worktree changes do not relabel that executed source. Source copies use `.py.txt`/`.go.txt`.

Run the portable, entirely offline check:

```sh
python3 benchmarks/results/strict-logs-second-failure-20260912/verify.py
```

It verifies 147 conditions against this archive. Successful evidence verification does not change the original test's FAIL outcome, close S12, or constitute a paid-model result.
