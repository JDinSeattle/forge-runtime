# Private exact-schema evaluation provisioning

This operator helper provisions the fixed DeepSeek Flash eval-v2 registration. It has **not yet been executed against PostgreSQL**. Offline validation does not prove that the grants/migrations have passed on the host; independent review and an explicitly authorized host run remain required.

Build from the same frozen source revision as the candidate deployment:

```text
go build -o /new/evaluation/bin/eval-provision ./scripts/evaluation/provision
```

The sole invocation is:

```text
FORGE_EVAL_ADMIN_DATABASE_URL=<privately injected loopback admin URL>
/new/evaluation/bin/eval-provision -candidate /new/evaluation/candidate -output /new/evaluation/private
```

No DSN or credential is accepted in argv. The URL must explicitly name a password-bearing administrator identity at `127.0.0.1:32773/forge`, with `sslmode=disable`, no caller-supplied search path or routing overrides. Ambient `PGSERVICE`, `PGSERVICEFILE` and `PGOPTIONS` are rejected. The helper derives a fresh exact-schema URL. It never reads a model key or signing key and never calls `BuildProviders`, `Engine.Open`, a service manager, Docker or a shell.

The candidate must be the existing `prepare.py` bundle: `platform.json`, `runner.json`, `registration.json` and `candidate.json`. Their hashes must agree. The candidate/output are sibling directories under one new `lr20260912_a/evaluation/<evaluation-name>` directory. The helper reads `lr20260912_a/runtime/runner.json` as nonsecret authority and requires all runner fields except source/profile maps to match. It does not open the existing journal, key, owner records, artifact files, mounted volumes or image files, and never rewrites them. Actual pool quiescence, mount/UUID checks and launching belong to the separate reviewed workflow.

The policy is intentionally narrow: requested model `deepseek/deepseek-v4-flash`, the reviewed `deepseek-flash-peak-ceiling-20260912` conservative quote with `exact_pricing=false`, one worker slot, the original runner identity/UDS and four slots, four fixed eval-v2 cases at 500,000 microUSD each, 2,000,000 microUSD total. Each run retains 20 model rounds, 64 tools and 600 seconds. There is no fallback, fake provider, native compaction or alternate registration. The alias remains a request alias; this helper makes no assertion about fixed server weights or returned native model versions.

A new, exclusively created mode-0700 private directory contains:

| File | Contents |
| --- | --- |
| `api.env` | `FORGE_DATABASE_URL` for the API role |
| `worker-db.env` | `FORGE_DATABASE_URL` for the worker role |
| `audit.env` | `FORGE_EVAL_AUDIT_DSN` for the audit role |
| `client.env` | `FORGE_API_URL`, `FORGE_TENANT`, short-lived `FORGE_TOKEN` |
| `recovery.json` | Credential-free planned schema/role/resource identity |
| `stage-*.json` | Append-only resource creation/check checkpoints |
| `failure.json` | On failure, fixed stage and SQLSTATE; uncertain resources remain retained |
| `provision.json` | Only after all checks pass, final nonsecret identity |

All files are created exclusively as mode0600, fsynced with their parent directory. `client.env` contains three plain literal values for the collector, with an exact loopback origin and validated tenant/token alphabets. The API, worker and audit DSN files retain shell quoting; a launcher must parse those assignments without executing a shell. No secret file is overwritten, hashed, logged or copied into a descriptor. The helper never retries provisioning into another directory or drops, resets or cleans any schema/role. An interrupted or failed attempt is retained for read-only diagnosis; rerunning against its existing output is refused. PostgreSQL errors are reduced to a fixed stage and SQLSTATE because their text can contain SQL/password details.

The random namespace is `eval_ds_<32 lowercase hexadecimal digits>`; API, worker and audit roles append `_api`, `_worker`, `_audit`. Only this schema is created/migrated. All identifiers are validated and quoted; no PUBLIC schema grant is changed. Migration verification records its OID, every table OID, the embedded migration version and the exact-schema membership-lock function OID. It checks the function's owner, SECURITY DEFINER, fixed catalog search path, schema-qualified membership access and absence of PUBLIC execute permission.

The API/worker roles cannot own tables, create roles/databases/schemas, inherit membership, or replicate. API has no BYPASSRLS; worker requires explicit BYPASSRLS. Both have business-table CRUD only in the new schema, no migration-table access or auth-table writes; only API can SELECT API tokens. Both receive EXECUTE on the new schema's migration10 membership-lock function, which is actually called against the fresh developer membership during provisioning. Audit has SELECT only, no token access or membership-function execution, a supplementary read-only transaction default, and a role-default `pg_catalog` search path. The exported audit DSN contains only `sslmode=disable`; the collector sets its fixed `pg_catalog` path and fully qualifies the descriptor schema. Provisioning temporarily supplies the exact new schema in its internal audit-role probe connection, independently of the exported minimal DSN. The helper validates every positive/negative table privilege and rejects any effective access to non-system tables outside this namespace instead of modifying unrelated grants.

The fresh tenant has `max_active=1`, developer membership, a two-hour API token, and one runner record with four slots. Credential group `deepseek-flash-evalv2` starts with one concurrent request, 10,000,000 token capacity, 2,000,000 microUSD, a 24-hour window, failure threshold3 and 10-second breaker cooldown. Initial counters/generation/window data and an earlier two-hour `launch_not_after` are recorded. It refuses an existing quota row before calling the existing `Configure` method. The final check requires no projects, runs, attempts or reservations and zero active capacity.

`provision.json` uses `schema_version=1`, `purpose=deepseek-eval-v2-provision`, `phase=ready`, `schema`, `schema_oid`, `roles`, `tenant_id`, `principal_id`, `api_url`, `runner_id`, `runner_endpoint`, `runner_slots`, `worker_slots`, `provider`, `model`, `config_id`, `credential_group`, `price_version`, `exact_pricing`, `batch_id`, `budget`, `database`, `candidate_dir`, `candidate_hashes`, `migration_version`, `relation_oids`, `membership_function_oid`, `role_checks`, `initial_quota`, and deadline timestamps. `batch_id` is deterministic from the registration digest, not a renewed entitlement. The quota window is not a lifetime budget or a vendor invoice. This helper authorizes no model call; the bounded collector/launcher must bind one immutable batch, enforce its four allocations and launch deadline, and retain unknown exposure. Creating another candidate/output manually does not grant another paid batch.

Offline checks are in `provision_test.go`. They exercise missing admin input, no key/runtime access, route overrides, exact cost/model limits, fallback rejection, authority mutation, symlink/hash changes, exclusive output, private file modes/quoting, identifier constraints, and complete/negative privilege decisions. Initial compile and test failures are retained alongside the final run in `benchmarks/results/eval-provision-offline`; no host database was contacted.
