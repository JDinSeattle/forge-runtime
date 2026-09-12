# One isolated eval-v2 collection

`evaluate.py --deployment /absolute/private/deployment.json` binds execution and collection to one private PostgreSQL schema, a pinned CLI and one fixed `eval-01` output. It does not provision roles, start processes, install a key or call a provider during local validation. This extension has offline tests; real PostgreSQL/CLI integration and paid model quality are still unverified.

The descriptor format is [deployment.template.json](../scripts/evaluation/deployment.template.json). The null values are required operator inputs, not executable defaults. Only its documented keys are accepted; duplicate JSON keys are rejected. The descriptor and client environment must be owned regular files with private permissions; paths must be canonical and contain no symlinks. The descriptor never contains credentials or their hashes.

The exact descriptor fields are:

| Field | Required binding |
| --- | --- |
| `version`, `purpose` | `1`, `isolated-eval-v1` |
| `authorization_id` | `deepseek-flash-2usd` |
| `output_dir` | Absolute fixed output with final directory `eval-01`; its existing parent must be private and owned. The output is created exclusively by `--execute`. |
| `bundle_dir` | Absolute prepared candidate directory, separate from the output tree. |
| `candidate_sha256` | SHA256 of the exact `candidate.json` bytes, which already seal platform/runner/registration inputs. |
| `registration_sha256`, `corpus_sha256` | Exact candidate seal values; normal bundle verification also checks the underlying registration bytes and fixed corpus. |
| `provider`, `model_id`, `total_budget_microusd` | `deepseek`, `deepseek-v4-flash`, `2000000`; registration must retain four allocations of `500000`. The request alias does not pin a returned server model version. |
| `tenant`, `api_url` | Exact valid tenant and HTTP origin `http://127.0.0.1:<port>`, without credentials, path, query or fragment. |
| `database` | Exactly `host=127.0.0.1`, `port=32773`, `name=forge`, `schema=eval_ds_<8–40 lowercase hex characters>`, positive `schema_oid`, and exact lowercase `audit_role`. |
| `cli` | Absolute owned executable `path` and SHA256. The collector rechecks this identity before every CLI operation, including watch. |

Provisioners should write the final descriptor once after schema/OID and file identities are established. A launcher must separately prove which binaries/configurations the actual API and worker loaded. The collector's descriptor is an operator-owned evidence binding, not process or remote attestation.

Existing candidate preparation remains unchanged. Local validation, after the descriptor has been populated, is:

```sh
python3 -I /absolute/source/scripts/evaluation/evaluate.py \
  --deployment /absolute/private/deployment.json \
  --bundle /absolute/candidate --output /absolute/evidence/eval-01
```

This prints the credential-free binding and performs only local validation. `--execute` additionally requires `--client-env /absolute/private/client.env` and a privately injected `FORGE_EVAL_AUDIT_DSN`; it retains the existing four-task sequential submission, per-task caps, idempotency receipts and stop-on-unknown behavior. `--collect` uses exactly the same descriptor and output, and never submits. The complete descriptor plus its original byte hash is saved in `batch.json` and the report. Collection rejects omission or replacement of this binding. A second execution at the fixed output fails rather than starting another batch. There is no fallback to `public` or a different CLI when isolated validation fails.

The audit DSN must identify the descriptor's exact role at `127.0.0.1:32773/forge`, with only an optional literal `sslmode=disable` query. It must not carry a `search_path` or options parameter. All business tables in `audit.sql` are qualified with psql's quoted identifier variable `:"schema"`; the process search path is only `pg_catalog`. Audit transactions remain read-only. Before any API submission, and again for each run audit, the SQL result must prove the expected database/schema OID/current and session role, SELECT permissions, no DML grants and no owner/superuser/BYPASSRLS role. The audit role needs SELECT on `runs`, `model_attempts`, `quota_reservations`, `artifacts` and `run_events` in the exact private schema. RLS uses the exact tenant set in the audit transaction.

The isolated CLI receives only its three client entries and a minimal PATH/locale/temp environment. Audit psql receives only its own connection fields and the same minimal process environment. Provider keys, worker/admin DSNs, inherited PG service/options and shell startup settings are not forwarded. No credential content or digest enters the descriptor or report.

A model attempt without exactly one matching reservation now fails ledger auditing in either mode. Duplicate/orphan reservations, wrong credential group, invalid status or money fields, and a settled reservation without definitive cost are also rejected. Unknown or unfinished usage continues to retain its conservative reserve; an audit failure retains the submitted task allocation in aggregate exposure rather than counting that task as zero. The existing synthetic quote-serialization fixture was updated with a matching reserved row to satisfy this stronger invariant; all previous quote omission/zero assertions remain.

When `--deployment` is omitted, the original public-schema/checkout-CLI path remains available and is labeled `legacy-public` in validation and reports. Its preflight continues to accept the existing SELECT-capable role semantics. Legacy mode cannot collect an isolated batch without the original binding. The public path is not suitable for the dedicated isolated deployment.

The fixed output prevents accidental second batches using this descriptor. It is not a general entitlement service or a lifetime provider quota. Deleting or replacing operator-owned records is outside this guard. The platform's rolling quota can renew; the operator must preserve the one-batch authorization and must not reset it by creating another descriptor/schema/output. The $2 value remains a conservative authorized ceiling, not a verified invoice. Provider billed totals remain unknown until independently reconciled.

Offline validation: 26 tests passed before the final validation-output wording change; final source is checked again in the accompanying freeze record. Tests cover descriptor/schema/OID/role/API/CLI/output substitutions, symlinks, duplicate or secret-bearing keys, changed candidate/registration allocations, environment isolation, missing/duplicate/unknown-cost reservations, and rejection of changed/omitted batch bindings. SQL execution is mocked; these tests do not prove PostgreSQL syntax, live ACL behavior or actual model calls. The original subprocess lifetime test starts only a local sleeping process tree, and the existing Go serialization/source-hash helpers run with module network access disabled.

The follow-up binding revision addresses two independently reproduced defects in the first collector release. A saved batch now has an exact supported envelope: corpus hash, fixed task order, both budget totals, registration/seal, deployment binding and a timezone-aware creation time. Reports derive budget fields from the validated registration. Changing a redundant batch cap or task order cannot produce a valid report.

Before collecting a case, the collector binds its saved project, submission intent and submission ID to that fixed case's source/profile, tenant and allocation. It verifies the actual saved CLI receipt's scoped filename, key, exact request-body hash and returned run ID. The request serialization is tested against the production generated Go Submit/Budget type, including Unicode/HTML escaping. GET and SQL must agree with the expected project/run/task/base and frozen model/budget limits; SQL's immutable admission snapshot must contain the expected source/profile. The audit still reads only the original five authorized tables, with additional columns from `runs`. Duplicate run IDs across case directories are rejected.

The new [binding evidence](evidence/isolated-collector-bindings-20260912/freeze.json) retains the independent failing report and both original synthetic counterexamples unchanged. Thirty offline tests passed after these fixes (3.229 seconds). Real PostgreSQL/API integration and independent delta review remain separate. A missing CLI receipt or incomplete submission evidence stops collection and retains the submitted allocation; it is not repaired by inventing another key or crediting a different run.
