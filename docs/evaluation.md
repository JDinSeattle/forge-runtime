# Fixed CLI repair evaluation

This harness prepares a small, fixed evaluation through the existing native provider adapters, application driver, CLI, isolated runner and trusted verification profiles. As of 2026-09-11, the **16 source/oracle grader combinations passed in real Docker containers**. No real model was called for this corpus. A model, its exact capabilities and prices, credentials and an authorized total budget are still required before reporting model repair quality.

## Corpus and fixed acceptance

`scripts/evaluation/corpus/manifest.json` freezes the case order, every source/oracle/task/grader byte and executable bit. The current revision is `eval-v2`, SHA256 `80686555b5ca55607fde2d18fa4bec792d8b2ac760b1126d93d593f86d2cdc26`. Each case gets one submitted run in this order; the run may use only its registered bounded rounds and tool calls. No best-of selection or replacement task is used.

| Case | Target behavior | Regression checks | Oracle target / regression assertion counts |
| --- | --- | --- | --- |
| `py-utf8-chunks` | Greedy UTF-8 byte-limited chunks without splitting a code point | ASCII, empty input and invalid limits | 4 / 5 |
| `py-latest-records` | Last record per key, ordered by last occurrence | Unique records, empty input, input preservation and independent top-level results | 3 / 4 |
| `go-midpoint` | Mathematical floor midpoint over the full signed int64 range | Ordinary intervals, invalid ordering, arguments and parsing | 4 / 6 |
| `go-rune-rle` | Run-length encoding of Unicode code points | ASCII, empty input and CLI argument contract | 4 / 5 |

The four cases are held out from the existing `clamp`, `expiry`, `ranges` and `go-ceil-div` demo scripts. This does **not** establish absence from model pretraining, representativeness of production repairs or statistical significance. The corpus is published and deliberately small. Any change after model evaluation begins requires a new revision and separate results.

Only each `source/` is registered as a workspace source. Its `task.txt` is submitted as the task. The `oracle/` solutions are never imported into an agent workspace. Expected values and grader programs remain in an operator-owned directory mounted read-only at `/forge-tests` for the trusted verifier. A successful alternative implementation need not match the oracle's source bytes. Model-authored claims of passing tests do not count.

Python runs isolated child interpreters for candidate calls. Go builds with the local toolchain, dependency downloads disabled, a constrained child execution and a trusted parent comparing exit status and exact output. Both profiles use 256 MiB container memory, one CPU, 64 PIDs and the production runner's 64 MiB `/tmp`; only Go requires executable tmpfs. The corpus source/oracle import set fits these limits. A model adding a larger dependency can still fail compilation; that is an unsuccessful evaluation outcome, not evidence of a target assertion failure.

## Actual oracle evidence

The first `eval-v1` run is retained in `benchmarks/results/evaluation-oracle-20260911/`. Python's eight combinations passed. Go's first two compilations failed with `ENOSPC` while compiling `fmt` inside 64 MiB tmpfs. The overall report is false. The initial exit-code-only harness listed the first compilation exit 1 as matching an expected red target; that entry is **not** valid assertion evidence.

Before any model calls, v2 removed incidental `fmt`/`strings` formatting dependencies from the four Go source/oracle files. Algorithm requirements and grader cases were unchanged. The original manifest and exact four original files are archived in `scripts/evaluation/revisions/`; an offline test reconstructs v1's hash, `e6e2a4d7a7569cc49aa6f3b0d819d0e008cc09c1c440b1cd7aa3a37f3d99289d`.

The corrected oracle harness requires one structured grader result identifying the case and suite. Expected target failure additionally requires a nonnegative executed assertion index; success requires a positive assertion count. Empty output, compiler/start errors and malformed output fail the harness.

The actual v2 report is `benchmarks/results/evaluation-oracle-v2-20260911/report.json`, with `passed=true` and 16 combinations. All four defective sources fail a target assertion and pass regression; all four oracle solutions pass target and regression. Per-combination elapsed time, exit status, parsed grader result, raw stdout/stderr, image identity and command log are retained there. These times are grader/container timings, not model latency. `scripts/evaluation/revisions/oracle-evidence-20260911.json` records independently recomputed hashes of both evidence directories.

Reproduce from the repository root with new output paths:

```sh
python3 -B -m unittest discover -s scripts/evaluation -p 'test_*.py' -v
python3 -B scripts/evaluation/oracle.py \
  --docker-host unix:///run/user/1000/forge-runtime-docker.sock \
  --python-image python@sha256:229a2c5bfa27522db7815ea81f9bed70af17ccb9de9fc7ad142b1877b5830d36 \
  --go-image golang@sha256:9fdc884aacc3bec89b20ffc69f4bb369c78210e3e4f600387b5128b12c199f81 \
  --output benchmarks/results/evaluation-oracle-new-run
```

The images must already exist in the explicitly selected daemon. The harness uses `--pull=never`, no network, no capabilities, no-new-privileges and read-only source/grader mounts. It removes only its own uniquely named containers. It never calls a model, modifies runner configuration or takes a workspace slot. Offline tests use explicitly synthetic model names and rates solely to check validation and serialization; they are not price quotes or model results.

## Operator registration and candidate preparation

Copy `scripts/evaluation/registration.template.json` into a private operator directory and fill every required value. The template intentionally supplies no model ID, prices, capabilities or spending limit. Supported native adapters are `openai`, `anthropic` and [DeepSeek](deepseek-provider.md); fake providers are rejected. Use a fresh config ID. Capability flags describe an exact model; registration does not imply that every advanced native feature is implemented by the adapter.

Amounts are integer micro-US dollars: 1 USD is 1,000,000 micro-USD. `input_price`, `output_price` and optional cache rates are micro-USD **per million tokens**. `price_version` identifies the immutable quote. Record official price and capability URLs in `price_sources` and `capability_sources`, and verify their applicability to the exact model and billing mode before execution. `request_timeout_ns` is an integer duration in nanoseconds. `context_tokens` is this runtime's conservative input reservation/dispatch bound, not simply a prompt tokenizer estimate; serialized tool definitions and native state also consume the bound. Requested input plus output must fit the registered native context window.

With `exact_pricing=true`, supply the complete applicable rates. Exact Anthropic quotes require `cache_read_price`, `cache_write_5m_price` and `cache_write_1h_price`; add these integer fields to the template. Omit genuinely unavailable optional cache rates instead of using JSON null. A missing rate can leave usage settlement unknown. With `exact_pricing=false`, input/output prices must be positive conservative ceilings covering their applicable billing classes; these authorize reservations and do not represent a confirmed vendor invoice. Explicit zero cache prices remain distinct from omitted prices; an offline test round-trips the actual production `application.ModelSpec` JSON type to check this distinction.

Give every task an explicit positive allocation. Their sum must be at most `total_budget_microusd`; one task cannot borrow another's unused or unresolved allowance. Each allocation must fund at least one conservative request reservation. The harness sends the task's allocation and tighter runtime/round/tool limits through the CLI, while the platform enforces each run's configured limits.

Credentials belong only in the operator's existing worker secret environment. Do not put API keys or database credentials in registration JSON, task text or published evidence. Unknown registration fields are rejected. `credential_group` is a quota identity, not an API key.

After filling `var/evaluation/registration.json` and ensuring the parent directory exists, generate candidate files:

```sh
python3 -B scripts/evaluation/prepare.py \
  --registration var/evaluation/registration.json \
  --platform-template var/local/platform.json \
  --runner-template var/local/runner.json \
  --python-image python@sha256:229a2c5bfa27522db7815ea81f9bed70af17ccb9de9fc7ad142b1877b5830d36 \
  --go-image golang@sha256:9fdc884aacc3bec89b20ffc69f4bb369c78210e3e4f600387b5128b12c199f81 \
  --output var/evaluation/candidate

python3 -B scripts/evaluation/evaluate.py \
  --bundle var/evaluation/candidate \
  --output var/evaluation/real-batch
```

`prepare.py` reads the templates and writes a new private candidate directory. It preserves template storage, signing paths and runner identity, while replacing registered sources/profiles/model configuration with this corpus and operator selection. It computes source hashes using the production workspace manifest function. It does not install files, restart services, read provider keys or call a provider. The second command, without a mode flag, verifies local hashes and inputs only; it creates no batch and makes no network calls.

## Required deployment gate before paid execution

**A candidate hash is not an attestation of a running API or worker.** The API resolves `config_id` from its own loaded configuration; a worker can start sampling before the submit response returns. The harness's later ledger comparison can detect a mismatch, but cannot prevent that first request. Do not run `--execute` until the following deployment procedure has been completed and reviewed by the operator:

1. Record the explicit provider/model, price and capability sources, authorized total and task allocations, candidate hashes and corpus hash. Ensure there are no queued/resumable runs from other work and four free fixed workspace slots. Existing terminal workspaces remain allocated physically until the separately reviewed workspace-GC procedure releases them.
2. Stop the worker before changing model configuration. Stop admissions and the runner using the deployment's actual process manager. Confirm the old processes have exited; preserve the original configs for restoration. Do not attach a second runner to the same journal/pool.
3. Install the candidate platform and runner configuration into the exact paths used by the deployment, or launch the services explicitly using those candidate paths. Preserve the same runner journal identity, artifact root, volume pool, signing key and DB. This is a maintenance deployment, not a SQL-only clone.
4. Compare the installed files with `candidate.json` SHA256 values; inspect the actual process command lines/unit definitions and resolved config paths. Start the runner and API, then the worker with the approved provider key in its secret environment. Recheck the loaded-path files, new PIDs/start times and startup health. Save a credential-free deployment record including binary/config hashes and the resolved paths. Keep configurations fixed for the batch.
5. Independently confirm the running configuration maps the chosen fresh `config_id` to the approved provider/model, native registry, frozen quote and limits. Only then execute the paid command. This is an operator precondition, not an automated guarantee offered by this harness.

The local runner startup script uses `var/local/runner.json` inside its existing rootless user-systemd arrangement; simply generating `var/evaluation/candidate/runner.json` does not change that service. See `cmd/forge-runner/README.md` and the deployment's process manager for the actual startup paths. These scripts deliberately do not perform deployment or alter live configuration.

## Execution and collection

The audit connection must point explicitly to the same loopback PostgreSQL `/forge` database used by the API. Configure `FORGE_EVAL_AUDIT_DSN` privately in the invoking environment. The connection needs SELECT on `runs`, `model_attempts`, `quota_reservations`, `artifacts` and `run_events`, plus schema access. It may use an existing appropriately scoped audit role; the harness creates no role or grants. It runs a read-only repeatable-read transaction and sets `forge.tenant_id` to the CLI tenant. No SQL is sent through the model, and credentials are not placed on the psql command line. The current audit path uses the deployment's `public` schema.

After explicit model/spending authorization and the deployment gate:

```sh
python3 -B scripts/evaluation/evaluate.py \
  --bundle var/evaluation/candidate \
  --client-env var/local/client.env \
  --output var/evaluation/real-batch \
  --execute
```

`client.env` must contain the three plain generated `FORGE_API_URL`, `FORGE_TOKEN`, `FORGE_TENANT` entries. The harness invokes the existing `bin/forge` CLI against that loopback API. It creates a project from each fixed source/profile, durably writes the submission intent and idempotency key, submits the run, watches SSE, polls state, downloads selected ready artifacts with checksum verification, and audits **all** model attempts and quota reservations for that exact tenant/run.

The output directory must be new. Repeating `--execute` against an existing output directory refuses before submission. A missing submit response remains an unconfirmed allocation in the report. Inspect its saved intent and the CLI's persisted idempotency receipt before any operator recovery; do not allocate a fresh key or silently start another run. The evaluation harness never automatically resubmits an uncertain request.

Collection-only mode is safe to repeat for saved confirmed IDs:

```sh
python3 -B scripts/evaluation/evaluate.py \
  --bundle var/evaluation/candidate \
  --client-env var/local/client.env \
  --output var/evaluation/real-batch \
  --collect
```

Collection uses the original sealed corpus/model/quote/budget and never submits, approves, resumes, cancels, retries or garbage-collects a run. The batch stops on a missing confirmed ID, audit/config mismatch, budget violation, timeout, `waiting_approval` or `needs_reconciliation`. The driver requires human approval for model-requested shell commands; the harness does not grant it. Such a stop remains visible in the denominator and outcome report. An operator intervention must be recorded separately rather than presented as an unattended pass.

## Report meaning

`batch.json` records the frozen plan and budget allocation. Each task retains its project, submission intent/result, CLI receipt, SSE output, state, artifact metadata/bytes, exact ledger rows and `task-report.json`. The aggregate `report.json` reports outcome counts and verified repairs out of **four planned tasks**, even when submission stops early.

A verified repair requires a terminal completed run, platform verification marked verified, a failing registered baseline target, the exact verification artifact referenced by the run, trusted target and regression success for the final workspace revision, and at least one native provider dispatch. Earlier failed verification reports are retained but cannot supply the final result.

Raw model timing is `dispatch_to_response_persisted_seconds` per attempt, derived from DB timestamps and including response persistence overhead. Missing endpoints remain null. `database_created_to_finished_seconds` measures total persisted run duration when its finished event is retained; `collection_elapsed_seconds` measures this collector invocation only. A later collection is not a replacement run latency sample.

Every attempt includes native request ID when available, raw known/unknown token counters, the immutable price snapshot, status/error and its reservation. `known_ledger_cost_microusd` is a settled local ledger amount under that quote. `unknown_or_unsettled_reserved_microusd` retains conservative exposure for failed or unresolved attempts; failed dispatched calls are never assumed free. Unconfirmed submissions and unaudited runs retain their full allocated ceilings. `conservative_accounted_exposure_microusd` combines those three categories without recycling unresolved funds.

These are local ledger measurements under the approved configuration and stated bounds. `vendor_invoice_verified=false` and `vendor_billed_cost_microusd=null` remain explicit until separately reconciled with a vendor invoice. Runtime limits cannot make an incorrect operator price ceiling into a guaranteed vendor bill. A `complete=true` batch means all four outcomes were collected in terminal states without an observed budget violation; it does not mean four repairs passed.

The current checked-in evidence supports the fixed corpus, oracle behavior and offline validation only. Native model quality, real billed usage and end-to-end paid evaluation remain unexecuted acceptance work.
