# E50 — actual worker and runner SIGTERM acceptance fixture

Status on 2026-09-12: the first actual attempt stopped at `before_runner`, after private schema/run creation but before a usable runner, worker, journal, volume owner or container existed. The original runner rejected Docker inspection output containing a stderr warning. **The complete SIGTERM scenario has not passed.** The separately preserved first-failure evidence is authoritative; the abort/second-attempt extension below has only offline validation. No container, service, mounted volume, runner journal, or public PostgreSQL schema was changed while developing this fixture. This document does not promote S16.4 to verified.

The fixture extends the acceptance capability beyond E48's actual worker processes with an in-process recording backend. It uses actual `forge-worker` and `forge-runner` child executables, the production gRPC client, `Docker.Start`, rootless capability checks, a physically bounded ext4 workspace, SQLite v5, and the production strict log spool. The provider is deterministic and its fees are synthetic. Existing API shutdown evidence remains a separate component; this fixture does not start an API service or claim HTTP download coverage.

## Scope and input

The new files are `scripts/faults/application/worker_runner_sigterm_test.go` and `worker_runner_sigterm_helpers_test.go`. They reuse E48's restricted PostgreSQL role, actual worker launch/signal identity, sequential database capture, and production cleanup helpers. No production code or existing acceptance case was changed.

The operator supplies `acceptance.json` with these exact fields:

```json
{
  "purpose": "worker-runner-sigterm-v1",
  "fixture_id": "lr20260912_a",
  "scope_root": "/absolute/project/var/lifecycle-rehearsals/lr20260912_a",
  "pool_root": "/absolute/project/var/lifecycle-rehearsals/lr20260912_a/pool-root",
  "runner_config": "/absolute/project/var/lifecycle-rehearsals/lr20260912_a/runtime/runner.json",
  "runner_binary": "/absolute/project/var/lifecycle-rehearsals/lr20260912_a/bin/forge-runner",
  "runner_sha256": "64 lowercase hexadecimal digits",
  "worker_binary": "/absolute/project/var/lifecycle-rehearsals/lr20260912_a/bin/forge-worker",
  "worker_sha256": "64 lowercase hexadecimal digits",
  "test_binary": "/absolute/project/var/lifecycle-rehearsals/lr20260912_a/bin/application-faults.test",
  "test_sha256": "64 lowercase hexadecimal digits",
  "evidence_dir": "/absolute/project/var/lifecycle-rehearsals/lr20260912_a/evidence/sigterm-01"
}
```

This particular test intentionally accepts only `lr20260912_a`. It rejects the demonstration pool, `r20260911_a` recovery pool, a foreign journal, symlinked ancestors, existing first-case owner markers/workspaces/journals, a test backend, a TCP runner, altered log limits, and an unconstrained or unpinned profile. It calls the production `sandbox.VerifyVolume` for all four mounted 256 MiB images. It does not prepare or mount them and cannot rebind an owner.

`runner.json` uses `runtime/journal.sqlite`, the private UDS `/tmp/forge-lifecycle-lr20260912_a/runner.sock`, one source/profile, the configured dedicated rootless Docker socket, and strict default limits: entry 16 KiB, operation 512 KiB, cumulative run 16 MiB / 32 process operations, preview 64 KiB. The profile uses the pinned Python digest `229a2c5bfa27522db7815ea81f9bed70af17ccb9de9fc7ad142b1877b5830d36`, UID/GID 1000, memory 256 MiB, one CPU, 64 PIDs and a 256 MiB workspace quota. Python `/tmp` remains noexec.

The source is exactly `def clamp(v, lo, hi):\n    return v\n`. The target loads `/workspace/app.py` using `runpy` and asserts `clamp(5,0,3)==3`; regression asserts `clamp(2,0,3)==2`. The first target must actually fail with an assertion and complete, trustworthy logs before the command is approved. An import error or incomplete log is not baseline failure evidence.

The host launcher must provide the full rootless UID/GID mapping: namespace UID 0 maps to host UID 1000, and task UID 1000 is covered by the subordinate range. The test records these maps. It creates only its private schema, non-owner restricted LOGIN/BYPASSRLS role and private worker configuration. The admin connection is accepted only on `127.0.0.1:32773/forge`; neither it nor another parent credential is inherited by a child process. `runtime/sigterm-private/database.json` retains the restricted recovery credential at mode 0600 and **must not be archived as public evidence**.

## Planned actual sequence and oracles

1. Check all paths, four real volume identities and executable hashes before creating a schema or process. Launch the actual runner, then the actual worker. Approve the first immutable literal command through the real store control path.
2. The Python command appends one durable start-counter line and creates one child. Parent and child emit distinct stdout markers and stderr containing `00 ff`. The parent writes timestamped lines every 200 ms. Both have a natural 70-second bound; the test has a three-minute bound. Expected process containers are the initial target and this command; the second proposed command is never approved.
3. Send SIGTERM to the original worker only after checking its executable hash and `/proc` start ticks. Require clean exit, an unchanged in-flight business operation, reserved tenant/runner capacity, and the same still-running container with init/parent/child cgroup tasks.
4. Send SIGTERM to the actual runner with the same PID identity checks. Require clean exit, the original still-running container/process tree, a durable `unknown` operation without cancellation, and a physically sealed spool marked `runner_shutdown`, incomplete/truncated, with no policy termination request or observed policy kill.
5. Launch a new runner using the same config, signing key, journal UUID and volume owner. Inspect the original operation with the still-valid original database lease/epoch. Require `unknown`, the identical container and request, and no stop/restart. Launch a new actual worker and wait for its normal 30-second lease claim; no lease, deadline or retry timestamp is edited.
6. The new worker's normal adoption may legally stop the old writer. Require a stopped original container, empty/removed cgroup, a single immutable operation receipt, and one Docker create/start/die. Its stop must occur after the successor claim and before the program's natural bound. The incomplete capture can settle as failed/cancelled under actual reconciliation semantics; it must never become trusted verification. The fixture does not demand permanent `needs_reconciliation` once the real job has stopped.
7. Stop at the next command's approval wait. There must still have been zero business-cancel events. Explicitly cancel the run, require its terminal cancellation/stop receipt, one confirmation of the old effect, two settled synthetic model reservations totaling 300 microusd, and zero active tenant/runner/provider capacity. Require the retained gap-log artifact in PostgreSQL READY exactly once with matching SHA-256 and length.
8. Run production `CleanupWorkspace`: stop, seal/publish the immutable snapshot, then release. Require zero unsettled operations/leased slots, durable removal of the two owned log containers, and an unchanged cumulative log reservation of two operations / 1 MiB. Shut down both replacement processes. Leave the same journal UUID and pool owner for the later S12.9 cases.

PostgreSQL tables, SQLite tables, Docker inspect/events and filesystem reads are sequential observations, **not an atomic cross-system snapshot**. The ledger preserves original timestamps so an auditor can reconstruct each boundary. The author does not substitute a recording backend, disabled quota verifier, or mounted-host-free test for this sequence.

## Execution and recovery entry points

Build the production commands and the application test package from one frozen source identity. The repository prepare/configure/launch workflow, invoked by the ordinary checkout owner, installs their paths/hashes in the manifest. The underlying invocation, run only in the authorized dedicated mapped namespace, is:

```text
FORGE_RUN_WORKER_RUNNER_SIGTERM=1
FORGE_WORKER_RUNNER_SIGTERM_ACCEPTANCE=/absolute/path/to/acceptance.json
FORGE_TEST_DATABASE_URL=<privately injected dedicated loopback admin credential>
application-faults.test -test.run '^TestRealWorkerRunnerSIGTERM$' -test.v -test.timeout 4m
```

The case uses a new `worker-runner-sigterm` child of `evidence_dir`, allowing the host launcher to keep its own files alongside it. It refuses to overwrite an earlier case. `recovery.json` records the original run/operation, schema, private configuration path and hashes, journal UUID, container ID/name when observed, and last durable test phase. Process failure cleanup signals only children launched by the test; it never business-cancels or deletes an unknown container/workspace automatically.

`TestRecoverWorkerRunnerSIGTERM` is a separate, explicitly enabled recovery action:

```text
FORGE_RECOVER_WORKER_RUNNER_SIGTERM=1
FORGE_WORKER_RUNNER_SIGTERM_ACCEPTANCE=/absolute/path/to/the/original/acceptance.json
application-faults.test -test.run '^TestRecoverWorkerRunnerSIGTERM$' -test.v -test.timeout 3m
```

Recovery requires the original executable/configuration hashes, journal UUID, four unchanged owners, absent runner socket, original isolated schema/run and private restricted credential. It refuses incomplete identity, an already released case, a changed config/binary or a journal subsequently used by another fixture. It starts only the original runner configuration, applies explicit business cancellation to the original run, lets a real worker settle it under natural lease rules, and seals/releases through production cleanup. It creates a new timestamped recovery evidence directory and never overwrites the failed case. A failure before a usable journal UUID still fails closed in this recovery entry point. The strictly proven `before_runner` case uses the separate intent-only abort entry point below; it must never be fed to journal recovery. The recovery entry point itself has only been compiled, not actually exercised.

## Evidence files and current validation

The future actual case writes executable/PID/start-tick identities, signal database/wall-clock boundaries, PostgreSQL snapshots, grant-stripped SQLite snapshots, original Docker inspect/top/events, cgroup contents, the single-write counter and writer timestamps. `runner-shutdown.spool`, `.meta.json`, `.stdout` and `.stderr` preserve the real detached capture prefix and its digest; `after-adoption` repeats the check before cleanup. The final receipt retains the authenticated log artifact reference. `cleanup-released-postgres.json`, `cleanup-released-sqlite.json` and the archived READY artifact bytes support later independent audit and HTTP-download follow-up.

Offline records are under `benchmarks/results/worker-runner-sigterm-e50`:

- `offline-01` preserves an AST-preflight failure: the newly constructed `ast.Expr` lacked `lineno`. No long program, PostgreSQL or Docker action ran. The fix adds `ast.fix_missing_locations`; it does not change the Python workload or production behavior.
- `offline-02` records ordinary and race tests, package vet, and application test-binary compilation, all exit 0. The local tests cover the two exact binary writes, deliberately wrong parent/child escapes, stream frame corruption, UID maps, symlinked ancestors and nine unsafe fixture shapes. The two actual lifecycle/recovery entry points were skipped.
- The 281 observed Go/SQL/module input hashes were unchanged across these checks. These are an observed source inventory, not an independent rebuild attestation. The preserved local compiled binary is identified in `offline-02/report.json`; it is not claimed as a later integrated main binary.

Actual process/container execution, the complete acceptance report and an independent raw-evidence audit remain pending an authorized, independently mounted fixture pool and root launch.


## Before-runner abort and a second independent attempt

`TestAbortUnstartedLifecycle` is a separate opt-in control action, never a shortcut in the original SIGTERM test. Its manifest is exactly `scope/acceptance-abort-01.json`, with `evidence_dir=scope/evidence/sigterm-01`. The three new executables must share `scope/bin/<40 lowercase hexadecimal revision>/`; original `scope/acceptance.json` and old binaries remain intact. The host workflow selects `--phase abort --attempt 01`. The underlying test requires `FORGE_ABORT_UNSTARTED_LIFECYCLE=1` and the selected manifest in `FORGE_WORKER_RUNNER_SIGTERM_ACCEPTANCE`; it does not use an administrator database connection.

The abort reads the original acceptance and archived acceptance-input, failure recovery descriptor, private worker configuration and its recorded digest, original private restricted database route, fixture source/run identity, original runner PID/executable/start ticks, and the retained read-only audit. It requires `phase=before_runner`, absent journal UUID, the original failed case's exact file inventory, an absent original runner/worker process, no original-run managed container, empty engine/artifact directories, no journal/WAL/SHM/socket, and four verified never-owned slots. No runner is launched and no owner or journal is created. Original binaries/configurations remain hash-bound. The private credential is never printed or archived; its file digest binds the evidence to that retained credential.

A read-only repeatable-read transaction checks the original run and its complete private-schema ledgers. Before mutation it requires queued/version 1, zero lease/owner/workspace, no pending command, the unchanged audit deadline, exactly one created event/snapshot, zero model/effect/step/allocation/quota/artifact/cleanup/approval/message rows and zero capacity/fee use. It then writes an exclusive durable `abort-before-runner/intent.json` and calls **only production `Store.Cancel`**. This intentionally yields `cancel_requested`, version 2, target `cancelled`, lease epoch 0 and `terminal=false`. It creates exactly one committed initialization control step and a pending `stop_execution` command. There is still no effect execution or no-active acknowledgment. Requiring zero steps/commands afterward would contradict the production control ledger.

The new `abort-before-runner/report.json` requires `passed=true`, `status="cancel_requested"`, `terminal=false`, with the original deadline, immutable run columns and original event/snapshot rows unchanged. Its before/after database observations are internally repeatable-read snapshots; process/Docker/filesystem observations are separate and are not a cross-system atomic snapshot. An interrupted abort may be explicitly repeated only with the same durable intent and bindings. It reobserves queued/version 1 or the exact cancel-requested/version 2 result, uses the idempotent Cancel path if needed, and never overwrites an existing intent/report. Malformed partial evidence fails closed for diagnosis.

The original run remains nonterminal, and its private schema/credential must never be routed to a worker. This result claims neither business cancellation completion nor successful lifecycle recovery. No deadline, lease or historical record is edited. Original evidence and private files are retained.

The second SIGTERM attempt requires exactly `scope/acceptance-02.json`, `evidence_dir=scope/evidence/sigterm-02` and `runtime/sigterm-private-02`. Before any new schema or process it rechecks the physical fresh-state prerequisites and the original abort report **against the live original database**, including the exact control step, cancel event/snapshot and zero execution/capacity state. It creates a new, explicitly different schema/run and never gives the original credential to a new child. Its `independent-attempt.json` records the original cancellation observation. All original SIGTERM, natural lease/adoption, one-operation execution, incomplete-log, fee and cleanup assertions remain unchanged.

The recovery entry point derives private configuration from the selected evidence attempt. S12's L5 likewise reads the selected `sigterm-01` or `sigterm-02` report and corresponding private schema. Its overall six-case output remains `evidence/logs-01/acceptance.json`; changing the selected SIGTERM attempt does not overwrite or rename earlier evidence. The selected new case still writes `evidence/sigterm-02/worker-runner-sigterm/acceptance.json`.

Offline validation for this extension is archived under `benchmarks/results/lifecycle-second-attempt`. Synthetic control observations cover refusal of changed deadlines, hidden work/capacity, leases, mixed schema/run history, missing stop intent, premature terminal state, rewritten snapshots, incorrect control steps, database routing overrides and mixed/short binary revisions. These are fixture-oracle tests, not PostgreSQL/Cancel/Docker execution evidence. The actual abort and attempt 02 remain pending independent review and host execution.
