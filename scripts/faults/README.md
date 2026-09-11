# Real runner process crash acceptance

`TestRealRunnerCrashMatrix` launches the actual `forge-runner` executable with a real Docker backend, kills that process with exit 86 at a precise boundary, reconnects to the same SQLite WAL journal and fixed volume pool, and reconciles the original operation ID. No TestBackend or fake receipt stands in for Docker evidence. The test is opt-in and ordinary `go test ./...` skips it.

The test adds unique `runner-fault-acceptance` tenant workspaces. It does not modify pre-existing runs, create mounts, remove images or operate production services. Stop and snapshot proofs precede release of each successful test workspace. If a case fails, its workspace and evidence remain for operator review. The test stops immediately after the failed case, preserving remaining pool capacity.

## Operator prerequisites

1. Use the provisioned volume manifest and the existing production runner JSON. At least one slot must be free; all existing runner jobs must be settled. Quiesce all platform workers and the production runner yourself before execution. Engine startup holds exclusive pool locks, so an active runner causes the harness to fail closed.
2. The configuration must use the real rootless daemon, fixed ext4 slots, and a digest-pinned `python-clamp` profile with the offline `clamp` source. It must point at the original journal: `.forge-pool.owner` forbids substituting another journal on the same pool.
3. Run the harness in the same full subordinate UID/GID map as the runner. Task UID 1000 creates private files which an ordinary unmapped host process cannot inspect. RootlessKit's mapped UID 0 is expected; task containers retain UID 1000 and all existing Docker limits.
4. Build fresh binaries before stopping services. The harness does not fetch dependencies or images.

From the project root:

```bash
GOCACHE=/tmp/forge-runtime-gocache go build -o bin/forge-runner ./cmd/forge-runner
GOCACHE=/tmp/forge-runtime-gocache go test -c -o bin/runner-faults.test ./internal/runnerclient
```

After operator-confirmed quiescence, execute using an unused RootlessKit state directory and a new evidence directory. Adjust the daemon mapping command to the reviewed local deployment if necessary; do not change pool paths or journal identity:

```bash
rootlesskit --propagation=rslave \
  --state-dir="/run/user/$(id -u)/forge-faults-kit" \
  env FORGE_RUNNER_FAULT_CONFIG="$PWD/var/local/runner.json" \
      FORGE_RUNNER_FAULT_BINARY="$PWD/bin/forge-runner" \
      FORGE_RUNNER_FAULT_TEST_BINARY="$PWD/bin/runner-faults.test" \
      FORGE_RUNNER_FAULT_EVIDENCE="$PWD/benchmarks/results/runner-faults-UNIQUE" \
  "$PWD/scripts/faults/run-runner-matrix.sh"
```

Only the operator restarts production services after reviewing exit status. On a failure, inspect the recorded workspace ID before restarting workers; never blindly rerun the harness to erase evidence or release an unresolved effect.

## Boundaries and assertions

| Boundary | Recovery evidence |
|---|---|
| Lease committed, no files imported | SQLite lease retains source hash/epoch and capacity without a workspace row or operation; epoch 2 resumes the identical source |
| One source file imported | Existing bytes match an exact subset of pinned source; only missing files are copied; no overwrite |
| Import synced, workspace row absent | Same source hash and epoch 2 yield revision 1 after restart |
| Operation prepared, dispatch not committed | Inspect reports uncertainty; explicit cancellation uses durable `dispatch_started=0` proof; Docker create/start counts 0 |
| Docker create completed, start absent | Inspect reports uncertainty; release refused; durable start-intent=0 plus exclusive writer ownership permits non-force removal and proves never-started cancellation; start count 0 |
| Docker start completed | Restart inspects the same named container and returns its observed result; create/start counts exactly 1 |
| Start observed by runner | Same daemon receipt recovery and immutable operation binding |
| Real job exit observed, receipt absent | Successful outcome retained and counter exactly 1; no rerun |
| Artifact pinned, SQLite operation finish absent | Artifact publication pin survives the crash; result recovers from daemon and original ID is immutable |
| Start RPC response delayed after acceptance | Client deadline triggers an inspect of the same ID; successful write occurs once |
| Lease expires while old writer survives runner crash | Epoch 2 adoption stops old container before replacement writer; expired grant and fresh old-epoch grant both rejected; old file cannot grow after stop proof |

Each case emits JSON containing the fault marker with real child PID, lease/status/revision facts, operation receipt, daemon create/start events, counter or writer evidence, artifact pins, no-active receipt, snapshot digest and released lease check. Logs and plans are owner-only and omit raw signing keys, grants and environment credentials. JSON explicitly states that PostgreSQL run/effect/budget/SSE dimensions are **N/A** for this isolated runner harness. F05/F07/F08 runner components are not a claim that the entire end-to-end fault matrix is complete.

## Fault-plan safety and initialization recovery

Fault hooks are disabled unless both `-allow-fault-injection` and `-fault-file` are supplied locally. There is no RPC or config JSON switch to arm faults. The plan must be a private regular file directly under `root_dir/operator-faults`, owned by the runner UID without symlinks. It has an exact tenant/run/workspace/operation/epoch binding, a whitelisted boundary, and an expiry of at most ten minutes. A fsynced exclusive `.used` marker makes it one-shot across process restarts. The only actions are exit 86 and a delay bounded to ten seconds. TestBackend use with these flags is rejected.

Journal schema v4 pins the immutable source hash and latest initialization epoch before import. An interrupted initialization with the same immutable binding can be retried using `PrepareWorkspace` and a current grant. Unknown files, changed source bytes, corrupt partial files, stale epochs, or v3 orphan leases with no recorded source hash are retained and rejected. There is deliberately no automatic delete/reimport or guess-based source rebinding. `Engine.InspectInitialization` exposes local diagnostic metadata without granting remote execution rights.

The v4 journal also has `runner_artifacts(object_key, ref_json)` and `operations.dispatch_started`. Legacy operation rows migrate conservatively as dispatched. A created container is never removed merely because `inspect` reports no start: `docker_start_intent=0` must prove that no start CLI was dispatched, while the replacement runner owns the writer lock. The intent is committed before invoking Docker start; legacy rows migrate as intent=1.

Artifact publication holds the store's shared publication lock through durable pinning and operation finish; garbage collection consults those pins and the registered journal identity under its exclusive lock.

## Recover a retained create-before-start case

When a prior `after_docker_create` case failed before cancellation, preserve its report and use a **new** evidence directory. Set `FORGE_RUNNER_FAULT_RECOVER` to the absolute path of that failed `after_docker_create.json` in addition to the same four normal inputs. Invoke the same wrapper through the reviewed user-service/RootlessKit launch. This mode verifies the original tenant/run/workspace/operation and durable `docker_start_intent=0`, calls only Cancel/Stop/Seal/Release for that exact workspace, asserts original create=1/start=0 daemon history and a single SQLite operation row, and exits without starting any new operation or allocating a workspace. Do not set this variable for a fresh matrix run. It is intentionally limited to this proven-never-started case; it is not an arbitrary unknown-effect cleanup command.
