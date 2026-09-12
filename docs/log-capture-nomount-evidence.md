# E44 supplement — standalone capture-stage acceptance

This is a narrower alternative to the shared-volume backend test whose execution was rejected. It creates four new containers without any host bind mount, volume, tmpfs mount, runner Engine, journal, pool, or service operation. It does not authorize or execute the previously rejected stop-services/shared-volume helper.

`TestNoMountCapturedDockerAcceptance` in `internal/sandbox/logs_nomount_integration_test.go` exercises the existing production `Docker.startCaptured` stage after fixture-owned `docker create`. It does **not** invoke `Docker.Start`, replace a quota verifier, or claim that creating these containers satisfies the production workspace quota precondition. The production Docker/attach/log policy files were unchanged and matched main when the binary was built.

Each create uses the exact pinned Python digest, `--pull=never`, a read-only root filesystem, network `none`, user `1000:1000`, no capabilities, no-new-privileges, 256 MiB memory with equal memory-swap limit, one CPU and 64 PIDs. The fixture verifies the returned container identity and inspected constraints before calling `startCaptured`. Exact name, full original container ID, three ownership labels, image reference and image ID are saved. No external model or paid API is called.

The recording sink is deliberately test-only: its limit is **512 KiB retained raw payload across stdout/stderr**, with a separate 64 KiB preview. It has no production spool frame headers, filesystem durability, SQLite reservations or artifact publication. All observed stream bytes continue to be counted/discarded after the limit. These results must not be described as real runner disk-spool or per-run quota acceptance.

Four sequential cases cover:

| Case | Evidence required |
| --- | --- |
| Binary | Exact stdout `out\0` and stderr `err\xff`, complete capture and zero exit. |
| Flood | Both streams emit at least 64 KiB; output-limit signal requests an observed stop, drains through EOF, and rejects trusted verification. |
| Cancel | Exact parent/child markers precede explicit context cancellation; capture completes and the outcome remains business cancellation, without a policy-stop claim. |
| Detach | Exact parent/child markers precede shutdown-context cancellation; the original container remains running and capture is incomplete. The same finite container later exits zero naturally, without restart or a second attach. |

The programs naturally finish in approximately 3.5–3.7 seconds or earlier; none contains an infinite loop. The full test has a 65-second context and is invoked with a 90-second test timeout. Each intent is atomically saved before create, the full container ID is saved immediately on acknowledgement, and cleanup checks exact ID/name/labels/image before acting. Cleanup removes only a confirmed stopped owned container. A missing acknowledgement can only be resolved by the unique saved name and labels; a foreign or unconfirmed identity is retained as an explicit failure.

The fixture records create time, the production pre-start callback time (which follows successful attach acknowledgement), Docker start/finish timestamps, control time, actual inspect facts, capture metadata, retained stream hashes and cleanup outcome. The stopped-container check includes the daemon-reported init PID of zero. It does not claim a separate host cgroup-membership inspection, OS-process runner crash, complete `Docker.Start` admission, Engine/SQLite/PG integration, filesystem quota enforcement, or a second observation of the earlier attach probe.

## Frozen build and local verification

Artifacts are in `benchmarks/results/log-limits-e44-nomount/`:

- `offline-race-01.log`: local race tests passed, including concurrent sink bounds and AST parsing of both the outer and literal child programs. The actual Docker test was skipped because the opt-in environment was absent.
- `logs_nomount_integration_test.go.txt`: exact frozen test source.
- `linked-inputs.json`: all 20 local module inputs reported by `go list -deps -test ./internal/sandbox`, including module checksums.
- `build-reproduction.json` / `build.log`: a second build to a different output path reproduced the original binary byte-for-byte; every listed input remained unchanged.
- `build-source.json`: source digest, original local executable location/size/digest, and comparison against main's production capture files.

Frozen test source SHA-256:
`804fc65f6347fd31ab94a68614f6bd546709d7d2c162f2421c2bf43e77e74149`.

Frozen executable SHA-256:
`ffc7f39dcf5852e41049a422020abc75ddbdd49cfe34ac9997385071149763f6`.

The old fixed-volume backend binary and its evidence are preserved separately. Large executables are local build artifacts, not committed binaries.

## Reviewed host invocation

Actual host execution is performed by root after independent review, using only this new no-mount fixture:

```sh
env FORGE_E44_NOMOUNT_DOCKER_HOST=unix:///run/user/1000/forge-runtime-docker.sock \
  FORGE_E44_NOMOUNT_OUTPUT=/absolute/new/evidence-directory \
  /absolute/logs-worktree/var/local/e44-nomount-sandbox.test \
  -test.run '^TestNoMountCapturedDockerAcceptance$' \
  -test.v -test.count=1 -test.timeout=90s
```

At this source freeze, no actual result from this new fixture has been supplied to the author. Its host result remains pending; the earlier shared-volume authorization question and all physical spool/Engine integration boundaries remain unchanged.
