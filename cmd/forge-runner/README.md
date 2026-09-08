# Forge runner: local production backend

The runner accepts typed gRPC requests, journals operation identities and receipts
in SQLite WAL with `synchronous=FULL`, and runs repository processes through a
separate rootless Docker daemon. Local transport uses a private Unix socket with
mode `0600`. Remote TCP requires TLS 1.3 mutual authentication, CA verification
and an explicit peer identity; there is no plaintext TCP mode.

This guide uses three Python repair fixtures, one Go fixture, and the four fixed
256 MiB ext4 volumes from [ADR 0002](../../docs/adr/0002-workspace-storage.md).
It keeps the host's default Docker context and unrelated workloads unchanged.
Commands run from the repository root as its ordinary owner. Linux user
namespaces, RootlessKit, rootless Docker, delegated cgroup v2, Go 1.26 and Python 3
must already be available.

## 1. Create control state and prepare the volume pool

Follow the database and build instructions in the [main README](../../README.md),
then create a new local configuration once:

```bash
make build
bin/forge-admin -repo "$PWD" -state "$PWD/var/local" local-setup
```

`local-setup` registers the sources/profiles and creates private JSON
configuration, a signing key and separate service environment files. It refuses
to overwrite an existing setup. This guide never prints or reads those key/env
contents; only the runner process and the explicit integration test load the key
when they need to sign or validate capabilities.

Prepare and mount the pool using the reviewed
[workspace-volume procedure](../../scripts/workspace-volumes/README.md).
Its image allocation and privileged mount step are explicit operator actions.
Afterwards these files must exist:

- `var/workspace-storage/manifest.json`: four ready image identities and UUIDs.
- `var/workspace-storage/mount-state.json`: four completed mount records bound to
  the digest of that exact manifest.
- `var/workspace-storage/images/slot-001.ext4` through `slot-004.ext4` and their
  corresponding mounts under `var/workspace-storage/mounts/`.

The configuration helper below does **not** prepare, format, attach, mount,
resize or unmount anything. Incomplete or inconsistent preparation is an error.

## 2. Select the dedicated rootless daemon and a pinned image

If the project's dedicated daemon is already running, use its explicit socket
and continue to image inspection. For a new checkout, the following operator
commands create its private daemon configuration and start only its own user
service. They do not install software or configure the default Docker daemon.

```bash
python3 -I - <<'PY'
import json, os
from pathlib import Path
root = Path.cwd().resolve()
runtime = Path('/run/user') / str(os.getuid())
config = {
    'data-root': str(root / 'var/docker-data'),
    'exec-root': str(runtime / 'forge-runtime-docker-exec'),
    'pidfile': str(runtime / 'forge-runtime-docker.pid'),
    'hosts': ['unix://' + str(runtime / 'forge-runtime-docker.sock')],
    'exec-opts': ['native.cgroupdriver=systemd'],
    'bridge': 'none', 'iptables': False, 'ip6tables': False,
    'ip-forward': False, 'userland-proxy': False,
    'log-driver': 'local', 'log-opts': {'max-size': '1m', 'max-file': '2'},
}
with (root / 'var/rootless-docker.json').open('x') as f:
    json.dump(config, f, indent=2)
    f.write('\n')
PY

systemd-run --user --unit=forge-runtime-docker --property=Delegate=yes --collect \
  --setenv="DOCKERD_ROOTLESS_ROOTLESSKIT_STATE_DIR=/run/user/$(id -u)/forge-runtime-rootlesskit" \
  /usr/bin/dockerd-rootless.sh --config-file "$PWD/var/rootless-docker.json"
```

The exclusive file creation deliberately refuses an existing daemon
configuration. Inspect/reuse the existing project service instead of overwriting
its state. The runner's startup check requires a rootless daemon and working
cgroup v2 memory, PID and CPU limit capabilities.

```bash
runner_docker_host="unix:///run/user/$(id -u)/forge-runtime-docker.sock"
docker --host "$runner_docker_host" info
docker --host "$runner_docker_host" pull python:3.12.13-slim
docker --host "$runner_docker_host" image inspect python:3.12.13-slim \
  --format '{{json .RepoDigests}}'
docker --host "$runner_docker_host" pull golang:1.26-bookworm
docker --host "$runner_docker_host" image inspect golang:1.26-bookworm \
  --format '{{json .RepoDigests}}'
```

Use a returned `python@sha256:...` reference explicitly. The configuration helper
never resolves a tag or chooses a model/image automatically. The image used for
the recorded September 8, 2026 execution was:

```text
python@sha256:229a2c5bfa27522db7815ea81f9bed70af17ccb9de9fc7ad142b1877b5830d36
golang@sha256:9fdc884aacc3bec89b20ffc69f4bb369c78210e3e4f600387b5128b12c199f81
```

## 3. Generate the runner's volume identities

Replace the digest below only with the explicit image you selected. There is no
need to transcribe any of the four loop devices, image inodes or UUIDs.

```bash
python3 -I cmd/forge-runner/configure-local.py \
  --image python@sha256:229a2c5bfa27522db7815ea81f9bed70af17ccb9de9fc7ad142b1877b5830d36 \
  --profile-image go-ceil-div=golang@sha256:9fdc884aacc3bec89b20ffc69f4bb369c78210e3e4f600387b5128b12c199f81 \
  --docker-host "$runner_docker_host" \
  --check

python3 -I cmd/forge-runner/configure-local.py \
  --image python@sha256:229a2c5bfa27522db7815ea81f9bed70af17ccb9de9fc7ad142b1877b5830d36 \
  --profile-image go-ceil-div=golang@sha256:9fdc884aacc3bec89b20ffc69f4bb369c78210e3e4f600387b5128b12c199f81 \
  --docker-host "$runner_docker_host"
```

The helper derives this checkout from its own location and validates the
**complete** manifest before updating anything: scope/owner, four ready slots,
fixed capacities, distinct identities, mount-record phases and manifest digest,
backing image file identity/allocation, ext4 UUID and inode ceiling. It refuses
symlinks, tag-only images, TCP Docker URLs, a test backend, or profiles that change
the fixed quota or task UID. It preserves source paths, signing-key paths,
transport and other control settings.

Python profiles explicitly use a non-executable 64 MiB `/tmp`. The Go profile
explicitly permits executing compiled binaries in that same bounded scratch
mount; its trusted grader builds offline with `GOTOOLCHAIN=local`, `GOPROXY=off`
and no external dependencies. Rootfs and candidate verification mounts remain
read-only. `python3 scripts/demo-repairs.py --fixture go-ceil-div` runs its
baseline failure, target/regression verification and independent patch replay.

The sole persistent output is `var/local/runner.json`, replaced atomically with
mode `0600` through a synced temporary file in that same directory. Repeating an
identical configuration leaves it unchanged. `--check` writes no configuration.
Neither mode reads signing-key or env files, contacts Docker, or acts on mounts.

These checks validate recorded preparation and the backing images. They do not
replace the production runner's live kernel checks. At startup and operation
admission, the runner checks the effective mount, `rw,nodev,nosuid`, ext4 identity,
loop backing path/offset/size, filesystem capacity and inode ceiling. A missing
mount never falls back to an ordinary host directory.

## 4. Start the mapped runner

```bash
go build -o bin/forge-runner ./cmd/forge-runner
./cmd/forge-runner/start-local.sh
systemctl --user status forge-runtime-runner.service --no-pager
journalctl --user -u forge-runtime-runner.service -n 30 --no-pager
stat -c '%a %n' var/local/runner.sock
```

The startup script creates only `forge-runtime-runner.service`. RootlessKit uses
`--propagation=rslave` and a project-specific state directory under
`$XDG_RUNTIME_DIR` (or `/run/user/<uid>`). The trusted runner runs as namespace UID
0, mapped to the ordinary host owner, with the full subordinate UID mapping.
Task containers remain UID/GID `1000:1000`, with capabilities dropped,
no-new-privileges, no network, read-only root filesystems, bounded `/tmp`, and
memory/PID/CPU limits. This mapping lets the trusted runner inspect task-created
`0600` files inside `0700` directories without running task code as host root.

The successful startup log reports `test_backend=false` and the socket path;
the socket mode must be `600`. A pool is exclusively bound to its runner journal.
Do not point another journal at that same pool or remove its ownership markers.
Each SQLite slot lease is persisted before import. Unknown effects and
interrupted imports retain their slot. Completed runs also retain their
workspace until the explicit cleanup workflow seals the snapshot and releases
it; capacity exhaustion is not permission to erase a workspace.

For remote deployment, replace the Unix socket with `server.tcp_address` plus
`server.tls.cert_file`, `key_file`, `ca_file` and `expected_peer_uri`. The matching
client must verify the server's CA, name and expected URI identity. Configure
this separately from the local helper; do not expose the local UDS over
unverified TCP.

## 5. Reproduce validation

The fast configuration suite requires no daemon or mounts:

```bash
python3 -B -m unittest discover -s cmd/forge-runner -p 'test_*.py' -v
```

Runner and transport checks:

```bash
go test -race ./internal/runner ./internal/sandbox ./internal/artifact ./internal/runnerclient
```

The transport suite needs permission to create local sockets. The real Docker
case is explicitly skipped unless the operator supplies this configuration:

```bash
FORGE_REAL_RUNNER_CONFIG="$PWD/var/local/runner.json" \
  go test -v -count=1 ./internal/runnerclient \
  -run '^TestRealDockerRunnerIsolationQuotaAndCancellation$'
```

Run it while at least one slot is free. It creates a unique `docker-probe-*`
workspace and checks actual UID/capability/NNP isolation, read-only rootfs,
bounded tmpfs, absent task networking/socket access, private-file read/patch,
in-kernel ENOSPC, and actual process exit before the no-active stop receipt. It
also checks that delayed same-epoch adoption cannot reopen a stopped workspace.
Only its own settled test workspace is snapshotted and released. Unknown effects
are retained for inspection; the test does not remove existing repair workspaces,
container images, mounts or unrelated Docker resources.

The strengthened [real execution log](../../benchmarks/results/real-docker-limits-20260908.log)
passed in 5.02 seconds. It observed UID/GID 1000, zero capabilities, NNP,
private-file interoperability, cgroup settings, actual PID EAGAIN and
`OOMKilled=true`, ENOSPC after 228,589,568 bytes, and zero remaining parent/child
tasks before the stop receipt. Repeating an actual write returned its original
receipt and left the write count at one. CPU quota was read from the real cgroup;
CPU fairness and simultaneous cross-slot disk stress were not measured.

Before replacing a running binary, drain active operations and use an orderly
restart of this project's runner service. A stopped service retains its journal,
receipts, mounts and slot assignments. Keep API/worker configuration and cleanup
steps in the [main README](../../README.md); never reclaim pool space by manually
deleting an active/unknown checkout or by pruning Docker resources.
