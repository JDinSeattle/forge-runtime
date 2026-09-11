# Coordinated recovery acceptance

This is an opt-in, destructive-fault **fixture** protocol, never a procedure to point a production worker at an independently restored SQL dump. It creates separate source and target databases, artifact roots, SQLite journals and fixed workspace pools under `var/recovery-rehearsals/ID`. Existing `var/local` configuration, live services and the original four workspace mounts are not modified. Retain the evidence directory after any failed stage; the scripts refuse overwriting it.

The source pool, immutable backup images, and target pool each contain four fully allocated 256 MiB ext4 images: about **3 GiB plus metadata**, and **eight additional loop mounts** after both pools are mounted. The original live four are separate. This protocol intentionally retains images, stopped source containers, databases and mounts for review. It performs no prune, forced unmount, reformat or cleanup of an unresolved workspace.

## Atomic recovery boundary

The source test uses the real PostgreSQL Store/reducer and real Docker backend. One command writes an execution counter and task-owned private file; a second operation stops after durable preparation, before any Docker launch. All dispatched source jobs stop. The sole source fixture process then exits 86 **without closing SQLite**, retaining committed WAL evidence. No source API, scheduler, worker, or artifact GC is ever started, so SQL, journal, artifacts and workspaces remain quiescent throughout collection. This separate artifact root excludes the live artifact collector.

`backup` captures PostgreSQL with `pg_dump`, uses SQLite's backup API to merge committed WAL (and separately retains the raw WAL as evidence), and copies the artifact bytes, source, key and configuration. `freeze-copy.py` validates the four **new source** loop/backing/mount identities, freezes only those filesystems, clones each image twice, syncs copies and directories, attempts every thaw even if another fails, and publishes a SHA-256 manifest covering all paired assets. A manifest is not published if freezing, copying, syncing or thawing fails. SIGKILL or host failure during freeze requires an operator to inspect and unfreeze the exact new-source paths listed by `--check`; never use a global device cleanup.

Target restore first verifies the complete backup file set and every digest. It restores SQL into a newly created database and copies the standalone SQLite snapshot and objects. **Offline physical relocation is explicit:** cloned `.forge-pool.owner` markers and SQLite `volume_slots.spec_json` receive the new target paths/device/inode; artifact journal registration receives the new path/filename while preserving `journal_identity.id`. The source/backup registry and bytes stay unchanged. Workspace/operation IDs, immutable request/receipt bytes, runner epochs, PostgreSQL leases and unknown states remain exact. A report records the physical mapping and paired-manifest digest.

The target test starts only an in-process runner, with a backend wrapper that rejects and counts **every** new external `Start`. Its fresh, local **operator fixture grant** allows clone inspection and a file-tool read; this is not a renewed scheduler lease. Copied PostgreSQL state is not modified. An original request past its immutable deadline is rejected by `StartOperation` and inspected instead. Before expiry, the same ID returns the durable result/unknown status. The test verifies READY artifact bytes, transition input/output replay, a private file read and snapshot from the actual restored ext4 image, original receipt identity, zero new target command launches, and the unresolved run remaining `needs_reconciliation`. This does not claim automatic resolution of external uncertainty, rolling compatibility with an old binary, or arbitrary online backup support.

## Build and validate without mount privileges

Run from the project root. Select a never-used ID and provide an administrative loopback `/forge` DSN in the environment (the harness uses it to create two new fixture databases). The image below is the already configured Python digest in this checkout; a fresh deployment must explicitly select and pull its pinned profile first.

```bash
export FORGE_RECOVERY_ID=r20260911_a
export FORGE_RECOVERY_DOCKER=unix:///run/user/1000/forge-runtime-docker.sock
export FORGE_RECOVERY_IMAGE=python@sha256:229a2c5bfa27522db7815ea81f9bed70af17ccb9de9fc7ad142b1877b5830d36
# Export FORGE_RECOVERY_ADMIN_DSN from your existing local administrative secret.
python3 -I scripts/recovery/test_recovery.py -v
GOCACHE=/tmp/forge-runtime-gocache GOPROXY=off go test -c -o bin/recovery.test ./tests/review
python3 -I scripts/recovery/rehearse.py prepare --id "$FORGE_RECOVERY_ID" \
  --docker-host "$FORGE_RECOVERY_DOCKER" --image "$FORGE_RECOVERY_IMAGE"
```

`prepare` copies the existing reviewed `scripts/workspace-volumes` helpers into the new source/target scopes so their derived paths stay confined to each fixture. Its output gives the exact first privileged command. Review the populated source manifest and copied helper bytes before executing it. With the concrete ID above, the command is:

```bash
sudo /usr/bin/python3 -I "$PWD/var/recovery-rehearsals/r20260911_a/source/scripts/workspace-volumes/mount.py" mount
```

Then generate the actual source configuration:

```bash
python3 -I scripts/recovery/rehearse.py source-config --id "$FORGE_RECOVERY_ID"
```

## Source process, paired capture and target

Task UID 1000 private files require the same subordinate UID/GID mapping as the dedicated daemon. Run each process in a **fresh mapped RootlessKit namespace**, created after the corresponding host mounts exist. In a normal operator terminal:

```bash
rootlesskit --propagation=rslave \
  --state-dir="/run/user/$(id -u)/forge-recovery-source-$FORGE_RECOVERY_ID" \
  /usr/bin/python3 -I "$PWD/scripts/recovery/run-process.py" source --id "$FORGE_RECOVERY_ID"
```

The wrapper requires mapped UID 0, rejects host root, and accepts source exit 86 only with synced evidence. If the calling environment has `no-new-privileges`, the operator may use the already reviewed delegated user-systemd launch architecture; the script does not attempt sudo, change daemon configuration or start privileged containers.

After the source process completes:

```bash
python3 -I scripts/recovery/rehearse.py backup --id "$FORGE_RECOVERY_ID"
python3 -I scripts/recovery/freeze-copy.py --check --id "$FORGE_RECOVERY_ID"
```

Review that `--check` lists only this ID's four new-source mount paths. Execute the second privileged stage in the operator terminal:

```bash
sudo /usr/bin/python3 -I "$PWD/scripts/recovery/freeze-copy.py" --id r20260911_a
```

After successful copy/thaw, mount only the target using its newly generated manifest:

```bash
sudo /usr/bin/python3 -I "$PWD/var/recovery-rehearsals/r20260911_a/target/scripts/workspace-volumes/mount.py" mount
```

Then, from the normal operator account:

```bash
python3 -I scripts/recovery/rehearse.py restore --id "$FORGE_RECOVERY_ID"
rootlesskit --propagation=rslave \
  --state-dir="/run/user/$(id -u)/forge-recovery-target-$FORGE_RECOVERY_ID" \
  /usr/bin/python3 -I "$PWD/scripts/recovery/run-process.py" target --id "$FORGE_RECOVERY_ID"
```

Keep `source-process.log`, `target-process.log`, `backup/manifest.json`, `relocation.json`, and `recovery-report.json` with the raw paired assets. A successful final report requires reading restored private workspace and artifact bytes; matching SQL counts alone cannot pass. The fixed pool is not auto-released because the second operation remains unresolved.

## Additive schema upgrade rehearsal

This can run while privileged fixture stages await the operator. It uses a temporary private PostgreSQL schema and temporary SQLite/files, without any mount or Docker call:

```bash
FORGE_REVIEW_ALLOW_FIXTURES=1 \
FORGE_RECOVERY_UPGRADE_OUTPUT="$PWD/benchmarks/results/additive-upgrade-UNIQUE" \
GOCACHE=/tmp/forge-runtime-gocache GOPROXY=off \
  go test ./tests/review -run '^TestRecovery(PostgresAdditiveUpgrade|SQLiteV3AdditiveUpgrade)$' -count=1 -v
```

Set `FORGE_REVIEW_DATABASE_URL` to the explicit loopback administrative test DSN. PostgreSQL starts from the immutable v6 migration set with committed run/unknown-effect/artifact/snapshot data, applies v7/v8 twice, and preserves exact existing bytes. Legacy audit input stays NULL rather than being declared replayable. SQLite synthesizes a v3 fixture, opens it through the real v4 migration, retains request/receipt bytes, and treats missing legacy dispatch proof conservatively. Its backend is explicitly `TestBackend`; it substantiates migration and replay refusal, not Docker isolation. The fixture removes only its own synthetic v4 columns/registration when constructing historical v3; this is not a supported production downgrade command.

## Independent OTLP Collector

The collector must run as its own real process/container. The harness sends traces through the production OTLP HTTP exporter and checks the collector's JSON file, not a mock receiver or only the exporter's flush return. It asserts four related API/run/model/effect spans and no credential/body/query canary in exported bytes. Model and command observation hooks are exercised without dispatching providers or commands.

The [official v0.160.0 release](https://github.com/open-telemetry/opentelemetry-collector-contrib/releases/tag/v0.160.0) was released September 2, 2026. The checked-in config uses its documented [file exporter](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/v0.160.0/exporter/fileexporter/README.md) to persist OTLP JSON with a bounded flush interval. Pull this explicit release, inspect the actual digest, and pass the digest to the harness:

```bash
docker --host "$FORGE_RECOVERY_DOCKER" pull otel/opentelemetry-collector-contrib:0.160.0
docker --host "$FORGE_RECOVERY_DOCKER" image inspect \
  --format '{{json .RepoDigests}}' otel/opentelemetry-collector-contrib:0.160.0
# Set FORGE_COLLECTOR_IMAGE to the exact returned official repository digest.
python3 -I scripts/recovery/collector.py --docker-host "$FORGE_RECOVERY_DOCKER" \
  --mode process --image "$FORGE_COLLECTOR_IMAGE" --output "$PWD/benchmarks/results/collector-UNIQUE"
```

Default `--mode process` works with the dedicated daemon's `--bridge=none --iptables=false`. It creates an unstarted `--network=none` container only to copy the official `/otelcol-contrib` executable, records the image and binary digests, then runs that binary as the ordinary host user on a dynamically assigned loopback port. It never starts the extraction container or changes daemon networking. The process receives a minimal environment without platform credentials. The temporary binary and only that extraction container are removed after completion; collector logs, extraction metadata, actual image/binary digests, raw trace JSON and the delivery report remain. `--mode container` is an optional alternative for an ordinary daemon with working bridge/port publication; it retains the stopped collector container.

This proves collector delivery under the exercised local conditions, not durable telemetry storage across arbitrary collector failures.
