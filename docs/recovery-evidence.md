# Operational recovery evidence — 2026-09-11

Additive PostgreSQL/SQLite migration and independent OTLP Collector delivery have passed the bounded local acceptance below. **The paired database/journal/workspace/object restore has not run yet.** Its new source images are prepared and independently inspected; the first source-pool mount requires the operator's terminal. The [recovery runbook](../scripts/recovery/README.md) contains the exact staged protocol and its authority boundaries.

## Additive upgrades: executed

| Dimension | Actual experiment | Result and limit |
| --- | --- | --- |
| PostgreSQL | A unique private schema starts from the immutable v6 migration files. Committed fixtures contain runs, an unknown effect, READY artifact metadata and a byte-preserving JSON snapshot including `9007199254740993` and `<>&`. Apply v7/v8 twice. | Existing run/effect/artifact rows and snapshot bytes remain exact; unknown effect remains unknown. New cleanup table has RLS. Legacy transition input columns remain NULL. |
| SQLite | A temporary historical v3 fixture contains one completed operation/receipt and one prepared operation without external evidence. Open it through the real v4 runner migration. | Two immutable request/receipt records survive. Legacy dispatch evidence defaults conservatively; inspection keeps the uncertain operation unknown and a repeated ID does not dispatch again. |
| Fixture isolation | PostgreSQL creates/drops only its uniquely named schema; SQLite and files use temporary directories. No public business-table cleanup or live schema downgrade. | No live runner, workspace mount or Docker task is used by these upgrade tests. |

Raw reports: [PostgreSQL 6→8](../benchmarks/results/additive-upgrade-20260911/postgres-upgrade.json), [SQLite 3→4](../benchmarks/results/additive-upgrade-20260911/sqlite-upgrade.json). Source: [upgrade_test.go](../tests/review/upgrade_test.go). The PostgreSQL integration run passed both tests; a subsequent local run of the current identity-aware SQLite fixture under `-race` passed in 1.166 seconds.

The SQLite backend is explicitly the no-process `TestBackend`. This verifies the real SQLite migration, immutable records and replay refusal; it does not substantiate Docker behavior. A historical v3 fixture is synthesized offline by removing only its own newly introduced v4 schema/registration before the upgrade. Existing receipt references survive; pre-v4 snapshot references that were never journaled cannot be reconstructed. These additive tests do not claim old/new binary rolling compatibility, a destructive down migration, or repair of an incompatible application snapshot.

## Independent Collector: executed

The harness extracted the official Collector Contrib binary from a digest-pinned image using an **unstarted** `--network=none` container, then ran that binary as an ordinary host process. This avoids changing the dedicated Docker daemon's `--bridge=none --iptables=false` configuration. The actual collector bound `127.0.0.1:39493`, received OTLP HTTP through the production exporter, and wrote the file-exporter JSON output. The temporary extraction container and executable were removed; image identity, binary digest, logs and trace bytes remain.

| Evidence | Actual value |
| --- | --- |
| Official release | `otelcol-contrib` 0.160.0 |
| Image digest | `sha256:799dc6cf12c96192af37b5bdba804da8c10b3bc563b43cb90c3f3c58d9572ad6` |
| Extracted binary SHA-256 | `8524ac54f6e1d4d00d9ba5eea91daadec2ebc31e4da80db9c17eba2e859ecdd4` |
| Actual collector process | PID 3990751, graceful exit 0 |
| Test result | PASS, package elapsed 0.174 seconds |
| Trace | `1acb3e9534d0d981b2d453b4f216a4f7` |
| Persisted topology | `POST unmatched` → `forge.run.drive` → `forge.model.request` and `forge.effect.execute` |
| Sensitive-data check | A canary in the HTTP authorization header, query and body is absent from collector output. |

The test uses a real TCP request through the production telemetry middleware, the persisted `traceparent` representation, and the production run/model/effect hooks. It asserts collector-file content and parent span IDs after flushing; a successful exporter return alone cannot pass. The model and effect hooks report an unknown observation without dispatching a model or command. This proves wire delivery and trace continuity in this sample. Crash/adoption links, every required metric, collector outage durability and actual provider execution are separate acceptance dimensions.

Raw evidence: [delivery report](../benchmarks/results/collector-20260911/delivery-report.json), [trace JSONL](../benchmarks/results/collector-20260911/collector-traces.jsonl), [test log](../benchmarks/results/collector-20260911/test.log), [collector log](../benchmarks/results/collector-20260911/collector.log), [image/process identity](../benchmarks/results/collector-20260911/collector-image.json), [unstarted extraction-container record](../benchmarks/results/collector-20260911/collector-extraction-container.json). Sources: [collector test](../tests/review/collector_test.go), [orchestrator](../scripts/recovery/collector.py), [configuration](../scripts/recovery/collector.yaml).

The official [v0.160.0 release](https://github.com/open-telemetry/opentelemetry-collector-contrib/releases/tag/v0.160.0) was published September 2, 2026. The [file-exporter documentation for that tag](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/v0.160.0/exporter/fileexporter/README.md) describes the JSON file and flush configuration used here.

## Paired restoration: prepared, awaiting execution

The dedicated fixture is `var/recovery-rehearsals/r20260911_a`. Its source and target root directories are mode 0700. Four source ext4 image files of 268,435,456 bytes each have been fully allocated and independently inspected as ready; all four currently have `mount: null`. The existing original four-volume live pool has not been changed.

The copied source/target provisioning helpers were compared byte-for-byte with the reviewed originals. The mount helper SHA-256 is `bfa00fab40652ba84e18409f85fd56bc1c1285923cfea6295a3bd31a7130027a`. The new [recovery helper tests](../scripts/recovery/test_recovery.py) pass 10 cases without sockets, Docker or mounts: confined IDs, mode-0700 scopes even under umask 022, exact backup file-set/hash checking, symlink rejection, all-attempt thaw/close behavior, and identity-preserving target-only artifact registration relocation.

The pending stages are:

1. Operator mounts only the four new source images using the copied fixed-scope helper.
2. The source process uses real PostgreSQL and Docker, writes a private workspace file, retains a separate uncertain operation and exits 86 with committed SQLite WAL.
3. Quiescent SQL/WAL/artifact capture is followed by a reviewed root helper freezing only the four new source filesystems, copying immutable backup and independent target images, and thawing every attempted mount.
4. Operator mounts the four new target images; an offline target-only relocation preserves journal identity, logical operation bytes, IDs and epochs while updating physical paths/device/inode and registry path.
5. The target verifies READY artifact bytes, reducer transition replay, a private file read and a sealed snapshot from restored ext4 storage, the original receipt, and blocked uncertainty. Every new target Docker `Start` is denied and counted; copied PostgreSQL leases/state are never renewed to make the test pass.

The source pool, backup images and target pool require approximately 3 GiB total plus metadata and eventually eight additional mounts. Source/target databases and artifact roots are independent of live services. Failure retains evidence; there is no automatic force-unmount, journal replacement, unknown-workspace release or image deletion. No complete recovery result, RPO/RTO or restored execution claim is recorded until the final `recovery-report.json` exists and its actual assertions pass.
