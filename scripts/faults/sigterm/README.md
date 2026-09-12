# Dedicated worker/runner and strict-log acceptance

This is an operator fixture for an actual `forge-worker` → actual
`forge-runner` → Docker → fixed ext4 volume → SQLite → PostgreSQL chain. The
provider is a deterministic local script: it makes no external model calls.
Preparing files, compiling the test, or passing offline checks is not evidence
that this chain has executed successfully.

The currently reviewed Go fixtures are bound to `lr20260912_a`, the local
rootless Docker socket, and loopback PostgreSQL on port 32773. A different
environment requires an explicit fixture change and review. Do not replace
these inputs with an original demo pool or recovery-rehearsal pool.

## Preparation and build

From the repository root, as its ordinary owner:

```bash
python3 -I scripts/faults/sigterm/prepare.py prepare \
  --id lr20260912_a \
  --docker-host unix:///run/user/1000/forge-runtime-docker.sock
python3 -I scripts/faults/sigterm/prepare.py inspect --id lr20260912_a
```

`prepare` creates four fully allocated 256 MiB images under
`var/lifecycle-rehearsals/lr20260912_a/pool-root/`. It copies the existing three
workspace-volume helpers without changing their bytes. Their fixed paths then
resolve to this new pool. It never mounts anything or controls services. An
existing or partial scope is retained and refused, not overwritten.

Freeze the reviewed source revision before building all three executables from
the same checkout. Capture the revision, input hashes, build output, Go build
metadata and final executable hashes with the evidence. The private `bin`
directory already exists; do not overwrite binaries from a prior attempt.

```bash
go build -buildvcs=true -o var/lifecycle-rehearsals/lr20260912_a/bin/forge-runner ./cmd/forge-runner
go build -buildvcs=true -o var/lifecycle-rehearsals/lr20260912_a/bin/forge-worker ./cmd/forge-worker
go test -c -o var/lifecycle-rehearsals/lr20260912_a/bin/application-faults.test ./scripts/faults/application
```

## Mount and configure

The operator mount command reported by `prepare` targets the new pool's copied
`scripts/workspace-volumes/mount.py`. In this desktop environment it must run
from the operator's terminal: inherited `no-new-privileges` prevents the agent
from obtaining mount privileges. The helper verifies the image identity,
capacity, loop association and exact mount destination before recording state.
No original pool needs to be stopped or unmounted.

After all four new slots are mounted and the executables are present:

```bash
python3 -I scripts/faults/sigterm/configure.py --id lr20260912_a
```

Configuration requires the actual four mounted filesystems to match the
recorded image and loop identities and to be unused. It creates a private
source snapshot, a fresh signing key and configuration, then writes
`acceptance.json` last. It does not create a runner journal, an Engine root,
ownership markers or a listening socket. It refuses an existing partial
runtime so that uncertain state cannot be reset by rerunning setup.

For ordinary users, `losetup` can report null backing inode/device fields.
Configuration still requires the live `/sys/dev/block/<device>` backing path,
zero offset, the fixed 256 MiB size limit and sector count, and read-write mode.
The exact backing image is separately checked through no-follow file
descriptors for its recorded device/inode, private ownership, full allocation
and ext4 UUID. Missing sysfs evidence or any explicit `losetup` identity
mismatch is rejected. Root-owned mount-state, exact mount ID/options, empty
slots and filesystem capacity remain required; copied privileged mount helpers
are unchanged. Configuration observations record which identity fields
`losetup` exposed and the checked sysfs values.

The runner uses its production Docker backend, the pinned Python image and
default strict framed-log limits: 16 KiB entries, 512 KiB per operation,
16 MiB cumulative per run, 32 process operations and 64 KiB previews. Separate
trusted `runner-byteguard.json` and `runner-countguard.json` configurations
lower only the cumulative run budget to 2 MiB or the operation count to 2.
Their roots, key, journal, sources, profiles and slots remain identical.
Changing configurations requires all preceding work to be settled and released.

## Actual execution

The private `var/local/review-database.env` must contain exactly one
`FORGE_REVIEW_DATABASE_URL` for the authorized local test database. The launcher
reads it as data, validates its endpoint, and passes the credential only in the
test child's environment. It is never a systemd argument or public artifact.

```bash
python3 -I scripts/faults/sigterm/run.py launch --phase sigterm
python3 -I scripts/faults/sigterm/run.py launch --phase logs
```

Run the log phase only after the SIGTERM phase succeeds and its production
cleanup has released all work. The second phase checks live journal and pool
state as well as the first phase's evidence; a success file alone is not an
authority to reuse a volume.

Each launch creates its own randomly named transient user service with
`Delegate=yes`, a fresh RootlessKit state directory and a full subordinate
UID/GID map. Only the new service's process group is subject to its deadline.
The existing API, workers, runner and Docker daemon stay running. The Go
fixtures spawn and stop their own actual worker/runner children, use separate
metrics endpoints, and enforce the production pool and process identity checks.

The SIGTERM case observes both shutdown boundaries while the original parent
and child container processes are still alive. It then waits for natural lease
expiry and verifies successor adoption against the same journal and operation.
Later adoption may stop the old writer for fencing; that is recorded separately
from shutdown. A capture gap remains incomplete and cannot become a successful
verification. This fixture does not claim a successful code repair.

The log case covers immutable publication and authenticated download,
policy-limit termination and explicit cancellation, cumulative reservation
guards, actual fixed-volume ENOSPC, and the prior SIGTERM gap. It uses the same
owned journal after release, preserving its UUID and volume ownership.

## Failure and evidence

Each phase has a single exclusive `evidence/host-<phase>` launcher directory.
The launcher will not overwrite it or automatically rerun a failed phase. It
does not kill or remove Docker jobs, change leases, rebind owners or reset a
journal. Follow the exact failed case's retained recovery descriptor before
attempting any recovery; preserve the original operation and container IDs.

Private credentials remain under `runtime/`; do not copy that tree wholesale
into public evidence. Archive only reviewed case records, process identities,
spool and receipt bytes, bounded logs, database observations and manifests.
Keep source/build/preflight success, actual case success and incomplete attempts
distinct when updating the acceptance ledger.

Offline preparation and launch-boundary checks:

```bash
python3 -I -m unittest discover -s scripts/faults/sigterm -p 'test_*.py' -v
```

These checks use temporary files and recording process objects; they execute no
systemd service, privileged helper, Docker job or paid model call.

## Explicit continuation after failure before runner startup

If the first trial failed before a journal, workspace or execution was created,
retain its original binaries, manifest, private database settings and failed
evidence. Freeze a reviewed new revision and build the three executables under
`bin/<full-40-character-revision>/`; never overwrite the original `bin/*` files.
Prepare separate manifests and execute each step explicitly:

```bash
python3 -I scripts/faults/sigterm/run.py prepare-continuation --revision <full-revision>
python3 -I scripts/faults/sigterm/run.py launch --phase abort
python3 -I scripts/faults/sigterm/run.py launch --phase sigterm --attempt 02
python3 -I scripts/faults/sigterm/run.py launch --phase logs --attempt 02
```

Preparation writes only `acceptance-abort-01.json` and `acceptance-02.json`.
The abort fixture verifies the exact original run and its absence of execution,
then requests cancellation through the production store. Its report explicitly
records nonterminal `cancel_requested`; it does not claim terminal cancellation
or successful lifecycle recovery. The original deadline remains unchanged.
The second trial must recheck that proof against the live original database and
unused pool, then use a different schema, run and private worker configuration.
Its worker never connects to the original schema. A later run cannot silently
resume or replace the first trial.

Trial 02 records SIGTERM evidence under `evidence/sigterm-02` and launcher logs
under `evidence/host-sigterm-02`. Its log phase still records the six cases under
`evidence/logs-01`, with L5 bound to the selected trial 02 SIGTERM evidence;
the log launcher uses `evidence/host-logs-02`. All outputs remain exclusive.
The child receives the validated private scope as `HOME`, preventing Docker
from looking for mapped-root credentials in `/root/.docker`.

The [integrated offline record](../../../benchmarks/results/lifecycle-integrated-offline-e50-20260912/identity.json)
identifies all four merged Go fixture sources. Ordinary and race checks each
pass 43 test/subtest entries and skip the three actual-environment entry points;
package vet also passes. Those results are preparation evidence only.

## Explicit continuation of the first log execution

`--attempt 02` selects the SIGTERM source. It continues to mean `logs-01` for
the original log invocation. A later, corrected log execution has its own
explicit selector and never overwrites that failed sample:

```bash
python3 -I scripts/faults/sigterm/run.py prepare-logs-continuation --revision <full-revision>
python3 -I scripts/faults/sigterm/run.py launch --phase logs-cleanup --attempt 02
python3 -I scripts/faults/sigterm/run.py launch --phase logs --attempt 02 --logs-execution 02
```

Preparation creates `acceptance-logs-cleanup-01.json` and
`acceptance-logs-02.json` from a new frozen binary directory, retaining the
original SIGTERM manifest and all old binaries. It performs no cleanup itself.
The first explicit phase handles only the known retained direct-RPC workspace;
it never needs a database credential or a model key. Its production
Stop/Snapshot/Release proof goes under `evidence/logs-01-cleanup`. An uncertain
outcome remains retained for inspection, with no automatic retry or deletion.

The new six-case sample goes under `evidence/logs-02`, with private database
settings under `runtime/strict-logs-private-02` and launcher evidence under
`evidence/host-logs-02-retry`. It requires the verified cleanup proof and an
idle unchanged journal before any new schema or workspace. L5 validates the
original SIGTERM manifest, old executable bytes and original process signals
separately from the new log executable identity; it never claims that E50 ran
the newer binary. Both phases acquire the existing fixture control lock.

The actual logs02 attempt stopped after Submit and before any runner/worker
start because its fixed publication-fault filename already belonged to logs01.
Its failed manifest, private schema and queued version1/epoch0 run are retained.
An additional read-only observation records zero execution/capacity and a
normalized journal comparison with the original preflight. It performs no
cancellation and does not authorize routing that schema to another worker.

The explicit third execution uses a run-specific publication-fault filename,
`acceptance-logs-03.json`, `evidence/logs-03`, `runtime/strict-logs-private-03`,
and `evidence/host-logs-03`. After freezing new binaries, use
`prepare-logs-third --revision FULL_REVISION`, then
`launch --phase logs --attempt 02 --logs-execution 03`. Before creating a new
schema or runner it rechecks the original cleanup plus the pinned logs02 failure
and read-only observation, historical binaries and retained fault files.
Existing manifests, tasks and actual test outcomes are never replaced.

These commands describe the reviewed continuation interface. Their presence
is not evidence that cleanup or the new full log execution has passed.
