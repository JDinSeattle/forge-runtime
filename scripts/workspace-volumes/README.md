# Fixed workspace volume preparation

These helpers implement the reviewable provisioning boundary proposed in [ADR 0002](../../docs/adr/0002-workspace-storage.md). They allocate **four 256MiB ext4 image files** below this repository's `var/workspace-storage/`: 1GiB aggregate image capacity, with ext4 overhead reducing usable checkout bytes. They do not start Docker, run task code, install tools, enable host quotas, alter existing filesystems or delete image data.

The application must lease each volume exclusively and verify its current mount/backing identity before execution. A successful preparation script is not itself evidence that Docker isolation, per-workspace leasing or container resource limits work.

## Review and unprivileged preparation

From the repository root:

```bash
# A checkout creates var/ with normal directory permissions. Verify it belongs
# to this user and is not a symlink, then explicitly make runtime state private.
test -d var && test -O var && test ! -L var && chmod 700 var
python3 -I scripts/workspace-volumes/volumes.py plan
python3 -m unittest discover -s scripts/workspace-volumes -p 'test_*.py' -v
python3 -I scripts/workspace-volumes/volumes.py prepare
python3 -I scripts/workspace-volumes/volumes.py inspect
```

`plan` creates nothing. `prepare` requires the repository owner, creates private directories, opens each new image with `O_EXCL|O_NOFOLLOW`, preallocates it, formats only that newly opened descriptor, re-reserves discarded extents and records its UUID/inode/device identity. Already-ready images are checked and skipped. It never reformats or deletes an existing image. `var/` and storage subdirectories must be owner-only; unexpected ownership/modes are rejected rather than silently changed.

Formatting/allocation errors may leave a partial image with a `planned` manifest entry. A rerun refuses to format that existing file. Inspect it and preserve evidence; any removal/recovery is a separate explicit operator action. Do not change the manifest to `ready` to bypass verification. Successful image creation is committed individually to an atomically replaced and synced manifest.

## Privileged mount stage

After reviewing the source and the populated manifest, run from this checkout's
repository root in an ordinary operator terminal:

```bash
sudo /usr/bin/python3 -I "$PWD/scripts/workspace-volumes/mount.py" mount
```

Only `mount`, `inspect`, `unmount` or `park` is accepted. Paths, byte limits, device names and mount options cannot be supplied by arguments. The root helper derives the repository from its own location and checks the fixed manifest scope. Python isolated mode is required; subprocesses use fixed system binaries, a clean environment and no shell. No persistent elevated permissions or privileged daemon is installed.

The helper opens each image through anchored, no-follow descriptors. `losetup` receives a pinned image descriptor and an explicit size limit; the helper checks backing inode/device and loop configuration before using it. `mount` receives pinned source-device and target-directory descriptors. A root-owned `mount-state.json` records exact loop/mount ownership. Existing matching mounts are reused only when that ownership record is present. Unrecorded or mismatched loops/mounts are left for inspection, never detached or overwritten automatically.

The script trusts the operator who owns and authorized this repository, its scripts and image files. It protects against accidental/symlink path substitution; it is not a sandbox for a malicious host user editing the approved program or image concurrently. Task containers must not receive these paths, the host directory, image files or provisioning helpers.

Read-only privileged inspection is:

```bash
sudo /usr/bin/python3 -I "$PWD/scripts/workspace-volumes/mount.py" inspect
```

Mount state and manifest are durable. A failure after attachment can leave an attached loop; a failure after successful mount can leave phase `attached`. An existing recorded exact match is inspected on retry. If a crash occurs before a newly attached loop can be recorded, the helper refuses automatic adoption and reports the unrecorded state. Preserve the image and inspect that exact loop manually; never use global loop cleanup.

## Release and teardown

First drain and release all workspace leases through the runner, preserve required receipts, and stop only the Forge runner and its dedicated rootless Docker service. The helper performs ordinary unmount first, allowing [shared/slave mount propagation](https://docs.kernel.org/filesystems/sharedsubtree.html) to remove inherited mounts. It then refuses loop detach if another mount namespace still references the volume, preserving the loop and ownership record for inspection or retry. It neither kills processes nor stops services itself.

```bash
sudo /usr/bin/python3 -I "$PWD/scripts/workspace-volumes/mount.py" unmount
```

Teardown refuses a slot with remaining checkout data, unexpected current-namespace mounts or mismatched ownership. It performs ordinary unmounts only, checks for remaining references in other namespaces, then detaches only the exact matching loop device. Busy state is an error; there is no force/lazy-unmount flag, `losetup -D`, Docker prune or image deletion. Images and manifests remain for reuse. Re-running `mount` after clean teardown uses the same files; it does not format them.

Mount and loop changes are not atomic across all four slots. A later error preserves already-confirmed state in `mount-state.json`; inspect it and rerun only after resolving the reported condition.

## Validation scope

The unit suite uses small temporary files and mocked filesystem-format/mount commands. It tests fixed-scope manifests, symlink/hardlink rejection, exclusive creation, existing-image preservation, descriptor-based calls, backing identity checks, mountinfo decoding, loop mismatch rejection and teardown ownership. It performs **no actual loop attachment, mount, mkfs, daemon operation or privilege escalation**.

Still required after real setup: nonroot task UID1000 creates 0600 files/0700 directories and the mapped runner can inspect/patch them; filling a disposable volume reaches ENOSPC without expanding the image or affecting another slot; actual cgroups enforce memory/PID/CPU limits; restarting runner/worker preserves volume and operation identity; unrelated rootful Docker workloads remain unchanged. See ADR 0002 for the complete acceptance scope.


For offline shutdown while retaining unresolved workspace evidence, stop the
project API, workers, runner and dedicated daemons first, then use `park` instead
of `unmount`. `park` permits retained files but keeps the exact image/mount/state
checks, post-unmount namespace-user rejection and ordinary busy-mount refusal. It does not
release logical leases, settle unknown costs, erase files or format images.
Remounting the same images preserves their journal-bound data for reconciliation.
