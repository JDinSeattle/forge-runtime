# ADR 0002: Bound durable workspaces with a private ext4 image pool

Date: 2026-09-07  
Status: Proposed from read-only host inspection; **no image, mount, daemon, quota or system configuration was changed during this investigation**. The commands below are a setup-script specification for review and subsequent authorization, not completed acceptance evidence.

## Decision and host evidence

Use a small, fixed pool of preallocated ext4 images, each exclusively leased to one workspace. Mount these images once under the project's `var/` directory through a narrowly scoped, authorized setup script. Run Forge's trusted runner through RootlessKit with namespace UID 0, and continue running task containers as namespace UID/GID 1000 with all capabilities dropped. Use a separate task-specific rootless Docker daemon and socket. Leave the existing rootful daemon, its containers and its data untouched.

This is the smallest proposed route to real container integration tests that provides both a durable aggregate disk bound and an inspectable per-workspace bound without installing a quota utility or changing the host root filesystem. It spends more space and limits the number of retained workspaces compared with project quotas. Those are deliberate initial tradeoffs.

The inspected host provides:

| Observation | Evidence read on this host | Implication |
| --- | --- | --- |
| Ubuntu 26.04.1, kernel `7.0.0-29-generic`, systemd 259. | `/etc/os-release`, `uname -r`, `systemd-run --version`. | Linux baseline is available; actual daemon tests are still required. |
| Project is on `/dev/nvme1n1p3`, ext4 mounted `rw,relatime`; no project-quota mount option. | Escalated read-only `findmnt -T <project>`. | Do not remount or alter the main filesystem for this project. |
| `/tmp` is tmpfs. | Host `findmnt -T /tmp`. | Put durable image files under project `var/`, not `/tmp`. |
| cgroup v2; user manager exposes `cpu memory pids`. | `/sys/fs/cgroup/user.slice/user-1000.slice/user@1000.service/cgroup.controllers`. | A delegated user service is plausible; inspect actual container controls before claiming enforcement. |
| Docker 29.8.0, RootlessKit 3.1.0, `dockerd-rootless.sh`, UID mapping helpers present; no user `docker.service` configured at inspection. | Installed binary versions, command locations and user-unit properties. | Start a separately named transient/user service later; do not use the setup utility's automatic default-context changes. |
| Host UID/GID 1000; subuid/subgid range `100000:65536`. | `id`, `/etc/subuid`, `/etc/subgid`. | Expected map is namespace 0→host 1000 and namespace 1..65536→host 100000..165535; capture actual maps after launch. |
| `fallocate`, `mkfs.ext4`, `mount`, `losetup`, `e2fsck`, `debugfs`, `chattr`, compiler and quota syscall headers present. | Command locations and local manuals/headers. | Image provisioning needs no package install. Mount/loop operations still require scoped host authorization. |
| Kernel has `CONFIG_QUOTA=y`, `CONFIG_QUOTACTL=y`, ext4 and user namespaces; quota format modules configured. | `/boot/config-7.0.0-29-generic`. | A quota-enabled *new image* is a possible later backend, not proof quotas currently work. |
| `/dev/fuse` exists; `fuse2fs` and `setquota` absent; `user_allow_other` commented in `/etc/fuse.conf`. | Escalated device/config inspection, package metadata. | Ordinary host-user FUSE cannot currently serve subordinate container UIDs simply by changing file modes. |

Sandbox-visible `/dev/fuse`, mount flags and user-bus errors differed from the host. Use actual host/daemon evidence when closing this gate, not the restricted command namespace's view.

## Fixed setup scope

For the first integration environment, choose these explicit values in the proposed script:

```text
project_root = /home/postedism/Desktop/mini claude code/forge-runtime
storage_root = <project_root>/var/workspace-storage
volume_count = 4
bytes_per_volume = 268435456           # 256 MiB, including filesystem overhead
aggregate_image_bytes = 1073741824     # 1 GiB across exactly four images
image_paths = <storage_root>/images/slot-{001,002,003,004}.ext4
mount_paths = <storage_root>/mounts/slot-{001,002,003,004}
host_runner_uid = 1000
host_runner_gid = 1000
task_namespace_uid = 1000
task_namespace_gid = 1000
```

These are proposed test limits, not measured capacity requirements. Each `WorkspaceQuotaBytes` for this profile must be at least the image's physical capacity. Usable source/build bytes are lower because ext4 metadata consumes space. Keep separate logical per-file, file-count and snapshot-size limits; a sparse file's logical size can exceed its allocated disk usage.

Image allocation, retained workspaces and active execution are different resources. An approval wait releases its execution slot but retains its volume. A full four-volume pool rejects further workspace provisioning with a capacity response, even when no command is currently active. Never reuse a volume while its old workspace remains active, retained for approval, or under reconciliation.

### Provisioning-script contract

The implementation agent should create a reviewable `scripts/provision-workspace-volumes.sh` with `plan`, `create`, `inspect` and `unmount` actions. Do not accept arbitrary paths, image sizes or device names from tool/model arguments. Keep the scope above in operator configuration, validate canonical paths under the fixed storage root and reject symlink components or preexisting unexpected objects. Record original state before mutations.

The unprivileged preparation stage creates only project-owned directories and new image files. For each slot, it must:

1. Create the image with exclusive-create semantics; never reformat an existing path. Refuse files that are symlinks, nonregular or have unexpected ownership/link count.
2. Preallocate exactly 268435456 bytes using `fallocate`. Do not substitute `truncate` after a failed reservation. Record file inode/device, length and allocated blocks, then sync the file and parent directory.
3. Format **only that new regular image** with `mkfs.ext4 -F -m 0 -E root_owner=1000:1000 <image>`. Keep the journal; no `data=writeback` or `noload` shortcuts. Record filesystem UUID, block size/count and inode count. Formatting can make metadata extents sparse; reapply the allocation reservation and verify allocated backing extents before publishing the image as provisioned.
4. Create the associated empty mount directory. Finish all preparation and show the exact image/mount manifest before requesting the privileged stage.

The approved privileged stage operates on that manifest only. It resolves each exact image with an open descriptor or otherwise prevents path substitution, verifies its identity/size, confirms the mount path is empty and unused, then attaches a fresh loop mapping and mounts ext4 with `rw,nodev,nosuid,errors=remount-ro`. Use the exact returned loop device rather than guessing `/dev/loopN`; do not use global loop cleanup. Persist the loop backing identity, offset, size limit, filesystem UUID and mount location atomically. Existing matching mounts are inspected and reused; existing mismatches fail closed.

Example *argument sequences* the script will ultimately issue, after those checks:

```text
fallocate -l 268435456 <exclusive-new-slot-image>
mkfs.ext4 -F -m 0 -E root_owner=1000:1000 <exclusive-new-slot-image>
fallocate -l 268435456 <formatted-slot-image>
losetup --find --show --nooverlap <verified-slot-image>
mount -t ext4 -o rw,nodev,nosuid,errors=remount-ro <returned-loop-device> <verified-empty-mountpoint>
```

Only loop attachment and mounting require host privilege in this backend. The runner, Docker daemon and task commands remain rootless. Do not grant general passwordless `mount`, `losetup`, shell or Python execution to the service. A future dynamic privileged broker is a separate design decision; the initial preprovisioned pool avoids one.

Keep privileged steps bounded and fail on partial state. A mounted volume is not automatically safe to delete because the setup command failed later. The manifest must let the operator inspect and unwind only this project's confirmed loop/mount objects.

## Rootless daemon and filesystem identity

Create a separately named user unit, for example `forge-docker.service`, with `Delegate=yes`, a dedicated data root under project `var/`, a dedicated exec/state directory and socket under `/run/user/1000/`, and an explicit configuration file. Start it through the installed `dockerd-rootless.sh`. Always supply the Forge socket or context to the Docker adapter; never mutate the user's default context and never fall back to `/var/run/docker.sock`.

Installed `dockerd-rootless.sh` uses RootlessKit `--propagation=rslave`; plain RootlessKit defaults to `rprivate`. Launch the runner with `rootlesskit --propagation=rslave --state-dir=<forge-runner-state> <forge-runner-binary> ...` so approved host mounts can become visible. Still verify the same image-backed filesystem from runner and daemon/container views before acceptance. RootlessKit supports several network backends on this installed version; the startup probe must establish which works. Do not bypass the script's network checks by modifying it or silently use a rootful daemon.

Docker documents cgroup resource flags in rootless mode as requiring cgroup v2 and systemd. The inspected delegation is promising, but the acceptance gate requires actual `memory.max`, `memory.swap.max`, `pids.max` and `cpu.max` values and exhaustion behavior in the task's cgroup. Daemon capability flags alone do not establish enforcement. [Docker rootless resource limits](https://docs.docker.com/engine/security/rootless/tips/)

The runner's namespace UID 0 maps to ordinary host UID 1000. It needs mapped filesystem capabilities over both its own files and task-created subordinate-UID files. Import checkout contents as the task UID/GID inside that namespace; directories can remain 0700 and files 0600/0700 as appropriate. The trusted baseline, journal, receipts and image files remain outside every task mount.

An initial recursive `chmod 0666/0777` is insufficient: a nonroot task can subsequently create a 0600 file or 0700 directory owned by host UID 100999. An ordinary host UID-1000 runner then cannot reliably inspect or patch it. Default ACLs or a shared group also do not establish enduring access because the owning task can restrict permissions. A mapped-namespace runner can use filesystem capabilities when both inode UID and GID are mapped; confirm that behavior against the actual task-created files before removing any temporary permissions workaround. [Linux user-namespace file capabilities](https://man7.org/linux/man-pages/man7/user_namespaces.7.html)

Do not run task commands as namespace root to solve this problem. All task container profiles retain UID/GID 1000, capability drop, no-new-privileges, read-only rootfs, private bounded tmpfs, default no network and no daemon socket. The trusted runner must never execute model-generated shell in its own namespace.

## Storage adapter and live quota verification

Add a storage lifecycle adapter alongside the existing `WorkspaceQuotaVerifier`; a verifier cannot itself create or reserve storage:

```text
Allocate(workspace_id, required_cap) -> exclusive volume lease + checkout path
Inspect(lease) -> mount/image identity, physical ceiling and allocation state
Release(lease, stopped_operation_evidence) -> durable release result
Recover() -> inspect existing leases/mounts before allowing new allocations
```

Persist the workspace-to-slot binding in the runner journal before importing files. The task-visible checkout is a child directory inside the leased filesystem; the slot mount itself may exist before workspace preparation. Change `PrepareWorkspace` so it imports into a newly allocated checkout rather than rejecting the preexisting mount as an ambiguous prior workspace. Repeated preparation uses the recorded lease. `ReleaseWorkspace` removes checkout contents through constrained access and releases the slot only after operations and evidence-retention requirements are resolved; it does not unmount the shared provisioning pool on each run.

Implement the real verifier from independent current observations:

1. Open the configured mount and checkout through constrained paths; match the workspace's recorded slot lease. Resolve the checkout's `st_dev` and inspect `/proc/self/mountinfo`; require the configured ext4 mount and reject a nested replacement mount or a plain directory on the host filesystem.
2. Match the observed major/minor device to `/sys/dev/block/<major>:<minor>/loop/backing_file`, loop offset/size limit and block device sector count. Canonicalize and match the protected backing image identity and stat length. Do not trust an image filename from a task.
3. Match filesystem UUID and inspect `statfs` capacity with checked integer arithmetic. Require the image/device/filesystem ceiling to be no larger than the profile cap, plus the finite configured inode capacity. Compare actual size, not free space or a recursive directory-size scan.
4. Require exclusive lease ownership, successful mount identity validation and the configured read/write/nosuid/nodev properties. A missing mount, changed backing inode, resized device, unreadable observation or capability mismatch produces `sandbox_unavailable`/reconciliation and no new operation.
5. Validate once at runner recovery and again immediately before command launch under the workspace operation gate. The deployment trusts the operator not to race unmount/resize against live jobs; normal teardown must acquire the same draining boundary.

This verifies an actual per-volume storage ceiling; it is not a fake verifier that returns success because a desired number appears in configuration. `statfs` free bytes alone would neither prove a quota nor reserve capacity. A fixed volume can exhaust before the numerical ceiling due to metadata/inodes, and that must surface as a bounded execution failure rather than a guarantee of a minimum writable size.

The image pool bounds task workspace storage only. Baselines, journal, artifacts, Docker image cache and bounded Docker logs have separate retention and capacity policies; do not label the 1GiB image total as the entire platform's disk footprint.

## Optional shared image with project quotas

The host has plausible prerequisites for one larger ext4 image with per-workspace project quotas. On a **new project image only**, the creation flags would be `-O quota,project -E quotatype=prjquota,root_owner=1000:1000`, with enforcement enabled on that image's mount. Assign each empty workspace directory a distinct project ID and inheritance flag before import. `chattr -p <id> +P <directory>` expresses the intended assignment. Keep the aggregate image preallocated and bounded. [ext4 features](https://man7.org/linux/man-pages/man5/ext4.5.html), [project inheritance](https://man7.org/linux/man-pages/man1/chattr.1.html)

There is no installed `setquota` here. A small reviewed helper could call `quotactl_fd` using `QCMD(Q_SETQUOTA, PRJQUOTA)` and read back `Q_GETQUOTA`; block limits use 1024-byte quota units, unlike filesystem blocks. Round the hard limit down to the allowed cap, set an inode hard limit, and reject zero/unlimited values. Quota setup and arbitrary-project inspection may need initial-namespace privilege; a mapped UID-0 runner is not host root. This backend therefore needs a narrow privileged setup/inspection broker or a separately authorized inspection command, not a static JSON manifest standing in for live quota checks. [Quota syscalls](https://man7.org/linux/man-pages/man2/quotactl.2.html)

Require tests that task users cannot change their project ID or clear inheritance. Current upstream code restricts project-ID/inheritance changes from noninitial user namespaces, but that is a reason to test the host kernel, not evidence its behavior has already been observed. [Linux file-attribute checks](https://github.com/torvalds/linux/blob/master/fs/file_attr.c)

If this backend is selected and quotas cannot be proved active, fail closed or select the explicitly configured per-volume backend. Do not accept a 1GiB shared filesystem as proof of a 64MiB per-workspace quota. Do not enable quotas on `/dev/nvme1n1p3`, change `/etc/fstab`, remount `/`, or alter unrelated filesystems. The initial per-volume pool is preferred because it avoids a privileged runtime broker.

FUSE is not selected for this initial host setup. `fuse2fs` is missing and a normal host-user FUSE mount denies subordinate-UID access without `allow_other`; enabling that requires a deliberate host configuration change here. A FUSE mount created in an unrelated user namespace also creates visibility/access constraints for sibling namespaces. It adds daemon-loss and durability behavior that would need its own fault tests. [FUSE access and namespaces](https://www.kernel.org/doc/html/latest/filesystems/fuse/fuse.html), [libfuse configuration](https://github.com/libfuse/libfuse/blob/master/util/fuse.conf)

## Recovery and cleanup

Worker or runner restart must leave all image files and mounts in place. Reload journal leases, inspect live Docker jobs and verify each volume identity before reopening workspace writes. A crashed runner is not a reason to format, reset permissions recursively, clear a slot or reapply a command. Missing mounts cause reconciliation, not automatic use of the now-visible empty host directory.

After host reboot, the approved provisioning script inspects and remounts the same images by recorded UUID/backing identity; it never reformats them. Check an unmounted image before reuse when needed. Filesystem repair is an explicit operator action with preserved image evidence, not an unconditional `e2fsck -y` during startup. Remounting storage does not prove old external effects stopped or that all data survived a host failure; preserve the baseline's recovery limitations.

Clean shutdown first drains Forge operations, confirms no task job uses the slots, stops the Forge runner, syncs each leased filesystem and preserves journal/evidence. Unmount only the four exact recorded mountpoints. If normal unmount reports busy, report it and retain state; do not use lazy/forced unmount or broad process kills. Detach only the corresponding loop devices after verifying their backing paths still match. Stop only the task-specific rootless Docker user unit after its Forge jobs are resolved. Preserve images by default; deletion is a separate explicit data-cleanup action. Never invoke `losetup -D`, Docker system prune or deletion against unrelated state.

## Required acceptance evidence

| Test | What must be observed |
| --- | --- |
| Nonroot read/write interoperability | Container reports UID/GID 1000; task creates 0600 file and 0700 nested directory, writes content; runner reads, hashes, patches and snapshots both through `os.Root`, including after runner restart. Capture UID/GID maps and host ownership. |
| Private control state | Task cannot reach image backing files, other slots, original baseline, journal, receipts, credentials or either Docker socket. |
| Per-volume exhaustion | Fill one disposable checkout with actual allocated bytes until ENOSPC (or earlier inode exhaustion in its separate test); image length and device capacity remain fixed, other slot remains writable, no host filesystem growth beyond bounded backing reservation. Clean up through normal runner/storage lifecycle. |
| Aggregate pool capacity | Four exclusive leases exhaust the pool; a fifth allocation is rejected without creating another image or reusing retained state. |
| Permission restriction regression | Task-created 0600/0700 content is still inspectable by the mapped runner, while a plain host UID-1000 probe lacks that access where expected. Initial chmod is not the acceptance mechanism. |
| Real cgroup enforcement | Inspect memory/swap/pids/cpu values; bounded memory-allocation and fork fixtures reach the configured limits; capture exit/kill evidence, no surviving controlled children and stable host behavior. |
| Rootless and isolation | Explicit Forge socket reports rootless+cgroup-v2/systemd; inspect read-only rootfs, network-none, capability drop, nonroot UID, bounded tmpfs and enforced resource flags. |
| Mount loss or replacement | Before new operation, absent/different backing mount is rejected; no fallback directory write; restore the original volume and reconcile. |
| Restart and command uncertainty | Restart worker/runner with an existing workspace and actual command; original image content/job identity/receipt are reused, no duplicate execution or volume reformat. |
| Cleanup isolation | Only exact Forge mounts/devices/user unit are touched; unrelated rootful workloads remain unchanged. Preserve command output and before/after inventory. |

These tests remain unexecuted at ADR creation. Their raw results are required before promoting workspace quota, UID interoperability or container resource enforcement to verified in the implementation map.
