#!/usr/bin/python3
"""Privileged fixed-volume mount/inspect/unmount. No daemon actions or deletion."""
import argparse
import os
from pathlib import Path
import stat
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).absolute().parent))
from common import (MANIFEST, MOUNT_STATE, VOLUME_BYTES, Tree, UnsafeState,
                    exact_mount, image_fd, json_print, loop_rows, manifest_digest,
                    mount_records, project_root, run_tool, validate_manifest,
                    verify_loop, verify_mount)


def associated_loop(slot):
    dev = f"{os.major(slot['device'])}:{os.minor(slot['device'])}"
    matches = [r for r in loop_rows() if r.get("back-maj:min") == dev and int(r.get("back-ino", -1)) == slot["inode"]]
    if len(matches) > 1:
        raise UnsafeState("multiple loop mappings already reference this image")
    if matches:
        verify_loop(matches[0], slot)
        return matches[0]
    return None


def save_state(tree, manifest, state):
    state["manifest_sha256"] = manifest_digest(manifest)
    tree.write_json(MOUNT_STATE, state, mode=0o644)


def validated_state(tree, manifest):
    try:
        state = tree.read_json(MOUNT_STATE, owner=0)
    except FileNotFoundError:
        return {"schema_version": 1, "manifest_sha256": manifest_digest(manifest), "slots": {}}
    if not isinstance(state, dict) or state.get("schema_version") != 1 or state.get("manifest_sha256") != manifest_digest(manifest) or not isinstance(state.get("slots"), dict):
        raise UnsafeState("privileged state does not match prepared image manifest")
    if set(state["slots"]) - {s["id"] for s in manifest["slots"]}:
        raise UnsafeState("unknown slot in privileged state")
    return state


def other_namespace_users(device):
    """Refuse teardown while any other mount namespace retains this filesystem."""
    own = os.readlink("/proc/self/ns/mnt")
    seen = {own}
    users = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdecimal():
            continue
        try:
            namespace = os.readlink(entry / "ns/mnt")
            if namespace in seen:
                continue
            records = mount_records((entry / "mountinfo").read_text())
            seen.add(namespace)
            if any(r["device"] == device for r in records):
                users.append(int(entry.name))
        except (FileNotFoundError, ProcessLookupError):
            continue
        except PermissionError as exc:
            raise UnsafeState("cannot inspect mount namespace users; refusing teardown") from exc
    return users


def mounted_slot(tree, slot):
    loop = associated_loop(slot)
    row = exact_mount(tree.storage_path / "mounts" / slot["mount"])
    if row is not None:
        if loop is None:
            raise UnsafeState("mount exists without matching loop mapping")
        verify_mount(row, loop)
    return loop, row


def capacity_evidence(tree, slot, loop):
    mounts = tree.directory("mounts")
    try:
        fd = os.open(slot["mount"], os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=mounts)
        try:
            actual = os.fstat(fd).st_dev
            if f"{os.major(actual)}:{os.minor(actual)}" != loop["maj:min"]:
                raise UnsafeState("mount changed during capacity inspection")
            filesystem = os.fstatvfs(fd)
        finally:
            os.close(fd)
    finally:
        os.close(mounts)
    sectors = int(Path(f"/sys/dev/block/{loop['maj:min']}/size").read_text().strip())
    capacity = filesystem.f_blocks * filesystem.f_frsize
    if sectors * 512 != VOLUME_BYTES or not 0 < capacity <= VOLUME_BYTES or filesystem.f_files <= 0:
        raise UnsafeState("actual block-device/filesystem capacity does not match bounded image")
    return {"device_bytes": sectors * 512, "filesystem_bytes": capacity, "filesystem_inodes": filesystem.f_files}


def mount_slot(tree, manifest, state, slot):
    with image_fd(tree, slot, writable=True) as backing:
        loop, row = mounted_slot(tree, slot)
        if row:
            record = state["slots"].get(slot["id"])
            if not record or record.get("loop") != loop["name"] or record.get("device") != loop["maj:min"]:
                raise UnsafeState("matching mount has no privileged ownership record; inspect manually")
            return {"id": slot["id"], "action": "verified_existing", "mount": row, "capacity": capacity_evidence(tree, slot, loop)}
        mounts = tree.directory("mounts")
        try:
            target = os.open(slot["mount"], os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=mounts)
            try:
                info = os.fstat(target)
                if info.st_uid != tree.uid or info.st_mode & 0o077 or os.listdir(target):
                    raise UnsafeState("mountpoint must be private, runner-owned and empty")
                if loop and slot["id"] not in state["slots"]:
                    raise UnsafeState("unrecorded loop mapping exists; do not adopt or detach automatically")
                if not loop:
                    device = run_tool("losetup", ["--find", "--show", "--nooverlap", "--sizelimit", str(VOLUME_BYTES), f"/proc/self/fd/{backing}"], pass_fds=(backing,)).strip()
                    rows = loop_rows(device)
                    if len(rows) != 1:
                        raise UnsafeState("cannot inspect newly attached loop; preserve state for operator")
                    loop = rows[0]
                    verify_loop(loop, slot)
                state["slots"][slot["id"]] = {"phase": "attached", "loop": loop["name"], "device": loop["maj:min"], "mountpoint_inode": info.st_ino, "mountpoint_device": info.st_dev}
                save_state(tree, manifest, state)
                devicefd = os.open(loop["name"], os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
                try:
                    devinfo = os.fstat(devicefd)
                    if not stat.S_ISBLK(devinfo.st_mode) or f"{os.major(devinfo.st_rdev)}:{os.minor(devinfo.st_rdev)}" != loop["maj:min"]:
                        raise UnsafeState("loop block device changed before mount")
                    # FD paths pin both source device and target directory, avoiding
                    # shell interpolation and path/symlink substitution.
                    run_tool("mount", ["--no-canonicalize", "-t", "ext4", "-o", "rw,nodev,nosuid,errors=remount-ro", f"/proc/self/fd/{devicefd}", f"/proc/self/fd/{target}"], pass_fds=(devicefd, target))
                finally:
                    os.close(devicefd)
            finally:
                os.close(target)
        finally:
            os.close(mounts)
        _, observed = mounted_slot(tree, slot)
        if observed is None:
            raise UnsafeState("mount not visible at its fixed intended path; preserve attached volume")
        state["slots"][slot["id"]]["phase"] = "mounted"
        state["slots"][slot["id"]]["mount_id"] = observed["mount_id"]
        save_state(tree, manifest, state)
        return {"id": slot["id"], "action": "mounted", "mount": observed, "capacity": capacity_evidence(tree, slot, loop)}


def unmount_slot(tree, manifest, state, slot, *, preserve_contents=False):
    with image_fd(tree, slot):
        loop, row = mounted_slot(tree, slot)
        record = state["slots"].get(slot["id"])
        if not loop and not row:
            if record:
                state["slots"].pop(slot["id"])
                save_state(tree, manifest, state)
            return {"id": slot["id"], "action": "already_unmounted"}
        if not record or record.get("loop") != loop["name"] or record.get("device") != loop["maj:min"]:
            raise UnsafeState("no matching privileged ownership record for teardown")
        elsewhere = [r for r in mount_records() if r["device"] == loop["maj:min"] and r["target"] != str(tree.storage_path / "mounts" / slot["mount"])]
        if elsewhere:
            raise UnsafeState("another current-namespace mount references this device")
        if row:
            target = tree.storage_path / "mounts" / slot["mount"]
            mounts = tree.directory("mounts")
            try:
                fd = os.open(slot["mount"], os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=mounts)
            finally:
                os.close(mounts)
            try:
                # An allocated checkout must be released through the runner first.
                if not preserve_contents and any(name != "lost+found" for name in os.listdir(fd)):
                    raise UnsafeState("workspace contents remain; release leases through runner before teardown")
                os.syncfs(fd) if hasattr(os, "syncfs") else os.fsync(fd)
            finally:
                os.close(fd)
            # Drop our directory descriptor before ordinary unmount so the
            # helper itself does not keep the mount busy. No force/lazy option.
            if exact_mount(target) != row:
                raise UnsafeState("mount identity changed before unmount")
            run_tool("umount", ["--no-canonicalize", str(target)])
            if exact_mount(target):
                raise UnsafeState("mount remains after unmount; do not detach loop")
        # Ordinary unmount can propagate to inherited shared/slave mounts.
        # Check remaining namespaces only afterward, but always before detach,
        # including retries where the host mount is already gone.
        users = other_namespace_users(loop["maj:min"])
        if users:
            raise UnsafeState(f"slot retained in other mount namespaces after ordinary unmount (example PIDs {users[:8]}); loop and ownership record preserved")
        # Re-read exact backing identity before the single-device detach.
        current = associated_loop(slot)
        if current is None or current["name"] != record["loop"]:
            raise UnsafeState("loop association changed before detach")
        run_tool("losetup", ["--detach", current["name"]])
        if associated_loop(slot) is not None:
            raise UnsafeState("loop still has a backing association after detach request; preserve ownership record")
        state["slots"].pop(slot["id"], None)
        save_state(tree, manifest, state)
        return {"id": slot["id"], "action": "unmounted_images_preserved"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("mount", "inspect", "unmount", "park"))
    args = parser.parse_args()
    initial_map = Path("/proc/self/uid_map").read_text().split()
    if os.geteuid() != 0 or initial_map != ["0", "0", "4294967295"] or not sys.flags.isolated:
        raise UnsafeState("run this reviewed helper with pkexec /usr/bin/python3 -I <fixed-script> <action>")
    with Tree(project_root()) as tree, tree.locked():
        manifest = validate_manifest(tree.read_json(MANIFEST, owner=tree.uid), tree, require_ready=True)
        state = validated_state(tree, manifest)
        # Validate all image identities before the first privileged mutation.
        for slot in manifest["slots"]:
            with image_fd(tree, slot):
                pass
        report = []
        for slot in manifest["slots"]:
            if args.action == "mount":
                report.append(mount_slot(tree, manifest, state, slot))
            elif args.action in ("unmount", "park"):
                report.append(unmount_slot(tree, manifest, state, slot, preserve_contents=args.action == "park"))
            else:
                loop, row = mounted_slot(tree, slot)
                report.append({"id": slot["id"], "loop": loop, "mount": row,
                               "capacity": capacity_evidence(tree, slot, loop) if row else None})
        json_print({"action": args.action, "images_deleted": False, "slots": report})


if __name__ == "__main__":
    try:
        main()
    except (UnsafeState, OSError, ValueError) as error:
        print(f"refused: {error}", file=sys.stderr)
        sys.exit(1)
