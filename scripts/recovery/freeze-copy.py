#!/usr/bin/python3
"""Freeze/copy only the four new source-rehearsal volumes; never the live pool."""
from __future__ import annotations

import argparse
import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import stat
import subprocess
import sys

sys.dont_write_bytecode = True
HERE = Path(__file__).absolute().parent
spec = importlib.util.spec_from_file_location("recovery_rehearse", HERE / "rehearse.py")
rehearse = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rehearse)
spec = importlib.util.spec_from_file_location("recovery_volume_common", rehearse.REPO / "scripts/workspace-volumes/common.py")
common = importlib.util.module_from_spec(spec)
spec.loader.exec_module(common)


def private_path(path, uid):
    if path.resolve() != path or path.is_symlink() or path.stat().st_uid != uid or path.stat().st_mode & 0o077:
        raise ValueError("unexpected recovery path identity")


def owned_directory(path, uid, gid):
    path.mkdir(mode=0o700)
    os.chown(path, uid, gid)


def create_provision_lock(storage, uid, gid):
    fd = os.open(storage / "provision.lock", os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        os.fchown(fd, uid, gid)
        os.fsync(fd)
    finally:
        os.close(fd)
    rehearse.sync_directory(storage)


def freeze(fd, enable):
    subprocess.run(["/usr/sbin/fsfreeze", "--freeze" if enable else "--unfreeze", f"/proc/self/fd/{fd}"], pass_fds=(fd,), check=True, timeout=20, env={"PATH": "/usr/bin:/usr/sbin", "LC_ALL": "C"})


def thaw_all(frozen, open_mounts):
    errors = []
    for fd in reversed(frozen):
        try:
            freeze(fd, False)
        except Exception as error:
            errors.append(str(error))
    for fd in open_mounts:
        try:
            os.close(fd)
        except OSError as error:
            errors.append(str(error))
    if errors:
        raise RuntimeError("attempted every thaw/close; operator must inspect the new source fixture mounts: " + "; ".join(errors))


def verify_source_mount(tree, slot):
    mount_path = tree.storage_path / "mounts" / slot["mount"]
    row = common.exact_mount(mount_path)
    candidates = [r for r in common.loop_rows() if r.get("back-maj:min") == f"{os.major(slot['device'])}:{os.minor(slot['device'])}" and int(r.get("back-ino", -1)) == slot["inode"]]
    if len(candidates) != 1 or row is None:
        raise ValueError("source fixture mount no longer matches its recorded image")
    common.verify_loop(candidates[0], slot)
    common.verify_mount(row, candidates[0])
    return mount_path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--id", required=True)
    parser.add_argument("--check", action="store_true", help="read-only validation/plan; never freeze, copy, or create paths")
    args = parser.parse_args()
    if (os.geteuid() != 0 and not args.check) or not sys.flags.isolated:
        raise ValueError("this narrow helper requires host root and Python -I")
    base = rehearse.scope(args.id)
    uid, gid = base.stat().st_uid, base.stat().st_gid
    if uid == 0:
        raise ValueError("fixture must belong to an ordinary operator")
    for path in (base, base / "source", base / "target", base / "backup"):
        private_path(path, uid)
    if not (base / "backup/metadata-boundary.json").is_file():
        raise ValueError("quiescent metadata/WAL backup must complete before image copying")
    evidence = json.loads((base / "source-evidence.json").read_text())
    if (Path("/proc") / str(evidence["pid"])).exists():
        raise ValueError("source fixture process remains present")
    with common.Tree(base / "source") as tree:
        source = common.validate_manifest(tree.read_json(common.MANIFEST, owner=uid), tree, require_ready=True)
        for slot in source["slots"]:
            verify_source_mount(tree, slot)
            with common.image_fd(tree, slot):
                pass
    if args.check:
        print(json.dumps({"check_only": True, "source": str(base / "source"), "backup": str(base / "backup"), "target": str(base / "target"), "source_slots": [slot["id"] for slot in source["slots"]], "freeze_mounts": [str(base / "source/var/workspace-storage/mounts" / slot["mount"]) for slot in source["slots"]], "copy_bytes": 2 * 4 * common.VOLUME_BYTES, "live_pool_modified": False}, indent=2))
        return
    target_root = base / "target"
    target_storage = target_root / "var/workspace-storage"
    owned_directory(target_storage, uid, gid)
    create_provision_lock(target_storage, uid, gid)
    for name in ("images", "mounts"):
        owned_directory(target_storage / name, uid, gid)
    backup_images = base / "backup/images"
    owned_directory(backup_images, uid, gid)
    open_mounts = []
    frozen = []
    with common.Tree(base / "source") as tree:
        source = common.validate_manifest(tree.read_json(common.MANIFEST, owner=uid), tree, require_ready=True)
        target = copy.deepcopy(source)
        target["project_root"] = str(target_root)
        try:
            for slot in source["slots"]:
                mount_path = verify_source_mount(tree, slot)
                fd = common.open_absolute_directory(mount_path)
                open_mounts.append(fd)
                # A timed-out subprocess may already have frozen the filesystem.
                # Attempt its thaw even when the freeze call did not return.
                frozen.append(fd)
                freeze(fd, True)
            for slot, restored in zip(source["slots"], target["slots"], strict=True):
                destinations = [backup_images / slot["image"], target_storage / "images" / slot["image"]]
                output = []
                try:
                    for destination in destinations:
                        fd = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
                        output.append(fd)
                        os.fchown(fd, uid, gid)
                        os.posix_fallocate(fd, 0, common.VOLUME_BYTES)
                    with common.image_fd(tree, slot) as source_fd:
                        offset = 0
                        while offset < common.VOLUME_BYTES:
                            data = os.pread(source_fd, min(1 << 20, common.VOLUME_BYTES - offset), offset)
                            if not data:
                                raise ValueError("source image truncated during frozen copy")
                            for fd in output:
                                cursor = 0
                                while cursor < len(data):
                                    cursor += os.write(fd, data[cursor:])
                            offset += len(data)
                    for fd in output:
                        os.fsync(fd)
                    info = os.fstat(output[1])
                    restored.update(device=info.st_dev, inode=info.st_ino, allocated_bytes=info.st_blocks * 512)
                finally:
                    for fd in output:
                        os.close(fd)
                owned_directory(target_storage / "mounts" / slot["mount"], uid, gid)
        finally:
            thaw_all(frozen, open_mounts)
    rehearse.sync_tree(target_storage)
    rehearse.sync_tree(backup_images)
    rehearse.write_json(target_storage / "manifest.json", target)
    os.chown(target_storage / "manifest.json", uid, gid)
    hashes = {}
    for path in sorted((base / "backup").rglob("*")):
        if path.is_symlink():
            raise ValueError("backup contains a symlink")
        if path.is_file():
            hashes[str(path.relative_to(base / "backup"))] = rehearse.digest(path)
    for slot in target["slots"]:
        if rehearse.digest(target_storage / "images" / slot["image"]) != hashes["images/" + slot["image"]]:
            raise ValueError("restored image bytes differ from the immutable backup")
    manifest = {"schema_version": 1, "id": args.id, "quiesced_source_process": evidence["pid"],
                "fixed_images": 4, "bytes_per_image": common.VOLUME_BYTES,
                "recovery_boundary": "No source fixture API/worker was started; source driver process and all dispatched jobs stopped before SQL/WAL/object capture; only new source fixture filesystems were frozen for image copy.",
                "sha256": hashes}
    rehearse.write_json(base / "backup/manifest.json", manifest)
    os.chown(base / "backup/manifest.json", uid, gid)
    print("Copied four frozen source images into immutable backup and separate target; all source filesystems thawed.")
    print("Next: sudo /usr/bin/python3 -I " + repr(str(target_root / "scripts/workspace-volumes/mount.py")) + " mount")
    print("Then: python3 -I scripts/recovery/rehearse.py restore --id " + args.id)


if __name__ == "__main__":
    main()
