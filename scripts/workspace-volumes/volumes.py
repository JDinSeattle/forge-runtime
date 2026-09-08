#!/usr/bin/python3
"""Unprivileged, fixed-scope plan/prepare/inspect; never mounts or deletes images."""
import argparse
import os
from pathlib import Path
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).absolute().parent))
from common import (COUNT, MANIFEST, VOLUME_BYTES, Tree, UnsafeState, exact_mount,
                    image_fd, inspect_image_fd, json_print, new_manifest,
                    project_root, run_tool, validate_manifest)


def prepare_slot(tree, manifest, slot):
    if slot["state"] == "ready":
        with image_fd(tree, slot):
            return "verified_existing"
    images = tree.directory("images", create=True)
    mounts = tree.directory("mounts", create=True)
    try:
        try:
            os.mkdir(slot["mount"], 0o700, dir_fd=mounts)
        except FileExistsError:
            pass
        mountfd = os.open(slot["mount"], os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=mounts)
        try:
            if os.listdir(mountfd) or exact_mount(tree.storage_path / "mounts" / slot["mount"]):
                raise UnsafeState("new image requires an empty unmounted slot directory")
        finally:
            os.close(mountfd)
        try:
            fd = os.open(slot["image"], os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=images)
        except FileExistsError as exc:
            raise UnsafeState(f"{slot['id']}: unrecorded/partial image exists; never reformat it; inspect manually") from exc
        try:
            path = f"/proc/self/fd/{fd}"
            run_tool("fallocate", ["-l", str(VOLUME_BYTES), path], pass_fds=(fd,))
            os.fsync(fd)
            os.fsync(images)
            run_tool("mkfs", ["-F", "-q", "-m", "0", "-U", slot["filesystem_uuid"],
                              "-E", f"root_owner={tree.uid}:{tree.gid}", path], pass_fds=(fd,))
            # mkfs may discard unused extents. Re-reserve the full fixed image.
            run_tool("fallocate", ["-l", str(VOLUME_BYTES), path], pass_fds=(fd,))
            os.fsync(fd)
            slot.update(inspect_image_fd(fd, slot, tree.uid, recorded=False))
            slot["state"] = "ready"
            tree.write_json(MANIFEST, manifest)
        finally:
            os.close(fd)
        return "created"
    finally:
        os.close(images)
        os.close(mounts)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("plan", "prepare", "inspect"))
    args = parser.parse_args()
    root = project_root()
    if args.action == "plan":
        json_print({"project_root": str(root), "storage_root": str(root / "var/workspace-storage"),
                    "volume_count": COUNT, "bytes_per_volume": VOLUME_BYTES,
                    "aggregate_image_bytes": COUNT * VOLUME_BYTES,
                    "privileged_actions": "none; use separately reviewed mount.py after preparation"})
        return
    if os.geteuid() == 0:
        raise UnsafeState("prepare/inspect must run as the unprivileged project owner")
    with Tree(root, create=args.action == "prepare") as tree:
        if os.geteuid() != tree.uid:
            raise UnsafeState("caller must own the project")
        with tree.locked(create=args.action == "prepare"):
            try:
                manifest = validate_manifest(tree.read_json(MANIFEST, owner=tree.uid), tree)
            except FileNotFoundError:
                if args.action != "prepare":
                    raise
                manifest = new_manifest(tree)
                tree.write_json(MANIFEST, manifest)
            report = []
            for slot in manifest["slots"]:
                if args.action == "prepare":
                    action = prepare_slot(tree, manifest, slot)
                else:
                    if slot["state"] != "ready":
                        raise UnsafeState(f"{slot['id']}: incomplete preparation")
                    with image_fd(tree, slot):
                        action = "verified"
                report.append({"id": slot["id"], "action": action,
                               "image": str(tree.storage_path / "images" / slot["image"]),
                               "mount": exact_mount(tree.storage_path / "mounts" / slot["mount"])})
            json_print({"manifest": str(tree.storage_path / MANIFEST), "slots": report})


if __name__ == "__main__":
    try:
        main()
    except (UnsafeState, OSError, ValueError) as error:
        print(f"refused: {error}", file=sys.stderr)
        sys.exit(1)
