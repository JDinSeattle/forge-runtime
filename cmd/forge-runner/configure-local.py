#!/usr/bin/env python3
"""Validate existing fixed-volume manifests and atomically configure the local runner.

No provisioning, daemon calls, signing-key reads, environment-file reads, or
changes outside var/local/runner.json are performed by this program.
"""
from __future__ import annotations

import argparse
import copy
import importlib.util
import json
import os
from pathlib import Path
import re
import stat
import struct
import sys
import uuid

sys.dont_write_bytecode = True
REPO = Path(__file__).absolute().parents[2]
_spec = importlib.util.spec_from_file_location("forge_volume_common", REPO / "scripts/workspace-volumes/common.py")
assert _spec and _spec.loader
common = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(common)
MAX_JSON = 1 << 20


def positive(value):
    return type(value) is int and value > 0


def build_slots(manifest, state, tree):
    common.validate_manifest(manifest, tree, require_ready=True)
    if (not isinstance(state, dict) or state.get("schema_version") != 1
            or state.get("manifest_sha256") != common.manifest_digest(manifest)):
        raise common.UnsafeState("mount-state does not match the complete prepared manifest")
    entries = state.get("slots")
    if not isinstance(entries, dict) or set(entries) != {s["id"] for s in manifest["slots"]}:
        raise common.UnsafeState("mount-state must contain exactly the four prepared slots")
    specs, devices, uuids, identities, mount_ids = [], set(), set(), set(), set()
    for slot in manifest["slots"]:
        mounted = entries[slot["id"]]
        if not isinstance(mounted, dict) or mounted.get("phase") != "mounted":
            raise common.UnsafeState("every slot must have recorded completed mounting")
        device = mounted.get("device", "")
        match = re.fullmatch(r"7:(0|[1-9][0-9]*)", device) if isinstance(device, str) else None
        if not match or mounted.get("loop") != "/dev/loop" + match[1]:
            raise common.UnsafeState("mount-state loop path/device mismatch")
        if any(not positive(mounted.get(k)) for k in ("mount_id", "mountpoint_device", "mountpoint_inode")):
            raise common.UnsafeState("mount-state is missing the original mount identity")
        identity = (slot["device"], slot["inode"])
        if (device in devices or slot["filesystem_uuid"] in uuids or identity in identities
                or mounted["mount_id"] in mount_ids or slot["allocated_bytes"] < common.VOLUME_BYTES):
            raise common.UnsafeState("slot identities must be distinct and fully allocated")
        devices.add(device); uuids.add(slot["filesystem_uuid"])
        identities.add(identity); mount_ids.add(mounted["mount_id"])
        specs.append({"id": slot["id"],
                      "mount_path": str(tree.root / "var/workspace-storage/mounts" / slot["mount"]),
                      "image_path": str(tree.root / "var/workspace-storage/images" / slot["image"]),
                      "image_device": slot["device"], "image_inode": slot["inode"],
                      "image_bytes": slot["bytes"], "device": device,
                      "filesystem_uuid": slot["filesystem_uuid"], "max_inodes": 65536})
    return specs


def check_images(tree, manifest):
    directory = tree.directory("images")
    try:
        for slot in manifest["slots"]:
            fd = os.open(slot["image"], os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=directory)
            try:
                info = os.fstat(fd)
                if (not stat.S_ISREG(info.st_mode) or info.st_uid != tree.uid or info.st_nlink != 1
                        or info.st_mode & 0o077 or info.st_size != common.VOLUME_BYTES
                        or info.st_blocks * 512 < common.VOLUME_BYTES
                        or info.st_dev != slot["device"] or info.st_ino != slot["inode"]):
                    raise common.UnsafeState("prepared backing image identity/protection changed")
                header = os.pread(fd, 120, 1024)
                if (len(header) != 120 or header[56:58] != b"\x53\xef"
                        or str(uuid.UUID(bytes=header[104:120])) != slot["filesystem_uuid"]
                        or not 0 < struct.unpack_from("<I", header)[0] <= 65536):
                    raise common.UnsafeState("backing ext4 UUID/inode ceiling does not match the supported pool")
            finally:
                os.close(fd)
    finally:
        os.close(directory)


def pinned_image(image):
    return isinstance(image, str) and re.fullmatch(r"[a-z0-9][a-z0-9._/:+-]*@sha256:[0-9a-f]{64}", image)


def image_repository(image):
    repository = image.split("@", 1)[0]
    prefix, separator, name = repository.rpartition("/")
    return prefix + separator + name.split(":", 1)[0]


def desired_config(current, specs, image, docker_host, root, *, profile_images=None):
    if image is not None and not pinned_image(image):
        raise common.UnsafeState("--image requires an explicit repository@sha256 digest")
    profile_images = {} if profile_images is None else profile_images
    if any(not pinned_image(value) for value in profile_images.values()):
        raise common.UnsafeState("every --profile-image requires an explicit repository@sha256 digest")
    if (not docker_host.startswith("unix:///") or "\0" in docker_host or "\n" in docker_host
            or "?" in docker_host or "#" in docker_host
            or str(Path(docker_host[7:])) != docker_host[7:] or ".." in Path(docker_host[7:]).parts):
        raise common.UnsafeState("--docker-host requires an explicit absolute Unix socket URL")
    if not isinstance(current, dict) or current.get("allow_test_backend", False):
        raise common.UnsafeState("configuration must use the real process backend")
    profiles = current.get("profiles")
    if not isinstance(profiles, dict) or not profiles:
        raise common.UnsafeState("run forge-admin local-setup before configuring the runner")
    if set(profile_images) - set(profiles):
        raise common.UnsafeState("--profile-image names an unknown profile")
    # Fail on an unexpected setup scope rather than silently relocating control
    # state or replacing signing-key/source paths. Those fields are preserved.
    if (current.get("root_dir") != str(root / "var/local/runner")
            or current.get("journal_path") != str(root / "var/local/runner.db")):
        raise common.UnsafeState("runner config is outside this checkout's local-setup scope")
    result = copy.deepcopy(current)
    for name, profile in result["profiles"].items():
        if (not isinstance(profile, dict) or profile.get("id") != name
                or profile.get("workspace_quota_bytes") != common.VOLUME_BYTES
                or profile.get("user") != "1000:1000"):
            raise common.UnsafeState("profile must retain the 256 MiB quota and nonroot task UID1000")
        executable_tmpfs = profile.get("tmpfs_executable", False)
        if type(executable_tmpfs) is not bool or (executable_tmpfs and not name.startswith("go-")):
            raise common.UnsafeState("only an explicitly named Go profile may enable the existing bounded executable tmpfs")
        existing = profile.get("image", "")
        if not isinstance(existing, str):
            raise common.UnsafeState("profile image must be a string")
        selected = profile_images.get(name)
        if selected is None and image is not None and image_repository(existing) == image_repository(image):
            selected = image
        if selected is None:
            selected = existing
        if not pinned_image(selected):
            raise common.UnsafeState(f"profile {name} needs an explicit --profile-image {name}=repository@sha256:digest")
        profile["image"] = selected
    result["volume_slots"] = specs
    result["docker_host"] = docker_host
    return result


def configure(image, docker_host, *, check=False, profile_images=None):
    with common.Tree(REPO) as tree:
        if os.getuid() != tree.uid:
            raise common.UnsafeState("run configuration as the unprivileged checkout owner")
        with tree.locked():
            manifest = tree.read_json(common.MANIFEST, owner=tree.uid)
            # Root-owned mount-state can appear unmapped in a user namespace.
            # Its full manifest digest and fixed loop/mount shape are checked;
            # the production runner separately verifies live kernel identities.
            state = tree.read_json(common.MOUNT_STATE)
            specs = build_slots(manifest, state, tree)
            check_images(tree, manifest)
            directory = common.open_absolute_directory(REPO / "var/local")
            try:
                parent = os.fstat(directory)
                if parent.st_uid != tree.uid or parent.st_mode & 0o077:
                    raise common.UnsafeState("var/local must be checkout-owner-only (0700)")
                fd = os.open("runner.json", os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=directory)
                try:
                    before = os.fstat(fd)
                    if (not stat.S_ISREG(before.st_mode) or before.st_uid != tree.uid
                            or before.st_nlink != 1 or before.st_mode & 0o077 or before.st_size > MAX_JSON):
                        raise common.UnsafeState("runner.json must be a private regular local-setup file")
                    raw = os.read(fd, MAX_JSON + 1)
                finally:
                    os.close(fd)
                if len(raw) > MAX_JSON:
                    raise common.UnsafeState("runner.json is too large")
                result = desired_config(json.loads(raw), specs, image, docker_host, REPO, profile_images=profile_images)
                data = (json.dumps(result, sort_keys=True, indent=2) + "\n").encode()
                if len(data) > MAX_JSON:
                    raise common.UnsafeState("generated configuration is too large")
                if check:
                    return "Validated four mounted-volume records and backing images; no files changed."
                if data == raw:
                    return "Runner configuration already matches the validated pool, socket and image digest."
                current = os.stat("runner.json", dir_fd=directory, follow_symlinks=False)
                if (current.st_dev, current.st_ino, current.st_mtime_ns, current.st_size) != (before.st_dev, before.st_ino, before.st_mtime_ns, before.st_size):
                    raise common.UnsafeState("runner.json changed during validation; retry after stopping the concurrent editor")
                temporary = ".runner.json." + uuid.uuid4().hex + ".tmp"
                target = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=directory)
                try:
                    with os.fdopen(target, "wb", closefd=False) as output:
                        output.write(data); output.flush(); os.fsync(target)
                    os.rename(temporary, "runner.json", src_dir_fd=directory, dst_dir_fd=directory)
                    os.fsync(directory)
                finally:
                    os.close(target)
                    try:
                        os.unlink(temporary, dir_fd=directory)
                    except FileNotFoundError:
                        pass
                return "Configured var/local/runner.json with four validated slots and the explicit socket/image digest."
            finally:
                os.close(directory)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", help="pin profiles using this same image repository; preserve other profiles")
    parser.add_argument("--profile-image", action="append", default=[], metavar="ID=REPOSITORY@sha256:DIGEST", help="explicit image for one profile; may be repeated")
    parser.add_argument("--docker-host", required=True, help="dedicated rootless Docker unix:///absolute/socket")
    parser.add_argument("--check", action="store_true", help="validate inputs without updating runner.json")
    args = parser.parse_args()
    try:
        profile_images = {}
        for item in args.profile_image:
            name, separator, value = item.partition("=")
            if not separator or not name or name in profile_images:
                raise common.UnsafeState("--profile-image requires unique ID=IMAGE entries")
            profile_images[name] = value
        print(configure(args.image, args.docker_host, check=args.check, profile_images=profile_images))
    except (common.UnsafeState, OSError, ValueError, TypeError, KeyError) as exc:
        print(f"Runner configuration rejected: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
