#!/usr/bin/python3
"""Prepare a new four-volume lifecycle fixture; never mount or stop services.

Only this checkout's var/lifecycle-rehearsals/ID may be created. The existing
volume helpers are copied unchanged so their fixed roots refer to the new pool.
An incomplete preparation is retained for inspection, never overwritten.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys

sys.dont_write_bytecode = True
REPO = Path(__file__).absolute().parents[3]
ROOT = REPO / "var/lifecycle-rehearsals"
PURPOSE = "worker-runner-sigterm-v1"
IMAGE = "python@sha256:229a2c5bfa27522db7815ea81f9bed70af17ccb9de9fc7ad142b1877b5830d36"
HELPERS = ("common.py", "volumes.py", "mount.py")
IMAGE_BYTES = 256 * 1024 * 1024
COUNT = 4


def scope(identity):
    if not isinstance(identity, str) or not re.fullmatch(r"lr[a-z0-9_]{2,30}", identity):
        raise ValueError("fixture ID must match lr[a-z0-9_]{2,30}")
    return ROOT / identity


def directory(path, *, private=False):
    if not path.is_absolute() or ".." in path.parts:
        raise ValueError("absolute confined path required")
    current = Path(path.anchor)
    for part in path.parts[1:]:
        current /= part
        info = current.lstat()
        if not stat.S_ISDIR(info.st_mode):
            raise ValueError("directory chain contains a symlink or non-directory")
    if private and (info.st_uid != os.getuid() or info.st_mode & 0o077):
        raise ValueError("fixture directory must be owner-only")
    return path


def digest(path):
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        raise ValueError("regular single-link input required")
    h = hashlib.sha256()
    with path.open("rb") as f:
        while block := f.read(1 << 20):
            h.update(block)
    return h.hexdigest()


def create_file(path, data):
    directory(path.parent, private=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    parent = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(parent)
    finally:
        os.close(parent)


def save(path, value):
    create_file(path, (json.dumps(value, indent=2, sort_keys=True) + "\n").encode())


def docker_endpoint(host):
    if not isinstance(host, str) or not host.startswith("unix:///"):
        raise ValueError("explicit absolute Unix Docker endpoint required")
    path = host[7:]
    if any(c in path for c in "\0\n\r?#") or ".." in Path(path).parts or str(Path(path)) != path:
        raise ValueError("invalid Unix Docker endpoint")
    return host


def layout(identity, host):
    base = scope(identity)
    docker_endpoint(host)
    if os.getuid() == 0 or os.geteuid() != os.getuid():
        raise ValueError("prepare as the ordinary unprivileged checkout owner")
    directory(REPO)
    if REPO.stat().st_uid != os.getuid():
        raise ValueError("caller must own the checkout")
    directory(REPO / "var", private=True)
    # Verify fixed helper inputs before creating anything. No caller-supplied
    # source or executable path is accepted by this preparation entry point.
    sources = {name: REPO / "scripts/workspace-volumes" / name for name in HELPERS}
    hashes = {name: digest(path) for name, path in sources.items()}
    free = os.statvfs(REPO)
    if free.f_bavail * free.f_frsize < COUNT * IMAGE_BYTES + (128 << 20):
        raise ValueError("insufficient space for four fixed images and bounded fixture evidence")
    try:
        ROOT.mkdir(mode=0o700)
    except FileExistsError:
        pass
    directory(ROOT, private=True)
    base.mkdir(mode=0o700)
    pool = base / "pool-root"
    for path in (pool, pool / "scripts", pool / "scripts/workspace-volumes", pool / "var", base / "bin", base / "evidence"):
        path.mkdir(mode=0o700)
    for name, path in sources.items():
        create_file(pool / "scripts/workspace-volumes" / name, path.read_bytes())
    descriptor = {
        "schema_version": 1, "purpose": PURPOSE, "fixture_id": identity,
        "scope_root": str(base), "pool_root": str(pool), "image": IMAGE,
        "docker_host": host, "volume_count": COUNT,
        "bytes_per_volume": IMAGE_BYTES, "copied_helper_sha256": hashes,
        "scope": "new independent pool; no original/recovery pool, service, journal or owner marker changes",
    }
    save(base / "preparation.json", descriptor)
    return base, descriptor


def read_preparation(identity):
    base = directory(scope(identity), private=True)
    path = base / "preparation.json"
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.getuid() or info.st_mode & 0o077 or info.st_size > 65536:
        raise ValueError("private preparation descriptor required")
    descriptor = json.loads(path.read_text())
    pool = base / "pool-root"
    expected = {"schema_version": 1, "purpose": PURPOSE, "fixture_id": identity,
                "scope_root": str(base), "pool_root": str(pool), "image": IMAGE,
                "volume_count": COUNT, "bytes_per_volume": IMAGE_BYTES}
    if any(descriptor.get(k) != v for k, v in expected.items()):
        raise ValueError("preparation scope or policy mismatch")
    docker_endpoint(descriptor.get("docker_host"))
    hashes = descriptor.get("copied_helper_sha256")
    if not isinstance(hashes, dict) or set(hashes) != set(HELPERS):
        raise ValueError("exact helper set required")
    for name, expected_hash in hashes.items():
        helper = pool / "scripts/workspace-volumes" / name
        directory(helper.parent, private=True)
        if digest(helper) != expected_hash:
            raise ValueError("copied helper changed: " + name)
    return base, descriptor


def prepare(identity, host):
    base, descriptor = layout(identity, host)
    helper = base / "pool-root/scripts/workspace-volumes/volumes.py"
    logfd = os.open(base / "prepare.log", os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(logfd, "wb") as log:
        result = subprocess.run([sys.executable, "-I", str(helper), "prepare"],
                                stdout=log, stderr=subprocess.STDOUT, timeout=300,
                                env={"PATH": "/usr/sbin:/usr/bin:/sbin:/bin", "LC_ALL": "C"})
    if result.returncode:
        raise ValueError("volume preparation failed; retain this scope and prepare.log for inspection")
    read_preparation(identity)
    return {"prepared": True, "scope_root": str(base),
            "mount_status": "not requested; operator mount still required",
            "next_command": ["sudo", "/usr/bin/python3", "-I", str(base / "pool-root/scripts/workspace-volumes/mount.py"), "mount"]}


def inspect(identity):
    base, descriptor = read_preparation(identity)
    command = [sys.executable, "-I", str(base / "pool-root/scripts/workspace-volumes/volumes.py"), "inspect"]
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            text=True, timeout=90,
                            env={"PATH": "/usr/sbin:/usr/bin:/sbin:/bin", "LC_ALL": "C"})
    if result.returncode:
        raise ValueError("volume inspection failed: " + result.stderr[:1000])
    return {"preparation": descriptor, "inspection": json.loads(result.stdout)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    p = sub.add_parser("prepare")
    p.add_argument("--id", required=True)
    p.add_argument("--docker-host", required=True)
    p = sub.add_parser("inspect")
    p.add_argument("--id", required=True)
    args = parser.parse_args()
    value = prepare(args.id, args.docker_host) if args.action == "prepare" else inspect(args.id)
    print(json.dumps(value, indent=2, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        print("refused: " + str(error), file=sys.stderr)
        sys.exit(1)
