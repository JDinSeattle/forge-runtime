"""Fixed-scope storage provisioning primitives; no imports from application code."""
from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import uuid

COUNT = 4
VOLUME_BYTES = 256 * 1024 * 1024
ROOT_PARTS = ("var", "workspace-storage")
MANIFEST = "manifest.json"
MOUNT_STATE = "mount-state.json"
MAX_JSON = 65536
TOOLS = {name: path for name, path in (
    ("fallocate", "/usr/bin/fallocate"),
    ("mkfs", "/usr/sbin/mkfs.ext4"),
    ("blkid", "/usr/sbin/blkid"),
    ("losetup", "/usr/sbin/losetup"),
    ("mount", "/usr/bin/mount"),
    ("umount", "/usr/bin/umount"),
)}


class UnsafeState(RuntimeError):
    pass


def project_root() -> Path:
    # Root is derived from this checked-in file, never from argv or environment.
    return Path(__file__).absolute().parents[2]


def run_tool(name: str, args: list[str], *, pass_fds=(), timeout=60) -> str:
    result = subprocess.run(
        [TOOLS[name], *args], shell=False, check=False,
        pass_fds=tuple(pass_fds), timeout=timeout,
        env={"PATH": "/usr/sbin:/usr/bin:/sbin:/bin", "LC_ALL": "C"},
        text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    if result.returncode:
        raise UnsafeState(f"{name} failed ({result.returncode}): {result.stderr[:4096]}")
    return result.stdout


def _component(value: str) -> None:
    if not value or value in (".", "..") or "/" in value or "\0" in value:
        raise UnsafeState("unsafe path component")


def open_absolute_directory(path: Path) -> int:
    if not path.is_absolute() or ".." in path.parts:
        raise UnsafeState("project path must be absolute without traversal")
    fd = os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        for part in path.parts[1:]:
            _component(part)
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=fd)
            os.close(fd)
            fd = child
        return fd
    except BaseException:
        os.close(fd)
        raise


class Tree:
    """Anchor operations to no-follow directory descriptors below one repo."""

    def __init__(self, root: Path, *, create=False):
        self.root = root
        self.repo_fd = open_absolute_directory(root)
        owner = os.fstat(self.repo_fd)
        self.uid, self.gid = owner.st_uid, owner.st_gid
        if self.uid == 0:
            os.close(self.repo_fd)
            raise UnsafeState("project must belong to the unprivileged runner user")
        self.storage_path = root.joinpath(*ROOT_PARTS)
        fd = os.dup(self.repo_fd)
        try:
            for part in ROOT_PARTS:
                if create:
                    try:
                        os.mkdir(part, 0o700, dir_fd=fd)
                    except FileExistsError:
                        pass
                child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=fd)
                info = os.fstat(child)
                if info.st_uid != self.uid or info.st_mode & 0o077:
                    os.close(child)
                    raise UnsafeState(f"{part} must be runner-owned with mode 0700")
                os.close(fd)
                fd = child
            self.fd = fd
        except BaseException:
            os.close(fd)
            os.close(self.repo_fd)
            raise

    def close(self):
        os.close(self.fd)
        os.close(self.repo_fd)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def directory(self, name: str, *, create=False) -> int:
        _component(name)
        if create:
            try:
                os.mkdir(name, 0o700, dir_fd=self.fd)
            except FileExistsError:
                pass
        fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=self.fd)
        info = os.fstat(fd)
        if info.st_uid != self.uid or info.st_mode & 0o077:
            os.close(fd)
            raise UnsafeState(f"private directory {name} has unexpected owner/mode")
        return fd

    @contextlib.contextmanager
    def locked(self, *, create=False):
        flags = os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC
        if create:
            flags |= os.O_CREAT
        fd = os.open("provision.lock", flags, 0o600, dir_fd=self.fd)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_uid != self.uid or info.st_nlink != 1 or info.st_mode & 0o077:
                raise UnsafeState("unexpected provisioning lock ownership/type")
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            yield
        finally:
            os.close(fd)

    def read_json(self, name: str, *, owner: int | None = None):
        _component(name)
        fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=self.fd)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > MAX_JSON:
                raise UnsafeState("unexpected JSON file type/size/link count")
            if owner is not None and info.st_uid != owner:
                raise UnsafeState(f"unexpected owner of {name}")
            if info.st_mode & 0o022:
                raise UnsafeState(f"{name} is writable by group/other")
            raw = os.read(fd, MAX_JSON + 1)
            if len(raw) > MAX_JSON:
                raise UnsafeState("JSON file too large")
            return json.loads(raw)
        finally:
            os.close(fd)

    def write_json(self, name: str, value, *, mode=0o600):
        _component(name)
        data = (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()
        if len(data) > MAX_JSON:
            raise UnsafeState("JSON output too large")
        temp = f".{name}.{uuid.uuid4().hex}.tmp"
        fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, mode, dir_fd=self.fd)
        try:
            with os.fdopen(fd, "wb", closefd=False) as stream:
                stream.write(data)
                stream.flush()
                os.fsync(fd)
            os.rename(temp, name, src_dir_fd=self.fd, dst_dir_fd=self.fd)
            os.fsync(self.fd)
        finally:
            os.close(fd)
            try:
                os.unlink(temp, dir_fd=self.fd)
            except FileNotFoundError:
                pass


def slot_names(index: int) -> tuple[str, str]:
    label = f"slot-{index:03d}"
    return label, label + ".ext4"


def new_manifest(tree: Tree) -> dict:
    return {
        "schema_version": 1, "project_root": str(tree.root),
        "runner_uid": tree.uid, "runner_gid": tree.gid,
        "volume_count": COUNT, "bytes_per_volume": VOLUME_BYTES,
        "aggregate_image_bytes": COUNT * VOLUME_BYTES,
        "slots": [dict(id=slot_names(i)[0], image=slot_names(i)[1],
                       mount=slot_names(i)[0], bytes=VOLUME_BYTES,
                       filesystem_uuid=str(uuid.uuid4()), state="planned")
                  for i in range(1, COUNT + 1)],
    }


def validate_manifest(value: dict, tree: Tree, *, require_ready=False) -> dict:
    expected = {"schema_version": 1, "project_root": str(tree.root),
                "runner_uid": tree.uid, "runner_gid": tree.gid,
                "volume_count": COUNT, "bytes_per_volume": VOLUME_BYTES,
                "aggregate_image_bytes": COUNT * VOLUME_BYTES}
    if not isinstance(value, dict) or any(value.get(k) != v for k, v in expected.items()):
        raise UnsafeState("manifest scope does not match this fixed project configuration")
    slots = value.get("slots")
    if not isinstance(slots, list) or len(slots) != COUNT:
        raise UnsafeState("unexpected slot count")
    for index, slot in enumerate(slots, 1):
        name, image = slot_names(index)
        if not isinstance(slot, dict) or any(slot.get(k) != v for k, v in {"id": name, "image": image, "mount": name, "bytes": VOLUME_BYTES}.items()):
            raise UnsafeState("slot paths/limits do not match fixed configuration")
        try:
            parsed = str(uuid.UUID(slot["filesystem_uuid"]))
        except (KeyError, ValueError, TypeError, AttributeError) as exc:
            raise UnsafeState("invalid filesystem UUID") from exc
        if parsed != slot["filesystem_uuid"] or slot.get("state") not in {"planned", "ready"}:
            raise UnsafeState("invalid slot state/UUID")
        if require_ready and slot["state"] != "ready":
            raise UnsafeState(f"{name} has not completed unprivileged preparation")
        if slot["state"] == "ready":
            for key in ("device", "inode", "allocated_bytes"):
                if type(slot.get(key)) is not int or slot[key] <= 0:
                    raise UnsafeState("missing image identity/allocation evidence")
    return value


def manifest_digest(manifest: dict) -> str:
    return hashlib.sha256(json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def inspect_image_fd(fd: int, slot: dict, uid: int, *, recorded=True) -> dict:
    info = os.fstat(fd)
    if not stat.S_ISREG(info.st_mode) or info.st_uid != uid or info.st_nlink != 1 or info.st_mode & 0o077:
        raise UnsafeState(f"{slot['id']}: image must be private, regular, runner-owned and singly linked")
    if info.st_size != VOLUME_BYTES or info.st_blocks * 512 < VOLUME_BYTES:
        raise UnsafeState(f"{slot['id']}: image length/allocation does not match reserved capacity")
    if recorded and (info.st_dev != slot.get("device") or info.st_ino != slot.get("inode")):
        raise UnsafeState(f"{slot['id']}: backing file was replaced")
    output = run_tool("blkid", ["-p", "-o", "export", f"/proc/self/fd/{fd}"], pass_fds=(fd,))
    fields = dict(line.split("=", 1) for line in output.splitlines() if "=" in line)
    if fields.get("TYPE") != "ext4" or fields.get("UUID") != slot["filesystem_uuid"]:
        raise UnsafeState(f"{slot['id']}: filesystem type/UUID mismatch")
    return {"device": info.st_dev, "inode": info.st_ino, "allocated_bytes": info.st_blocks * 512}


@contextlib.contextmanager
def image_fd(tree: Tree, slot: dict, *, writable=False):
    directory = tree.directory("images")
    try:
        fd = os.open(slot["image"], (os.O_RDWR if writable else os.O_RDONLY) | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=directory)
        try:
            inspect_image_fd(fd, slot, tree.uid)
            yield fd
        finally:
            os.close(fd)
    finally:
        os.close(directory)


def decode_mount_path(value: str) -> str:
    return re.sub(r"\\([0-7]{3})", lambda m: chr(int(m.group(1), 8)), value)


def mount_records(text: str | None = None) -> list[dict]:
    if text is None:
        text = Path("/proc/self/mountinfo").read_text()
    result = []
    for line in text.splitlines():
        try:
            left, right = line.split(" - ", 1)
            fields, tail = left.split(), right.split()
            result.append({"mount_id": int(fields[0]), "device": fields[2],
                           "root": decode_mount_path(fields[3]),
                           "target": decode_mount_path(fields[4]),
                           "options": fields[5].split(","),
                           "type": tail[0], "source": decode_mount_path(tail[1])})
        except (ValueError, IndexError) as exc:
            raise UnsafeState("malformed mountinfo") from exc
    return result


def exact_mount(path: Path, records=None) -> dict | None:
    rows = [r for r in (mount_records() if records is None else records) if r["target"] == str(path)]
    if len(rows) > 1:
        raise UnsafeState("stacked mounts are not supported")
    return rows[0] if rows else None


def loop_rows(device: str | None = None) -> list[dict]:
    args = ["--json", "--list", "--output", "NAME,BACK-INO,BACK-MAJ:MIN,OFFSET,SIZELIMIT,RO,MAJ:MIN"]
    if device:
        if not re.fullmatch(r"/dev/loop[0-9]+", device):
            raise UnsafeState("unexpected loop device name")
        args.append(device)
    value = json.loads(run_tool("losetup", args))
    return value.get("loopdevices") or []


def verify_loop(row: dict, slot: dict) -> None:
    expected_dev = f"{os.major(slot['device'])}:{os.minor(slot['device'])}"
    if (not re.fullmatch(r"/dev/loop[0-9]+", str(row.get("name", "")))
            or int(row.get("back-ino", -1)) != slot["inode"]
            or row.get("back-maj:min") != expected_dev
            or int(row.get("offset", -1)) != 0
            or int(row.get("sizelimit", -1)) != VOLUME_BYTES
            or row.get("ro") not in (False, 0)):
        raise UnsafeState("loop identity/offset/size/read-write mode does not match image")


def verify_mount(row: dict, loop: dict) -> None:
    if (row["type"] != "ext4" or row["root"] != "/" or row["device"] != loop.get("maj:min")
            or not {"rw", "nosuid", "nodev"}.issubset(row["options"])):
        raise UnsafeState("mounted filesystem identity/options mismatch")


def json_print(value):
    print(json.dumps(value, indent=2, sort_keys=True))
