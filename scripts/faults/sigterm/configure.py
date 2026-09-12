#!/usr/bin/python3
"""Configure only a new, mounted lifecycle fixture; never launch or mount it."""
from __future__ import annotations

import argparse
import copy
import importlib.util
import json
import os
from pathlib import Path
import stat
import sys

sys.dont_write_bytecode = True


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


p = load_module("lifecycle_prepare", Path(__file__).with_name("prepare.py"))
SOURCE = "def clamp(v, lo, hi):\n    return v\n"
TARGET = ["python", "-I", "-B", "-c", "import runpy; a=runpy.run_path('/workspace/app.py'); assert a['clamp'](5,0,3)==3"]
REGRESSION = ["python", "-I", "-B", "-c", "import runpy; a=runpy.run_path('/workspace/app.py'); assert a['clamp'](2,0,3)==2"]


def inspect_binaries(base):
    result = {}
    for label, name in (("runner", "forge-runner"), ("worker", "forge-worker"), ("test", "application-faults.test")):
        path = base / "bin" / name
        p.directory(path.parent, private=True)
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.getuid() or info.st_mode & 0o022 or not info.st_mode & stat.S_IXUSR:
            raise ValueError("own non-writable-by-others executable required: " + name)
        result[label + "_binary"] = str(path)
        result[label + "_sha256"] = p.digest(path)
    return result


def runner_config(base, descriptor, slots):
    runtime = base / "runtime"
    return {
        "root_dir": str(runtime / "engine"), "journal_path": str(runtime / "journal.sqlite"),
        "artifact_root": str(runtime / "artifacts"), "signing_key_file": str(runtime / "runner.key"),
        "sources": {"lifecycle": str(runtime / "source")},
        "profiles": {"lifecycle-python": {
            "id": "lifecycle-python", "image": p.IMAGE, "user": "1000:1000",
            "memory_bytes": 256 << 20, "workspace_quota_bytes": p.IMAGE_BYTES,
            "cpus": 1, "pids": 64, "target_command": TARGET, "verify_command": REGRESSION,
        }},
        "server": {"unix_socket": "/tmp/forge-lifecycle-" + descriptor["fixture_id"] + "/runner.sock"},
        "docker_host": descriptor["docker_host"], "volume_slots": slots,
        "allow_test_backend": False,
        "logs": {"entry_bytes": 16384, "operation_bytes": 524288, "run_bytes": 16777216,
                 "max_operations": 32, "preview_bytes": 65536},
    }


def actual_slots(base, descriptor, config_module):
    common = config_module.common
    pool = base / "pool-root"
    with common.Tree(pool) as tree:
        if tree.uid != os.getuid():
            raise ValueError("ordinary pool owner required")
        with tree.locked():
            manifest = tree.read_json(common.MANIFEST, owner=tree.uid)
            state = tree.read_json(common.MOUNT_STATE, owner=0)
            slots = config_module.build_slots(manifest, state, tree)
            config_module.check_images(tree, manifest)
            rows = common.loop_rows()
            observations = []
            for slot, volume in zip(manifest["slots"], slots, strict=True):
                expected_path = pool / "var/workspace-storage/mounts" / slot["mount"]
                if volume["mount_path"] != str(expected_path):
                    raise ValueError("volume escaped the fixed fixture pool")
                # mkfs gives the filesystem root mode 0755; its parent scope
                # and mounts directory are 0700. Require owned/non-writable by
                # others without rewriting filesystem metadata to fit a test.
                p.directory(expected_path)
                root_info = expected_path.stat()
                if root_info.st_uid != os.getuid() or root_info.st_mode & 0o022:
                    raise ValueError("mounted root must remain owned and protected")
                mount = common.exact_mount(expected_path)
                if mount is None or mount["mount_id"] != state["slots"][slot["id"]]["mount_id"]:
                    raise ValueError("new pool has not been mounted with its recorded identity")
                matches = [r for r in rows if r.get("maj:min") == volume["device"]]
                if len(matches) != 1:
                    raise ValueError("single recorded loop association required")
                common.verify_loop(matches[0], slot)
                common.verify_mount(mount, matches[0])
                if set(os.listdir(expected_path)) - {"lost+found"}:
                    raise ValueError("initial fixture pool is not empty; retain existing ownership and data")
                st = os.stat(expected_path, follow_symlinks=False)
                if f"{os.major(st.st_dev)}:{os.minor(st.st_dev)}" != volume["device"]:
                    raise ValueError("mounted filesystem changed while inspecting")
                fs = os.statvfs(expected_path)
                device_bytes = int(Path("/sys/dev/block", volume["device"], "size").read_text().strip()) * 512
                if device_bytes != p.IMAGE_BYTES or not 0 < fs.f_blocks * fs.f_frsize <= device_bytes or not 0 < fs.f_files <= 65536:
                    raise ValueError("actual filesystem/device capacity does not match the fixed slot")
                observations.append({"slot_id": volume["id"], "mount": mount, "device_bytes": device_bytes,
                                     "filesystem_bytes": fs.f_blocks * fs.f_frsize, "inodes": fs.f_files})
    return slots, observations


def configure(identity):
    if os.getuid() == 0 or os.getuid() != os.geteuid():
        raise ValueError("configure as the ordinary unprivileged fixture owner")
    base, descriptor = p.read_preparation(identity)
    if os.path.lexists(base / "runtime") or os.path.lexists(base / "acceptance.json"):
        raise ValueError("configuration already exists or is partial; never replace a journal/key/config")
    binaries = inspect_binaries(base)
    config_module = load_module("lifecycle_volume_config", p.REPO / "cmd/forge-runner/configure-local.py")
    slots, observed = actual_slots(base, descriptor, config_module)
    if len(slots) != 4:
        raise ValueError("all four dedicated slots are required")
    config = runner_config(base, descriptor, slots)
    # All identity, binary and mount checks precede runtime/key creation. A
    # missing mount therefore cannot leave an apparently configured fixture.
    runtime = base / "runtime"
    runtime.mkdir(mode=0o700)
    (runtime / "source").mkdir(mode=0o700)
    p.create_file(runtime / "source/app.py", SOURCE.encode())
    p.create_file(runtime / "runner.key", os.urandom(32))
    p.save(runtime / "runner.json", config)
    # Separate trusted configurations isolate each admission guard. Restarting
    # between them must retain the journal/owner identity and first release all
    # prior work; the Go acceptance enforces that lifecycle boundary.
    variants = {}
    for name, field, value in (("byteguard", "run_bytes", 2 << 20), ("countguard", "max_operations", 2)):
        variant = copy.deepcopy(config)
        variant["logs"][field] = value
        path = runtime / ("runner-" + name + ".json")
        p.save(path, variant)
        variants[name] = {"path": str(path), "sha256": p.digest(path), "changed_field": "logs." + field, "value": value}
    p.save(runtime / "configuration-observations.json", {
        "scope": "new fixture configuration only; actual Go acceptance has not run",
        "volume_observations": observed, "source_sha256": p.digest(runtime / "source/app.py"),
        "runner_config_sha256": p.digest(runtime / "runner.json"),
        "runner_config_variants": variants,
        "signing_key": "new private 32-byte key; bytes and digest deliberately not archived",
    })
    acceptance = {"purpose": p.PURPOSE, "fixture_id": identity, "scope_root": str(base),
                  "pool_root": str(base / "pool-root"), "runner_config": str(runtime / "runner.json"),
                  "evidence_dir": str(base / "evidence/sigterm-01"), **binaries}
    p.save(base / "acceptance.json", acceptance)
    return {"configured": True, "acceptance": str(base / "acceptance.json"),
            "services_started": False, "privileged_actions": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--id", required=True)
    args = parser.parse_args()
    print(json.dumps(configure(args.id), indent=2, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, RuntimeError) as error:
        print("refused: " + str(error), file=sys.stderr)
        sys.exit(1)
