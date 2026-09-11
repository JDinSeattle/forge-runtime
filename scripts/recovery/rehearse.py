#!/usr/bin/env python3
"""Prepare and restore one independent, quiescent Forge recovery rehearsal.

No live configuration, original workspace pool, daemon, or worker is modified.
Privileged mount/freeze commands are printed for a separate operator terminal.
"""
from __future__ import annotations

import argparse
import datetime
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import subprocess
import sys
from urllib.parse import urlsplit, unquote

sys.dont_write_bytecode = True
REPO = Path(__file__).absolute().parents[2]
ROOT = REPO / "var/recovery-rehearsals"
BYTES = 256 * 1024 * 1024


def scope(identity):
    if not re.fullmatch(r"[a-z][a-z0-9_]{2,35}", identity):
        raise ValueError("rehearsal ID must match [a-z][a-z0-9_]{2,35}")
    return ROOT / identity


def digest(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        while block := f.read(1 << 20):
            h.update(block)
    return h.hexdigest()


def write_json(path, value):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "w") as f:
        json.dump(value, f, indent=2, sort_keys=True)
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())
    sync_directory(path.parent)


def sync_directory(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def sync_tree(root):
    for entry in sorted(root.rglob("*"), reverse=True):
        if entry.is_symlink():
            raise ValueError("recovery input contains a symlink")
        if entry.is_dir():
            sync_directory(entry)
        elif entry.is_file():
            with entry.open("rb") as f:
                os.fsync(f.fileno())
        else:
            raise ValueError("recovery input contains a nonregular file")
    sync_directory(root)


def verify_backup(backup):
    proof = json.loads((backup / "manifest.json").read_text())
    if proof.get("schema_version") != 1 or proof.get("fixed_images") != 4 or proof.get("bytes_per_image") != BYTES:
        raise ValueError("unsupported paired backup manifest")
    hashes = proof["sha256"]
    actual = set()
    for entry in backup.rglob("*"):
        if entry.is_symlink() or not (entry.is_file() or entry.is_dir()):
            raise ValueError("backup contains a nonregular entry")
        if entry.is_file() and entry != backup / "manifest.json":
            actual.add(str(entry.relative_to(backup)))
    if set(hashes) != actual:
        raise ValueError("backup file set differs from its manifest")
    for name, expected in hashes.items():
        candidate = backup / name
        if candidate.resolve() != candidate or not candidate.is_relative_to(backup) or not re.fullmatch(r"[0-9a-f]{64}", expected):
            raise ValueError("invalid backup manifest path or hash")
        if digest(candidate) != expected:
            raise ValueError("backup digest mismatch: " + name)
    return proof


def rebind_artifact_journal(artifacts, source_journal, target_journal):
    # This operates only on a new target copy; it never edits source or backup.
    old_name = ".runner-journal-" + hashlib.sha256(str(source_journal).encode()).hexdigest()
    markers = list(artifacts.glob(".runner-journal-*"))
    if len(markers) != 1 or markers[0].name != old_name or markers[0].is_symlink():
        raise ValueError("paired artifact backup does not have the exact sole source journal authority")
    original = json.loads(markers[0].read_text())
    database = sqlite3.connect("file:" + str(target_journal) + "?mode=ro", uri=True)
    try:
        identity = database.execute("SELECT id FROM journal_identity WHERE singleton=1").fetchone()[0]
    finally:
        database.close()
    if not re.fullmatch(r"[0-9a-f]{64}", identity) or original != {"journal_path": str(source_journal), "journal_id": identity}:
        raise ValueError("paired artifact journal identity mismatch")
    new_name = ".runner-journal-" + hashlib.sha256(str(target_journal).encode()).hexdigest()
    with (artifacts / new_name).open("x") as f:
        json.dump({"journal_path": str(target_journal), "journal_id": identity}, f, sort_keys=True)
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())
    (artifacts / new_name).chmod(0o600)
    sync_directory(artifacts)
    markers[0].unlink()
    sync_directory(artifacts)
    return {"before": str(source_journal), "after": str(target_journal), "journal_id": identity, "offline_target_only": True}


def pg_env(database):
    u = urlsplit(os.environ["FORGE_RECOVERY_ADMIN_DSN"])
    if u.scheme not in ("postgres", "postgresql") or u.hostname != "127.0.0.1" or u.path != "/forge":
        raise ValueError("explicit loopback /forge administrative DSN required")
    return dict(os.environ, PGHOST=u.hostname, PGPORT=str(u.port or 5432),
                PGUSER=unquote(u.username or ""), PGPASSWORD=unquote(u.password or ""),
                PGDATABASE=database, PGSSLMODE="disable")


def copy_tree(source, destination):
    destination.mkdir(mode=0o700)
    for entry in sorted(source.rglob("*")):
        relative = entry.relative_to(source)
        if entry.is_symlink():
            raise ValueError("recovery input contains a symlink")
        target = destination / relative
        if entry.is_dir():
            target.mkdir(mode=0o700)
        elif entry.is_file():
            shutil.copyfile(entry, target)
            target.chmod(0o600)
            with target.open("rb") as f:
                os.fsync(f.fileno())
        else:
            raise ValueError("recovery input contains a nonregular file")
    sync_tree(destination)


def slots(base):
    storage = base / "var/workspace-storage"
    manifest = json.loads((storage / "manifest.json").read_text())
    state = json.loads((storage / "mount-state.json").read_text())
    spec = importlib.util.spec_from_file_location("recovery_configure", REPO / "cmd/forge-runner/configure-local.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    with module.common.Tree(base) as tree:
        result = module.build_slots(manifest, state, tree)
        module.check_images(tree, manifest)
    return result


def prepare(args):
    if not re.fullmatch(r"[a-z0-9][a-z0-9._/:+-]*@sha256:[0-9a-f]{64}", args.image or ""):
        raise ValueError("an explicit pinned Python image is required")
    if not (args.docker_host or "").startswith("unix:///"):
        raise ValueError("an explicit dedicated rootless Docker socket is required")
    ROOT.mkdir(mode=0o700, exist_ok=True)
    base = scope(args.id)
    base.mkdir(mode=0o700)
    for label in ("source", "target"):
        root = base / label
        root.mkdir(mode=0o700)
        (root / "scripts").mkdir(mode=0o700)
        helpers = root / "scripts/workspace-volumes"
        helpers.mkdir(mode=0o700)
        (root / "var").mkdir(mode=0o700)
        for name in ("common.py", "volumes.py", "mount.py"):
            shutil.copyfile(REPO / "scripts/workspace-volumes" / name, helpers / name)
        (root / "runtime").mkdir(mode=0o700)
    (base / "source/input").mkdir(mode=0o700)
    (base / "source/input/seed.txt").write_text("recovery source snapshot\n")
    (base / "source/runner.key").write_bytes(os.urandom(32))
    (base / "source/runner.key").chmod(0o600)
    config = {"schema_version": 1, "id": args.id, "root": str(base),
              "source_database": "forge_recovery_s_" + args.id,
              "target_database": "forge_recovery_t_" + args.id,
              "docker_host": args.docker_host, "image": args.image}
    write_json(base / "rehearsal.json", config)
    subprocess.run([sys.executable, "-I", str(base / "source/scripts/workspace-volumes/volumes.py"), "prepare"], check=True)
    print("Source fixture prepared; current live four-volume pool was not touched.")
    print("Next: sudo /usr/bin/python3 -I " + repr(str(base / "source/scripts/workspace-volumes/mount.py")) + " mount")
    print("Then: python3 -I scripts/recovery/rehearse.py source-config --id " + args.id)


def source_config(args):
    base = scope(args.id)
    config = json.loads((base / "rehearsal.json").read_text())
    config["volume_slots"] = slots(base / "source")
    write_json(base / "source-config.json", config)
    print("Run TestRecoverySourceProcess in a fresh mapped RootlessKit namespace with FORGE_RECOVERY_CONFIG=" + str(base / "source-config.json"))
    print("Expected deliberate process exit: 86, after all source fixture jobs stop and source-evidence.json is synced.")


def backup(args):
    base = scope(args.id)
    config = json.loads((base / "source-config.json").read_text())
    evidence = json.loads((base / "source-evidence.json").read_text())
    if (Path("/proc") / str(evidence["pid"])).exists():
        raise ValueError("source fixture process must have exited before paired backup")
    # Only the completed job ID created by this fixture is inspected. The
    # deliberately uncertain second operation never reached Docker dispatch.
    result = subprocess.run(["docker", "--host", config["docker_host"], "inspect", evidence["completed_operation"]["job_id"]], check=True, capture_output=True, text=True)
    job = json.loads(result.stdout)[0]
    if job["State"]["Running"] or job["State"]["ExitCode"] != 0:
        raise ValueError("source fixture Docker job has not stopped successfully")
    destination = base / "backup"
    destination.mkdir(mode=0o700)
    subprocess.run(["pg_dump", "--format=custom", "--no-owner", "--no-acl", "--file", str(destination / "database.dump")], env=pg_env(config["source_database"]), check=True)
    journal = base / "source/runtime/runner.db"
    wal = Path(str(journal) + "-wal")
    if not wal.is_file() or wal.stat().st_size == 0:
        raise ValueError("expected source WAL evidence; do not claim a WAL backup from an empty file")
    wal_bytes = wal.stat().st_size
    shutil.copyfile(wal, destination / "runner.db-wal.evidence")
    # SQLite's backup API merges the committed WAL into a standalone database.
    # Copying runner.db alone is deliberately not the restoration protocol.
    original = sqlite3.connect("file:" + str(journal) + "?mode=ro", uri=True)
    restored = sqlite3.connect(destination / "runner.db")
    original.backup(restored)
    if restored.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
        raise ValueError("SQLite snapshot integrity failed")
    restored.close()
    original.close()
    copy_tree(base / "source/runtime/artifacts", destination / "artifacts")
    copy_tree(base / "source/input", destination / "input")
    for src, name in [(base / "source/runner.key", "runner.key"), (base / "source-config.json", "source-config.json"), (base / "source-evidence.json", "source-evidence.json")]:
        shutil.copyfile(src, destination / name)
        (destination / name).chmod(0o600)
    sync_tree(destination)
    write_json(destination / "metadata-boundary.json", {"quiesced_process_pid": evidence["pid"], "source_job_stopped": True, "source_wal_bytes": wal_bytes, "sqlite_backup_api": True, "database_dump_sha256": digest(destination / "database.dump")})
    print("Metadata/WAL/object backup complete; source has no running fixture process or job.")
    print("Next: sudo /usr/bin/python3 -I " + repr(str(REPO / "scripts/recovery/freeze-copy.py")) + " --id " + args.id)


def restore(args):
    base = scope(args.id)
    backup = base / "backup"
    verify_backup(backup)
    config = json.loads((backup / "source-config.json").read_text())
    target_slots = slots(base / "target")
    # Target SQL is a freshly created database, not a schema alias and never a
    # destination for the production worker. CREATE DATABASE refuses reuse.
    subprocess.run(["createdb", "--template=template0", config["target_database"]], env=pg_env("forge"), check=True)
    subprocess.run(["pg_restore", "--exit-on-error", "--no-owner", "--no-acl", "--dbname", config["target_database"], str(backup / "database.dump")], env=pg_env(config["target_database"]), check=True)
    target = base / "target"
    copy_tree(backup / "artifacts", target / "runtime/artifacts")
    copy_tree(backup / "input", target / "input")
    shutil.copyfile(backup / "runner.key", target / "runner.key")
    (target / "runner.key").chmod(0o600)
    shutil.copyfile(backup / "runner.db", target / "runtime/runner.db")
    database = sqlite3.connect(target / "runtime/runner.db")
    logical_before = list(database.execute("SELECT request_json,status,job_id,receipt_json FROM operations ORDER BY id"))
    bindings = []
    for old, new in zip(config["volume_slots"], target_slots, strict=True):
        if old["id"] != new["id"] or old["filesystem_uuid"] != new["filesystem_uuid"] or old["image_bytes"] != new["image_bytes"]:
            raise ValueError("restored slot logical identity differs")
        stored = json.loads(database.execute("SELECT spec_json FROM volume_slots WHERE id=?", (old["id"],)).fetchone()[0])
        if stored != old:
            raise ValueError("backup journal did not bind the original slot")
        owner = Path(new["mount_path"]) / ".forge-pool.owner"
        if owner.read_text() != str(base / "source/runtime/runner.db") + "\n":
            raise ValueError("restored owner marker did not bind the source journal")
        # Explicit offline physical relocation: only this new clone is changed.
        # Workspace IDs, operation IDs/bytes/epochs/job IDs and receipts stay exact.
        owner.write_text(str(target / "runtime/runner.db") + "\n")
        with owner.open("rb") as f:
            os.fsync(f.fileno())
        database.execute("UPDATE volume_slots SET spec_json=? WHERE id=?", (json.dumps(new, separators=(",", ":")), new["id"]))
        bindings.append({"before": old, "after": new})
    database.commit()
    if logical_before != list(database.execute("SELECT request_json,status,job_id,receipt_json FROM operations ORDER BY id")):
        raise ValueError("relocation changed logical operation evidence")
    if database.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
        raise ValueError("relocated journal integrity failed")
    database.close()
    registry = rebind_artifact_journal(target / "runtime/artifacts", base / "source/runtime/runner.db", target / "runtime/runner.db")
    sync_tree(target / "runtime")
    config["volume_slots"] = target_slots
    config["paired_manifest_sha256"] = digest(backup / "manifest.json")
    write_json(base / "target-config.json", config)
    write_json(base / "relocation.json", {"physical_bindings": bindings, "artifact_journal_authority": registry, "logical_operations_unchanged": True, "source_database": config["source_database"], "target_database": config["target_database"], "production_worker_attached": False})
    print("Paired restore/physical relocation complete. Run TestRecoveryRestoredProcess using target-config.json in a new mapped namespace; never attach a production worker.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prepare", "source-config", "backup", "restore"))
    parser.add_argument("--id", required=True)
    parser.add_argument("--image")
    parser.add_argument("--docker-host")
    args = parser.parse_args()
    if os.geteuid() == 0:
        raise ValueError("run unprivileged as the checkout owner; only the printed helpers require host root")
    os.umask(0o077)
    {"prepare": prepare, "source-config": source_config, "backup": backup, "restore": restore}[args.action](args)


if __name__ == "__main__":
    main()
