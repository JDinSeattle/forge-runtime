#!/usr/bin/python3
"""Unprivileged tests: temporary files and mocks, no database, Docker, or mounts."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

HERE = Path(__file__).absolute().parent


def module(name, filename):
    spec = importlib.util.spec_from_file_location(name, HERE / filename)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


rehearse = module("rehearse_test_subject", "rehearse.py")
freeze = module("freeze_test_subject", "freeze-copy.py")


class RecoveryTests(unittest.TestCase):
    def test_prepare_has_private_scope_even_under_umask_022(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "rehearsals"
            args = SimpleNamespace(id="rfixture", image="python@sha256:" + "a" * 64, docker_host="unix:///tmp/fixture.sock")
            previous = os.umask(0o022)
            try:
                with patch.object(rehearse, "ROOT", root), patch.object(rehearse.subprocess, "run"):
                    rehearse.prepare(args)
                for label in ("source", "target"):
                    for child in ("", "scripts", "scripts/workspace-volumes", "var", "runtime"):
                        self.assertEqual((root / args.id / label / child).stat().st_mode & 0o777, 0o700)
            finally:
                os.umask(previous)

    def test_scope_never_accepts_absolute_traversal_or_shell_text(self):
        for value in ("../live", "/tmp/root", "a/../../x", "a;reboot", "abc\n", "", "ab", "a" * 37):
            with self.subTest(value=value), self.assertRaises(ValueError):
                rehearse.scope(value)
        self.assertEqual(rehearse.scope("r20260911_a"), rehearse.ROOT / "r20260911_a")

    def test_manifest_detects_tamper_extra_and_symlink(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw = root / "database.dump"
            raw.write_bytes(b"committed snapshot")
            manifest = {"schema_version": 1, "fixed_images": 4, "bytes_per_image": rehearse.BYTES,
                        "sha256": {"database.dump": rehearse.digest(raw)}}
            rehearse.write_json(root / "manifest.json", manifest)
            self.assertEqual(rehearse.verify_backup(root), manifest)
            raw.write_bytes(b"wrong snapshot")
            with self.assertRaisesRegex(ValueError, "digest mismatch"):
                rehearse.verify_backup(root)
            raw.write_bytes(b"committed snapshot")
            (root / "extra").write_bytes(b"unexpected")
            with self.assertRaisesRegex(ValueError, "file set"):
                rehearse.verify_backup(root)
            (root / "extra").unlink()
            (root / "symlink").symlink_to(raw)
            with self.assertRaisesRegex(ValueError, "nonregular"):
                rehearse.verify_backup(root)

    def test_copy_tree_never_traverses_symlink(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source"
            source.mkdir()
            (source / "link").symlink_to("/etc/passwd")
            with self.assertRaisesRegex(ValueError, "symlink"):
                rehearse.copy_tree(source, root / "target")

    def test_thaw_failure_still_attempts_every_mount_and_close(self):
        calls = []

        def thaw(fd, enable):
            calls.append((fd, enable))
            if fd == 12:
                raise OSError("injected thaw failure")

        with patch.object(freeze, "freeze", side_effect=thaw), patch.object(freeze.os, "close") as close:
            with self.assertRaisesRegex(RuntimeError, "attempted every thaw"):
                freeze.thaw_all([11, 12, 13], [11, 12, 13, 14])
            self.assertEqual(calls, [(13, False), (12, False), (11, False)])
            self.assertEqual([c.args[0] for c in close.call_args_list], [11, 12, 13, 14])

    def test_partial_freeze_only_thaws_known_frozen_set(self):
        with patch.object(freeze, "freeze") as thaw, patch.object(freeze.os, "close") as close:
            freeze.thaw_all([11], [11, 12])
            thaw.assert_called_once_with(11, False)
            self.assertEqual(close.call_count, 2)

    def test_private_path_rejects_writable_scope_and_symlinks(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            freeze.private_path(root, os.getuid())
            root.chmod(0o755)
            with self.assertRaisesRegex(ValueError, "identity"):
                freeze.private_path(root, os.getuid())
            root.chmod(0o700)
            (root / "link").symlink_to(root, target_is_directory=True)
            with self.assertRaisesRegex(ValueError, "identity"):
                freeze.private_path(root / "link", os.getuid())

    def test_pg_environment_requires_loopback_and_has_no_shell(self):
        with patch.dict(os.environ, {"FORGE_RECOVERY_ADMIN_DSN": "postgres://user:p%24%28literal%29@127.0.0.1:32773/forge?sslmode=disable"}):
            env = rehearse.pg_env("forge_recovery_t_demo")
            self.assertEqual(env["PGPASSWORD"], "p$(literal)")
            self.assertEqual(env["PGDATABASE"], "forge_recovery_t_demo")
        with patch.dict(os.environ, {"FORGE_RECOVERY_ADMIN_DSN": "postgres://user@production/forge"}):
            with self.assertRaisesRegex(ValueError, "loopback"):
                rehearse.pg_env("forge_recovery_t_demo")

    def test_registry_relocation_preserves_durable_id_and_source_copy(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            artifacts = root / "target-artifacts"
            artifacts.mkdir()
            source = root / "source.db"
            target = root / "target.db"
            identity = "a" * 64
            database = sqlite3.connect(target)
            database.execute("CREATE TABLE journal_identity(singleton INTEGER PRIMARY KEY,id TEXT)")
            database.execute("INSERT INTO journal_identity VALUES(1,?)", (identity,))
            database.commit()
            database.close()
            old = ".runner-journal-" + hashlib.sha256(str(source).encode()).hexdigest()
            marker = {"journal_id": identity, "journal_path": str(source)}
            (artifacts / old).write_text(json.dumps(marker))
            proof = rehearse.rebind_artifact_journal(artifacts, source, target)
            self.assertEqual(proof["journal_id"], identity)
            self.assertFalse((artifacts / old).exists())
            new = ".runner-journal-" + hashlib.sha256(str(target).encode()).hexdigest()
            self.assertEqual(json.loads((artifacts / new).read_text()), {"journal_path": str(target), "journal_id": identity})
            # A second run refuses to rebind a marker for another source.
            with self.assertRaisesRegex(ValueError, "exact sole source"):
                rehearse.rebind_artifact_journal(artifacts, source, target)

    def test_registry_refuses_wrong_durable_identity(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source, target = root / "source.db", root / "target.db"
            database = sqlite3.connect(target)
            database.execute("CREATE TABLE journal_identity(singleton INTEGER PRIMARY KEY,id TEXT)")
            database.execute("INSERT INTO journal_identity VALUES(1,?)", ("a" * 64,))
            database.commit()
            database.close()
            old = root / (".runner-journal-" + hashlib.sha256(str(source).encode()).hexdigest())
            old.write_text(json.dumps({"journal_id": "b" * 64, "journal_path": str(source)}))
            with self.assertRaisesRegex(ValueError, "identity mismatch"):
                rehearse.rebind_artifact_journal(root, source, target)
            self.assertTrue(old.exists())


if __name__ == "__main__":
    unittest.main()
