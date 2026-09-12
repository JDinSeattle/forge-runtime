import importlib.util
import json
import os
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("sigterm_prepare", Path(__file__).with_name("prepare.py"))
prepare = importlib.util.module_from_spec(spec)
spec.loader.exec_module(prepare)


class PreparationScopeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="forge-lifecycle-plan-")
        self.addCleanup(self.temp.cleanup)
        self.repo = Path(self.temp.name) / "repo"
        self.repo.mkdir(mode=0o700)
        (self.repo / "var").mkdir(mode=0o700)
        helpers = self.repo / "scripts/workspace-volumes"
        helpers.mkdir(parents=True)
        for name in prepare.HELPERS:
            (helpers / name).write_text("# inert unit-fixture helper: " + name + "\n")
        for name, value in (("REPO", self.repo), ("ROOT", self.repo / "var/lifecycle-rehearsals")):
            item = patch.object(prepare, name, value)
            item.start()
            self.addCleanup(item.stop)
        item = patch.object(prepare.os, "statvfs", return_value=types.SimpleNamespace(f_bavail=1 << 30, f_frsize=4096))
        item.start()
        self.addCleanup(item.stop)

    def layout(self):
        return prepare.layout("lr_unit", "unix:///run/user/1000/fixture.sock")

    def test_exact_private_copy_and_fixed_capacity(self):
        before = os.umask(0o022)
        try:
            base, value = self.layout()
        finally:
            os.umask(before)
        self.assertEqual(value["volume_count"], 4)
        self.assertEqual(value["bytes_per_volume"], 256 << 20)
        self.assertEqual(prepare.read_preparation("lr_unit"), (base, value))
        for path in base.rglob("*"):
            self.assertEqual(path.stat().st_mode & 0o077, 0)
        for name in prepare.HELPERS:
            self.assertEqual((base / "pool-root/scripts/workspace-volumes" / name).read_bytes(),
                             (self.repo / "scripts/workspace-volumes" / name).read_bytes())

    def test_reuse_never_overwrites_prior_descriptor(self):
        base, _ = self.layout()
        before = (base / "preparation.json").read_bytes()
        with self.assertRaises(FileExistsError):
            self.layout()
        self.assertEqual(before, (base / "preparation.json").read_bytes())

    def test_bad_ids_and_socket_rejected_before_writes(self):
        for value in ("../lr_escape", "lr_test/child", "r20260911_a", "lr", "lr_" + "x" * 31):
            with self.subTest(value=value), self.assertRaises(ValueError):
                prepare.layout(value, "unix:///tmp/test.sock")
        for host in ("tcp://127.0.0.1:2375", "unix:///tmp/../test", "unix:///tmp/socket?x", "unix:///tmp/socket\n"):
            with self.subTest(host=host), self.assertRaises(ValueError):
                prepare.layout("lr_unit", host)
        self.assertFalse(prepare.ROOT.exists())

    def test_symlink_scope_cannot_reach_other_tree(self):
        outside = Path(self.temp.name) / "outside"
        outside.mkdir(mode=0o700)
        prepare.ROOT.symlink_to(outside, target_is_directory=True)
        with self.assertRaises(ValueError):
            self.layout()
        self.assertEqual(list(outside.iterdir()), [])

    def test_helper_mutation_or_symlink_is_not_a_valid_preparation(self):
        base, _ = self.layout()
        helper = base / "pool-root/scripts/workspace-volumes/mount.py"
        helper.write_text("changed")
        with self.assertRaisesRegex(ValueError, "helper changed"):
            prepare.read_preparation("lr_unit")
        helper.unlink()
        helper.symlink_to(self.repo / "scripts/workspace-volumes/mount.py")
        with self.assertRaises(ValueError):
            prepare.read_preparation("lr_unit")

    def test_descriptor_cannot_substitute_original_pool(self):
        base, value = self.layout()
        value["pool_root"] = str(self.repo)
        (base / "preparation.json").write_text(json.dumps(value))
        with self.assertRaisesRegex(ValueError, "scope or policy"):
            prepare.read_preparation("lr_unit")

    def test_conflicting_file_is_retained(self):
        base, _ = self.layout()
        target = base / "keep"
        prepare.create_file(target, b"original")
        with self.assertRaises(FileExistsError):
            prepare.create_file(target, b"replacement")
        self.assertEqual(target.read_bytes(), b"original")

    def test_insufficient_capacity_does_not_create_scope(self):
        with patch.object(prepare.os, "statvfs", return_value=types.SimpleNamespace(f_bavail=1, f_frsize=4096)):
            with self.assertRaisesRegex(ValueError, "insufficient space"):
                self.layout()
        self.assertFalse(prepare.ROOT.exists())


if __name__ == "__main__":
    unittest.main()
