import importlib.util
import json
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("sigterm_configure", Path(__file__).with_name("configure.py"))
configure = importlib.util.module_from_spec(spec)
spec.loader.exec_module(configure)


class ConfigurationBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="forge-lifecycle-config-")
        self.addCleanup(self.temp.cleanup)
        self.repo = Path(self.temp.name) / "repo"
        self.repo.mkdir(mode=0o700)
        (self.repo / "var").mkdir(mode=0o700)
        helpers = self.repo / "scripts/workspace-volumes"
        helpers.mkdir(parents=True)
        for name in configure.p.HELPERS:
            (helpers / name).write_text("# inert fixture\n")
        for name, value in (("REPO", self.repo), ("ROOT", self.repo / "var/lifecycle-rehearsals")):
            item = patch.object(configure.p, name, value)
            item.start()
            self.addCleanup(item.stop)
        with patch.object(configure.p.os, "statvfs", return_value=types.SimpleNamespace(f_bavail=1 << 30, f_frsize=4096)):
            self.base, _ = configure.p.layout("lr_unit", "unix:///run/user/1000/fixture.sock")
        for name in ("forge-runner", "forge-worker", "application-faults.test"):
            path = self.base / "bin" / name
            path.write_bytes(b"inert unit-test input, never executed")
            path.chmod(0o700)

    def test_missing_mount_cannot_create_runtime_key_or_acceptance(self):
        with patch.object(configure, "load_module", return_value=None), patch.object(configure, "actual_slots", side_effect=FileNotFoundError("mount-state missing")):
            with self.assertRaises(FileNotFoundError):
                configure.configure("lr_unit")
        self.assertFalse((self.base / "runtime").exists())
        self.assertFalse((self.base / "acceptance.json").exists())

    def test_partial_runtime_is_never_reinitialized(self):
        runtime = self.base / "runtime"
        runtime.mkdir(mode=0o700)
        (runtime / "journal.sqlite").write_bytes(b"retain uncertain state")
        with self.assertRaisesRegex(ValueError, "never replace"):
            configure.configure("lr_unit")
        self.assertEqual((runtime / "journal.sqlite").read_bytes(), b"retain uncertain state")
        self.assertFalse((runtime / "runner.key").exists())

    def test_binary_symlink_is_rejected_before_runtime_creation(self):
        original = self.base / "bin/forge-runner"
        original.unlink()
        original.symlink_to(self.base / "bin/forge-worker")
        with self.assertRaises(ValueError):
            configure.configure("lr_unit")
        self.assertFalse((self.base / "runtime").exists())

    def test_rendering_keeps_strict_backend_and_private_scoped_material(self):
        # The mocked result tests configuration rendering only. It is not a
        # volume verifier or an input to a runner/container execution.
        slots = [{"id": f"slot-{i:03}", "mount_path": str(self.base / "pool-root" / str(i))} for i in range(1, 5)]
        with patch.object(configure, "load_module", return_value=None), patch.object(configure, "actual_slots", return_value=(slots, [])):
            result = configure.configure("lr_unit")
        self.assertTrue(result["configured"])
        self.assertFalse(result["services_started"])
        runtime = self.base / "runtime"
        config = json.loads((runtime / "runner.json").read_text())
        self.assertFalse(config["allow_test_backend"])
        self.assertEqual(config["volume_slots"], slots)
        self.assertEqual(config["logs"]["run_bytes"], 16 << 20)
        self.assertEqual(config["root_dir"], str(runtime / "engine"))
        self.assertEqual(config["journal_path"], str(runtime / "journal.sqlite"))
        observations = json.loads((runtime / "configuration-observations.json").read_text())
        for name, field, value in (("byteguard", "run_bytes", 2 << 20), ("countguard", "max_operations", 2)):
            path = runtime / ("runner-" + name + ".json")
            variant = json.loads(path.read_text())
            self.assertEqual(variant["logs"][field], value)
            variant["logs"][field] = config["logs"][field]
            self.assertEqual(variant, config, "guard variants must change only their one budget")
            self.assertEqual(observations["runner_config_variants"][name]["sha256"], configure.p.digest(path))
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        self.assertEqual((runtime / "source/app.py").read_text(), configure.SOURCE)
        self.assertEqual(len((runtime / "runner.key").read_bytes()), 32)
        self.assertEqual((runtime / "runner.key").stat().st_mode & 0o777, 0o600)
        self.assertFalse((runtime / "journal.sqlite").exists())
        self.assertFalse((runtime / "engine").exists())
        acceptance = json.loads((self.base / "acceptance.json").read_text())
        self.assertEqual(acceptance["purpose"], "worker-runner-sigterm-v1")
        self.assertEqual(acceptance["test_sha256"], configure.p.digest(self.base / "bin/application-faults.test"))
        with self.assertRaises(ValueError):
            configure.configure("lr_unit")


if __name__ == "__main__":
    unittest.main()
