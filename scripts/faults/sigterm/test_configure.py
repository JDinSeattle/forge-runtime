from contextlib import nullcontext
import copy
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import Mock, patch

spec = importlib.util.spec_from_file_location("sigterm_configure", Path(__file__).with_name("configure.py"))
configure = importlib.util.module_from_spec(spec)
spec.loader.exec_module(configure)
volume_config = configure.load_module("sigterm_test_volume_config", configure.p.REPO / "cmd/forge-runner/configure-local.py")


class LiveLoopEvidenceTests(unittest.TestCase):
    """Synthetic sysfs/tool records only; never access a host loop or mount."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="forge-lifecycle-loop-")
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name) / "fixture"
        self.pool = self.base / "pool-root"
        self.mount_path = self.pool / "var/workspace-storage/mounts/slot-001"
        self.mount_path.mkdir(parents=True, mode=0o700)
        self.slot = {"id": "slot-001", "image": "slot-001.ext4", "mount": "slot-001",
                     "device": os.makedev(8, 1), "inode": 101}
        self.volume = {"id": "slot-001", "device": "7:10", "mount_path": str(self.mount_path),
                       "image_path": str(self.pool / "var/workspace-storage/images/slot-001.ext4")}
        self.row = {"name": "/dev/loop10", "maj:min": "7:10", "back-ino": None,
                    "back-maj:min": None, "offset": 0, "sizelimit": configure.p.IMAGE_BYTES, "ro": False}
        self.sysroot = Path(self.temp.name) / "sysfs"
        (self.sysroot / "7:10/loop").mkdir(parents=True)
        self.fields = {"loop/backing_file": self.volume["image_path"], "loop/offset": "0",
                       "loop/sizelimit": str(configure.p.IMAGE_BYTES), "size": "524288", "ro": "0"}
        for name, value in self.fields.items():
            self.write_field(name, value)
        item = patch.object(configure, "SYS_BLOCK", self.sysroot)
        item.start()
        self.addCleanup(item.stop)

    def write_field(self, name, value):
        (self.sysroot / "7:10" / name).write_text(value + "\n")

    def verify(self, row=None):
        return configure.verify_live_loop(self.row if row is None else row, self.slot, self.volume)

    def test_null_identity_requires_complete_live_evidence(self):
        before = copy.deepcopy(self.row)
        observed = self.verify()
        self.assertEqual(observed["backing_file"], self.volume["image_path"])
        self.assertEqual(observed["size"] * 512, configure.p.IMAGE_BYTES)
        self.assertEqual(observed["ro"], 0)
        self.assertFalse(observed["losetup_backing_inode_available"])
        self.assertFalse(observed["losetup_backing_device_available"])
        self.assertEqual(self.row, before, "must not invent losetup identity fields")

    def test_available_and_partially_available_identities_must_match(self):
        for supplied in ({"back-ino": 101, "back-maj:min": "8:1"}, {"back-ino": "101"},
                         {"back-maj:min": "8:1"}, {}):
            row = {key: value for key, value in self.row.items() if not key.startswith("back-")}
            row.update(supplied)
            with self.subTest(supplied=supplied):
                observed = self.verify(row)
                self.assertEqual(observed["losetup_backing_inode_available"], "back-ino" in supplied)
                self.assertEqual(observed["losetup_backing_device_available"], "back-maj:min" in supplied)

    def test_explicit_tool_mismatches_cannot_fall_back_to_valid_sysfs(self):
        for key, value in (("back-ino", 102), ("back-ino", ""), ("back-ino", False),
                           ("back-ino", 101.0), ("back-maj:min", "8:2"), ("back-maj:min", ""),
                           ("name", "/dev/loop11"), ("maj:min", "7:11"),
                           ("offset", 512), ("offset", None), ("sizelimit", 0),
                           ("sizelimit", None), ("ro", True), ("ro", None), ("ro", 0.0)):
            with self.subTest(key=key, value=value):
                with self.assertRaises(ValueError):
                    self.verify({**self.row, key: value})

    def test_every_sysfs_field_is_required_even_with_full_tool_identity(self):
        row = {**self.row, "back-ino": 101, "back-maj:min": "8:1"}
        for name, value in self.fields.items():
            with self.subTest(name=name):
                path = self.sysroot / "7:10" / name
                path.unlink()
                with self.assertRaises(FileNotFoundError):
                    self.verify(row)
                self.write_field(name, value)

    def test_wrong_live_backing_path_is_not_normalized_into_a_match(self):
        for value in ("/another/pool/slot-001.ext4", self.volume["image_path"] + " (deleted)",
                      self.volume["image_path"].replace("/images/", "/images/../images/"),
                      "relative/slot-001.ext4", ""):
            with self.subTest(value=value):
                self.write_field("loop/backing_file", value)
                with self.assertRaisesRegex(ValueError, "backing path"):
                    self.verify()

    def test_sysfs_geometry_and_read_write_evidence_fail_closed(self):
        for name, original in self.fields.items():
            if name == "loop/backing_file":
                continue
            for value in ("", "-1", "null", "1.0", "0\n0", str(int(original) + 1)):
                with self.subTest(name=name, value=value):
                    self.write_field(name, value)
                    with self.assertRaises(ValueError):
                        self.verify()
            self.write_field(name, original)

    def actual_fixture(self):
        # Only the mounted-filesystem/manifest observations are mocked. The
        # real sysfs reader and mount-options validator run on local fixtures.
        # This is guard wiring coverage, not mounted ext4 acceptance.
        manifest = {"slots": [self.slot]}
        state = {"slots": {"slot-001": {"mount_id": 42}}}
        tree = types.SimpleNamespace(uid=os.getuid(), locked=lambda: nullcontext(),
                                     read_json=Mock(side_effect=[manifest, state]))
        mount = {"mount_id": 42, "type": "ext4", "root": "/", "device": "7:10",
                 "options": ["rw", "nosuid", "nodev"]}
        common = types.SimpleNamespace(Tree=lambda _: nullcontext(tree), MANIFEST="manifest.json",
                                       MOUNT_STATE="mount-state.json", loop_rows=Mock(return_value=[self.row]),
                                       exact_mount=Mock(return_value=mount), verify_mount=volume_config.common.verify_mount)
        config = types.SimpleNamespace(common=common, build_slots=Mock(return_value=[self.volume]),
                                       check_images=Mock())
        return config, tree, mount

    def run_actual(self, config, filesystem=None):
        real_stat = os.stat

        def local_mount_stat(path, *args, **kwargs):
            result = real_stat(path, *args, **kwargs)
            if Path(path) == self.mount_path:
                fields = list(result)
                fields[2] = os.makedev(7, 10)
                return os.stat_result(fields)
            return result

        filesystem = filesystem or types.SimpleNamespace(f_blocks=55000, f_frsize=4096, f_files=65536)
        with patch.object(configure.os, "stat", side_effect=local_mount_stat), patch.object(configure.os, "statvfs", return_value=filesystem):
            return configure.actual_slots(self.base, {}, config)

    def test_actual_slots_keeps_root_state_and_image_checks_on_both_sides(self):
        config, tree, _ = self.actual_fixture()
        slots, observed = self.run_actual(config)
        self.assertEqual(slots, [self.volume])
        self.assertEqual(tree.read_json.call_args_list[0].kwargs, {"owner": os.getuid()})
        self.assertEqual(tree.read_json.call_args_list[1].kwargs, {"owner": 0})
        self.assertEqual(config.check_images.call_count, 2)
        self.assertEqual(observed[0]["loop"]["size"], 524288)

    def test_valid_sysfs_cannot_bypass_image_fd_rejection(self):
        for errors in ([ValueError("image FD identity/protection changed")],
                       [None, ValueError("image FD identity/protection changed")]):
            config, _, _ = self.actual_fixture()
            config.check_images.side_effect = errors
            with self.subTest(at_check=len(errors)):
                with self.assertRaisesRegex(ValueError, "image FD"):
                    self.run_actual(config)

    def test_mount_identity_and_flags_are_still_required(self):
        for changed in ({"mount_id": 43}, {"options": ["rw", "nosuid"]},
                        {"type": "xfs"}, {"root": "/subdir"}, {"device": "7:11"}):
            config, _, mount = self.actual_fixture()
            mount.update(changed)
            with self.subTest(changed=changed):
                with self.assertRaises((ValueError, volume_config.common.UnsafeState)):
                    self.run_actual(config)

    def test_used_or_writable_slot_and_bad_capacity_are_still_rejected(self):
        config, _, _ = self.actual_fixture()
        marker = self.mount_path / "retained-workspace"
        marker.write_text("preserve unknown ownership")
        with self.assertRaisesRegex(ValueError, "not empty"):
            self.run_actual(config)
        self.assertEqual(marker.read_text(), "preserve unknown ownership")
        marker.unlink()
        self.mount_path.chmod(0o777)
        config, _, _ = self.actual_fixture()
        with self.assertRaisesRegex(ValueError, "protected"):
            self.run_actual(config)
        self.mount_path.chmod(0o700)
        for changed in ({"f_blocks": 65537}, {"f_files": 65537}, {"f_files": 0}):
            config, _, _ = self.actual_fixture()
            fs = types.SimpleNamespace(**({"f_blocks": 55000, "f_frsize": 4096, "f_files": 65536} | changed))
            with self.subTest(changed=changed):
                with self.assertRaisesRegex(ValueError, "capacity"):
                    self.run_actual(config, fs)


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
