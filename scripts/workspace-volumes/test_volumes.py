"""Validation tests: small temporary files, no mounts/loops/daemons or mkfs."""
from contextlib import contextmanager
import copy
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import common
import volumes
import mount as privileged


class StorageValidation(unittest.TestCase):
    def setUp(self):
        if os.getuid() == 0:
            self.skipTest("run validation tests as unprivileged project owner")
        self.temp = tempfile.TemporaryDirectory(prefix="forge-volume-validation-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "project"
        self.root.mkdir(mode=0o700)
        self.tree = common.Tree(self.root, create=True)
        self.addCleanup(self.tree.close)
        self.manifest = common.new_manifest(self.tree)

    def test_fixed_manifest_rejects_scope_and_path_changes(self):
        common.validate_manifest(self.manifest, self.tree)
        for key, value in (("project_root", "/tmp/other"), ("volume_count", 5),
                           ("bytes_per_volume", common.VOLUME_BYTES * 2),
                           ("runner_uid", self.tree.uid + 1)):
            with self.subTest(key=key):
                changed = copy.deepcopy(self.manifest)
                changed[key] = value
                with self.assertRaises(common.UnsafeState):
                    common.validate_manifest(changed, self.tree)
        for value in ("../foreign", "/etc/passwd", "slot-001.ext4;touch /tmp/no"):
            changed = copy.deepcopy(self.manifest)
            changed["slots"][0]["image"] = value
            with self.assertRaises(common.UnsafeState):
                common.validate_manifest(changed, self.tree)

    def test_unready_manifest_cannot_reach_privileged_stage(self):
        with self.assertRaises(common.UnsafeState):
            common.validate_manifest(self.manifest, self.tree, require_ready=True)

    def test_symlink_ancestor_and_leaf_are_rejected(self):
        alias = Path(self.temp.name) / "alias"
        alias.symlink_to(self.root, target_is_directory=True)
        with self.assertRaises(OSError):
            common.open_absolute_directory(alias)
        (self.tree.storage_path / "manifest.json").symlink_to("/etc/passwd")
        with self.assertRaises(OSError):
            self.tree.read_json(common.MANIFEST)
        (self.tree.storage_path / "images").symlink_to(Path(self.temp.name), target_is_directory=True)
        with self.assertRaises(OSError):
            self.tree.directory("images")

    def test_json_metadata_rejects_hardlink_and_bad_owner(self):
        self.tree.write_json(common.MANIFEST, self.manifest)
        os.link(self.tree.storage_path / common.MANIFEST, self.tree.storage_path / "alias.json")
        with self.assertRaises(common.UnsafeState):
            self.tree.read_json(common.MANIFEST)

    def test_prepare_never_formats_existing_unrecorded_file(self):
        images = self.tree.directory("images", create=True)
        os.close(images)
        slot = self.manifest["slots"][0]
        target = self.tree.storage_path / "images" / slot["image"]
        target.write_bytes(b"preserve this evidence")
        with mock.patch.object(volumes, "run_tool") as tool:
            with self.assertRaises(common.UnsafeState):
                volumes.prepare_slot(self.tree, self.manifest, slot)
            tool.assert_not_called()
        self.assertEqual(target.read_bytes(), b"preserve this evidence")

    def test_exclusive_prepare_then_repeat_only_inspects(self):
        # Exercise FD pinning, allocation and idempotency with a 4KiB file.
        # Fake formatting is explicit here and is not evidence of real ext4.
        with mock.patch.object(common, "VOLUME_BYTES", 4096), mock.patch.object(volumes, "VOLUME_BYTES", 4096):
            manifest = common.new_manifest(self.tree)
            slot = manifest["slots"][0]
            seen = []

            def fake_prepare(name, args, *, pass_fds=(), **_):
                seen.append(name)
                self.assertEqual(len(pass_fds), 1)
                fd = pass_fds[0]
                self.assertEqual(args[-1], f"/proc/self/fd/{fd}")
                if name == "fallocate":
                    os.posix_fallocate(fd, 0, 4096)
                elif name != "mkfs":
                    self.fail("unexpected preparation command")
                return ""

            blkid = f"TYPE=ext4\nUUID={slot['filesystem_uuid']}\n"
            with mock.patch.object(volumes, "run_tool", side_effect=fake_prepare), mock.patch.object(common, "run_tool", return_value=blkid):
                self.assertEqual(volumes.prepare_slot(self.tree, manifest, slot), "created")
                self.assertEqual(volumes.prepare_slot(self.tree, manifest, slot), "verified_existing")
            self.assertEqual(seen, ["fallocate", "mkfs", "fallocate"])
            self.assertEqual(slot["state"], "ready")
            self.assertGreater(slot["inode"], 0)

    def test_image_replacement_is_detected_before_filesystem_probe(self):
        path = self.tree.storage_path / "image"
        fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            os.posix_fallocate(fd, 0, 4096)
            slot = self.manifest["slots"][0]
            slot.update(device=os.fstat(fd).st_dev, inode=os.fstat(fd).st_ino + 1)
            with mock.patch.object(common, "VOLUME_BYTES", 4096), mock.patch.object(common, "run_tool") as tool:
                with self.assertRaises(common.UnsafeState):
                    common.inspect_image_fd(fd, slot, self.tree.uid)
                tool.assert_not_called()
        finally:
            os.close(fd)

    def test_mountinfo_handles_spaces_and_rejects_stacking(self):
        text = "43 25 7:9 / /home/user/mini\\040claude/mount rw,nosuid,nodev - ext4 /dev/loop9 rw\n"
        rows = common.mount_records(text)
        self.assertEqual(rows[0]["target"], "/home/user/mini claude/mount")
        self.assertEqual(common.exact_mount(Path(rows[0]["target"]), rows), rows[0])
        with self.assertRaises(common.UnsafeState):
            common.exact_mount(Path(rows[0]["target"]), rows + rows)
        with self.assertRaises(common.UnsafeState):
            common.mount_records("malformed")

    def test_loop_identity_and_options_fail_closed(self):
        slot = self.manifest["slots"][0]
        slot.update(device=os.makedev(259, 3), inode=1234)
        loop = {"name": "/dev/loop9", "back-ino": 1234, "back-maj:min": "259:3",
                "offset": 0, "sizelimit": common.VOLUME_BYTES, "ro": False, "maj:min": "7:9"}
        common.verify_loop(loop, slot)
        for key, value in (("back-ino", 999), ("offset", 4096), ("sizelimit", 0),
                           ("ro", True), ("name", "/dev/nvme1n1p3")):
            with self.subTest(key=key), self.assertRaises(common.UnsafeState):
                common.verify_loop(dict(loop, **{key: value}), slot)
        row = {"type": "ext4", "root": "/", "device": "7:9", "options": ["rw", "nosuid", "nodev"]}
        common.verify_mount(row, loop)
        with self.assertRaises(common.UnsafeState):
            common.verify_mount(dict(row, options=["rw"]), loop)

    def test_duplicate_loop_mappings_are_not_adopted(self):
        slot = self.manifest["slots"][0]
        slot.update(device=os.makedev(259, 3), inode=1234)
        row = {"back-maj:min": "259:3", "back-ino": 1234}
        with mock.patch.object(privileged, "loop_rows", return_value=[row, row]):
            with self.assertRaises(common.UnsafeState):
                privileged.associated_loop(slot)

    def test_unmount_refuses_foreign_state_without_any_command(self):
        slot = self.manifest["slots"][0]

        @contextmanager
        def fake_image(*_, **__):
            yield 42

        with mock.patch.object(privileged, "image_fd", fake_image), mock.patch.object(privileged, "mounted_slot", return_value=({"name": "/dev/loop9", "maj:min": "7:9"}, {})), mock.patch.object(privileged, "run_tool") as tool:
            with self.assertRaises(common.UnsafeState):
                privileged.unmount_slot(self.tree, self.manifest, {"slots": {}}, slot)
            tool.assert_not_called()

    def test_park_preserves_data_and_keeps_namespace_guard(self):
        slot = self.manifest["slots"][0]
        target = self.tree.storage_path / "mounts" / slot["mount"]
        target.mkdir(parents=True, mode=0o700)
        target.parent.chmod(0o700)
        retained = target / "unknown-workspace"
        retained.write_text("preserve unresolved evidence")
        loop = {"name": "/dev/loop9", "maj:min": "7:9"}
        row = {"device": "7:9", "target": str(target), "mount_id": 42}
        state = {"slots": {slot["id"]: {"loop": "/dev/loop9", "device": "7:9"}}}

        @contextmanager
        def fake_image(*_, **__):
            yield 42

        for scenario in ("contents", "busy", "retained_namespace", "propagated"):
            with self.subTest(scenario=scenario):
                state = {"slots": {slot["id"]: {"loop": "/dev/loop9", "device": "7:9"}}}
                events = []

                def command(name, args):
                    events.append(name)
                    if scenario == "busy" and name == "umount":
                        raise common.UnsafeState("target is busy")

                def remaining_users(device):
                    self.assertEqual(device, "7:9")
                    self.assertEqual(events, ["umount"])
                    return [123] if scenario == "retained_namespace" else []

                with mock.patch.object(privileged, "image_fd", fake_image), mock.patch.object(privileged, "mounted_slot", return_value=(loop, row)), mock.patch.object(privileged, "other_namespace_users", side_effect=remaining_users) as users, mock.patch.object(privileged, "mount_records", return_value=[row]), mock.patch.object(privileged, "run_tool", side_effect=command) as tool, mock.patch.object(privileged, "save_state") as save, mock.patch.object(privileged, "exact_mount", side_effect=[row, None]), mock.patch.object(privileged, "associated_loop", side_effect=[loop, None]):
                    if scenario == "propagated":
                        privileged.unmount_slot(self.tree, self.manifest, state, slot, preserve_contents=True)
                        self.assertEqual(tool.call_args_list, [mock.call("umount", ["--no-canonicalize", str(target)]), mock.call("losetup", ["--detach", "/dev/loop9"])])
                        self.assertNotIn(slot["id"], state["slots"])
                        save.assert_called_once()
                    else:
                        with self.assertRaises(common.UnsafeState):
                            privileged.unmount_slot(self.tree, self.manifest, state, slot, preserve_contents=scenario != "contents")
                        self.assertIn(slot["id"], state["slots"])
                        save.assert_not_called()
                        if scenario == "contents":
                            tool.assert_not_called()
                        else:
                            tool.assert_called_once_with("umount", ["--no-canonicalize", str(target)])
                        if scenario != "retained_namespace":
                            users.assert_not_called()
                self.assertEqual(retained.read_text(), "preserve unresolved evidence")

    def test_live_capacity_rejects_host_directory_before_reading_sysfs(self):
        mounts = self.tree.directory("mounts", create=True)
        slot = self.manifest["slots"][0]
        os.mkdir(slot["mount"], 0o700, dir_fd=mounts)
        os.close(mounts)
        with self.assertRaises(common.UnsafeState):
            privileged.capacity_evidence(self.tree, slot, {"maj:min": "7:999"})

    def test_unmount_retains_state_when_detach_is_not_confirmed(self):
        slot = self.manifest["slots"][0]
        loop = {"name": "/dev/loop9", "maj:min": "7:9"}
        state = {"slots": {slot["id"]: {"loop": "/dev/loop9", "device": "7:9"}}}

        @contextmanager
        def fake_image(*_, **__):
            yield 42

        with mock.patch.object(privileged, "image_fd", fake_image), mock.patch.object(privileged, "mounted_slot", return_value=(loop, None)), mock.patch.object(privileged, "other_namespace_users", return_value=[]), mock.patch.object(privileged, "mount_records", return_value=[]), mock.patch.object(privileged, "associated_loop", return_value=loop), mock.patch.object(privileged, "run_tool") as tool:
            with self.assertRaises(common.UnsafeState):
                privileged.unmount_slot(self.tree, self.manifest, state, slot)
            tool.assert_called_once_with("losetup", ["--detach", "/dev/loop9"])
        self.assertIn(slot["id"], state["slots"])

    def test_commands_have_fixed_binary_clean_environment_and_no_shell(self):
        completed = mock.Mock(returncode=0, stdout="{}", stderr="")
        with mock.patch.object(common.subprocess, "run", return_value=completed) as process:
            common.run_tool("blkid", ["-p", "/proc/self/fd/9"], pass_fds=(9,))
        args, kwargs = process.call_args
        self.assertEqual(args[0][0], "/usr/sbin/blkid")
        self.assertIs(kwargs["shell"], False)
        self.assertEqual(kwargs["pass_fds"], (9,))
        self.assertEqual(set(kwargs["env"]), {"PATH", "LC_ALL"})


if __name__ == "__main__":
    unittest.main()
