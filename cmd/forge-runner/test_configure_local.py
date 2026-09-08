"""Configuration boundary tests; no image allocation, mount or daemon calls."""
import copy
import importlib.util
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location("forge_configure_local", Path(__file__).with_name("configure-local.py"))
configure = importlib.util.module_from_spec(spec)
spec.loader.exec_module(configure)


class ConfigurationBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.tree = SimpleNamespace(root=Path("/tmp/forge-config-test"), uid=1000, gid=1000)
        self.manifest = configure.common.new_manifest(self.tree)
        for index, slot in enumerate(self.manifest["slots"], 1):
            slot.update(state="ready", device=123, inode=100 + index, allocated_bytes=configure.common.VOLUME_BYTES)
        self.state = {"schema_version": 1,
                      "manifest_sha256": configure.common.manifest_digest(self.manifest),
                      "slots": {slot["id"]: {"phase": "mounted", "device": f"7:{i}", "loop": f"/dev/loop{i}",
                                            "mount_id": i + 1000, "mountpoint_device": 123, "mountpoint_inode": i + 2000}
                                for i, slot in enumerate(self.manifest["slots"], 1)}}
        self.image = "python@sha256:" + "a" * 64
        self.current = {"root_dir": str(self.tree.root / "var/local/runner"),
                        "journal_path": str(self.tree.root / "var/local/runner.db"),
                        "signing_key_file": "/private/path-only-not-read",
                        "sources": {"fixture": "/trusted/source"},
                        "profiles": {"python": {"id": "python", "user": "1000:1000", "image": "python:tag",
                                                "workspace_quota_bytes": configure.common.VOLUME_BYTES}}}

    def slots(self):
        return configure.build_slots(self.manifest, self.state, self.tree)

    def test_preserves_control_paths_and_replaces_only_explicit_config(self):
        original = copy.deepcopy(self.current)
        result = configure.desired_config(self.current, self.slots(), self.image, "unix:///run/user/1000/forge.sock", self.tree.root)
        self.assertEqual(self.current, original)
        self.assertEqual(result["signing_key_file"], original["signing_key_file"])
        self.assertEqual(result["sources"], original["sources"])
        self.assertEqual(result["profiles"]["python"]["image"], self.image)
        self.assertEqual(len(result["volume_slots"]), 4)

    def test_rejects_changed_scope_even_when_mount_digest_recomputed(self):
        for key, value in (("project_root", "/other/repository"), ("volume_count", 5), ("bytes_per_volume", 1)):
            with self.subTest(key=key):
                manifest = copy.deepcopy(self.manifest); manifest[key] = value
                state = copy.deepcopy(self.state); state["manifest_sha256"] = configure.common.manifest_digest(manifest)
                with self.assertRaises(configure.common.UnsafeState):
                    configure.build_slots(manifest, state, self.tree)

    def test_rejects_partial_stale_or_aliased_mount_records(self):
        for mutation in (
                lambda s: s.update(manifest_sha256="0" * 64),
                lambda s: s["slots"].pop("slot-004"),
                lambda s: s["slots"]["slot-001"].update(phase="attached"),
                lambda s: s["slots"]["slot-001"].update(loop="/dev/loop2"),
                lambda s: s["slots"]["slot-002"].update(device="7:1", loop="/dev/loop1")):
            state = copy.deepcopy(self.state); mutation(state)
            with self.assertRaises(configure.common.UnsafeState):
                configure.build_slots(self.manifest, state, self.tree)

    def test_rejects_outside_image_and_duplicate_image_identity(self):
        for change in ({"image": "../../runner.key"}, {"inode": 102}):
            manifest = copy.deepcopy(self.manifest); manifest["slots"][0].update(change)
            state = copy.deepcopy(self.state); state["manifest_sha256"] = configure.common.manifest_digest(manifest)
            with self.assertRaises(configure.common.UnsafeState):
                configure.build_slots(manifest, state, self.tree)

    def test_requires_explicit_digest_unix_socket_and_nonroot_real_profile(self):
        for image, host in (("python:tag", "unix:///run/forge.sock"), (self.image, "tcp://localhost:2375"),
                            (self.image, "unix:///run/../secret.sock")):
            with self.assertRaises(configure.common.UnsafeState):
                configure.desired_config(self.current, self.slots(), image, host, self.tree.root)
        for changed in ({"allow_test_backend": True}, {"root_dir": "/elsewhere"}):
            current = copy.deepcopy(self.current); current.update(changed)
            with self.assertRaises(configure.common.UnsafeState):
                configure.desired_config(current, self.slots(), self.image, "unix:///run/forge.sock", self.tree.root)

    def test_python_image_does_not_overwrite_pinned_go_profile(self):
        current = copy.deepcopy(self.current)
        go_image = "golang@sha256:" + "b" * 64
        current["profiles"]["go-ceil-div"] = {"id": "go-ceil-div", "user": "1000:1000", "image": go_image,
                                               "workspace_quota_bytes": configure.common.VOLUME_BYTES}
        result = configure.desired_config(current, self.slots(), self.image, "unix:///run/forge.sock", self.tree.root)
        self.assertEqual(result["profiles"]["python"]["image"], self.image)
        self.assertEqual(result["profiles"]["go-ceil-div"]["image"], go_image)

    def test_fresh_mixed_profiles_require_their_own_explicit_digests(self):
        current = copy.deepcopy(self.current)
        current["profiles"]["go-ceil-div"] = {"id": "go-ceil-div", "user": "1000:1000", "image": "golang:1.26-bookworm",
                                               "workspace_quota_bytes": configure.common.VOLUME_BYTES}
        with self.assertRaises(configure.common.UnsafeState):
            configure.desired_config(current, self.slots(), self.image, "unix:///run/forge.sock", self.tree.root)
        go_image = "golang@sha256:" + "b" * 64
        result = configure.desired_config(current, self.slots(), self.image, "unix:///run/forge.sock", self.tree.root,
                                           profile_images={"go-ceil-div": go_image})
        self.assertEqual(result["profiles"]["go-ceil-div"]["image"], go_image)
        changed = configure.desired_config(result, self.slots(), None, "unix:///run/forge.sock", self.tree.root,
                                            profile_images={"go-ceil-div": "golang@sha256:" + "c" * 64})
        self.assertEqual(changed["profiles"]["python"]["image"], self.image)
        with self.assertRaises(configure.common.UnsafeState):
            configure.desired_config(current, self.slots(), self.image, "unix:///run/forge.sock", self.tree.root,
                                      profile_images={"typo": go_image})

    def test_python_cannot_enable_executable_tmpfs(self):
        current = copy.deepcopy(self.current)
        current["profiles"]["python"]["tmpfs_executable"] = True
        with self.assertRaises(configure.common.UnsafeState):
            configure.desired_config(current, self.slots(), self.image, "unix:///run/forge.sock", self.tree.root)


if __name__ == "__main__":
    unittest.main()
