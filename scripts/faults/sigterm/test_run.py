import importlib.util
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("sigterm_run", Path(__file__).with_name("run.py"))
run = importlib.util.module_from_spec(spec)
spec.loader.exec_module(run)


class LaunchBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="forge-lifecycle-launch-")
        self.addCleanup(self.temp.cleanup)
        self.repo = Path(self.temp.name)
        (self.repo / "var").mkdir(mode=0o700)
        (self.repo / "var/local").mkdir(mode=0o700)
        item = patch.object(run.p, "REPO", self.repo)
        item.start()
        self.addCleanup(item.stop)
        self.path = self.repo / "var/local/review-database.env"
        self.secret = "unit_secret_never_to_argv"
        self.dsn = "postgres://test:" + self.secret + "@127.0.0.1:32773/forge?sslmode=disable"

    def credential(self, value=None):
        self.path.write_text(value or "FORGE_REVIEW_DATABASE_URL='" + self.dsn + "'\n")
        self.path.chmod(0o600)

    def test_credentials_only_enter_child_environment(self):
        self.credential()
        a = {"scope_root": str(self.repo), "test_binary": str(self.repo / "bin/application-faults.test")}
        for phase in run.PHASES:
            argv, env = run.command(phase, a)
            self.assertNotIn(self.secret, repr(argv))
            self.assertEqual(env["FORGE_TEST_DATABASE_URL"], self.dsn)
            self.assertEqual(sum(key.startswith("FORGE_RUN_") for key in env), 1)
            self.assertNotIn("FORGE_REVIEW_DATABASE_URL", env)

    def test_wrong_database_and_duplicates_refused_without_disclosing_secret(self):
        for value in (self.dsn.replace("127.0.0.1", "example.com"), self.dsn + "&search_path=public", self.dsn.replace("32773", "5432")):
            self.credential("FORGE_REVIEW_DATABASE_URL='" + value + "'\n")
            with self.assertRaises(ValueError) as caught:
                run.credential()
            self.assertNotIn(self.secret, str(caught.exception))
        self.credential(("FORGE_REVIEW_DATABASE_URL='" + self.dsn + "'\n") * 2)
        with self.assertRaises(ValueError):
            run.credential()

    def test_shared_or_symlinked_credential_refused(self):
        self.credential()
        self.path.chmod(0o644)
        with self.assertRaises(ValueError):
            run.credential()
        self.path.chmod(0o600)
        other = self.path.with_name("alias")
        os.link(self.path, other)
        with self.assertRaises(ValueError):
            run.credential()
        self.path.unlink()
        self.path.symlink_to(other)
        with self.assertRaises(ValueError):
            run.credential()

    def test_unmapped_child_refuses_before_reading_inputs_or_credentials(self):
        with patch.object(run.os, "getuid", return_value=1000), patch.object(run, "inputs") as inputs, patch.object(run, "credential") as credentials:
            with self.assertRaisesRegex(ValueError, "subordinate"):
                run.mapped_child("sigterm")
            inputs.assert_not_called()
            credentials.assert_not_called()

    def test_zero_process_exit_cannot_substitute_for_actual_case_reports(self):
        with self.assertRaises(OSError):
            run.completed_report("logs", self.repo)
        root = self.repo / "evidence"
        root.mkdir(mode=0o700)
        folder = root / "logs-01"
        folder.mkdir(mode=0o700)
        report = folder / "acceptance.json"
        cases = {name: {"passed": True} for name in ("L1", "L2-L3-default", "L3-bytes", "L3-count", "L4", "L5")}
        run.p.save(report, {"passed": True, "cases": cases})
        run.completed_report("logs", self.repo)
        cases["L4"]["passed"] = False
        report.write_text(run.json.dumps({"passed": True, "cases": cases}))
        with self.assertRaisesRegex(ValueError, "six actual"):
            run.completed_report("logs", self.repo)
        del cases["L4"]
        report.write_text(run.json.dumps({"passed": True, "cases": cases}))
        with self.assertRaisesRegex(ValueError, "six actual"):
            run.completed_report("logs", self.repo)

    def test_launch_uses_only_new_unit_and_retains_prior_attempt(self):
        # This tests launch boundaries with a recording process, not systemd,
        # RootlessKit, Docker, mount verification, or a successful acceptance.
        self.credential()
        credential = run.credential()
        # Simulate the launcher's fixed host UID without changing the shared
        # os module used to validate real temporary-file ownership. CI may run
        # as a different UID; credential validation is covered separately.
        host_os = SimpleNamespace(**vars(os))
        host_os.getuid = lambda: 1000
        host_os.geteuid = lambda: 1000
        base = self.repo / "scope"
        base.mkdir(mode=0o700)
        (base / "evidence").mkdir(mode=0o700)
        calls = []

        class RecordingClient:
            def __init__(self, argv, **kwargs):
                calls.append(argv)

            def wait(self, timeout):
                return 1  # Retain a failure; never automatically retry it.

        with patch.object(run, "os", host_os), \
             patch.object(run, "credential", return_value=credential), \
             patch.object(run, "inputs", return_value=(base, {})), \
             patch.object(run, "unit_state", return_value={"LoadState": "not-found"}), \
             patch.object(run.p, "digest", return_value="a" * 64), \
             patch.object(run.subprocess, "Popen", RecordingClient), \
             patch("builtins.print"):
            self.assertEqual(run.launch("sigterm"), 1)
            with self.assertRaises(FileExistsError):
                run.launch("sigterm")
        self.assertEqual(len(calls), 1)
        argv = calls[0]
        self.assertEqual(argv[0], "/usr/bin/systemd-run")
        unit = next(arg for arg in argv if arg.startswith("--unit="))
        self.assertRegex(unit, r"^--unit=forge-lifecycle-lr20260912_a-sigterm-[a-f0-9]{16}\.service$")
        self.assertNotIn(self.secret, repr(argv))
        self.assertFalse(any("EnvironmentFile" in arg or "--setenv" in arg for arg in argv))
        self.assertIn("--property=RuntimeMaxSec=240s", argv)
        self.assertIn("--property=KillMode=control-group", argv)
        self.assertEqual((base / "evidence/host-sigterm/execution.log").stat().st_mode & 0o777, 0o600)
        self.assertTrue((base / "evidence/host-sigterm/result.json").exists())


if __name__ == "__main__":
    unittest.main()
