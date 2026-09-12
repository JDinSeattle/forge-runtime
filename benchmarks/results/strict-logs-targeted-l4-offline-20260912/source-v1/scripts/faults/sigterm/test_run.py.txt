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
            argv, env = run.command(phase, a, "02" if phase in ("logs-cleanup", "logs-recovery", "logs-l4") else "01")
            self.assertNotIn(self.secret, repr(argv))
            if phase == "logs-cleanup":
                self.assertNotIn("FORGE_TEST_DATABASE_URL", env)
            else:
                self.assertEqual(env["FORGE_TEST_DATABASE_URL"], self.dsn)
            self.assertEqual(env["HOME"], str(self.repo))
            self.assertEqual(sum(details[1] in env for details in run.PHASES.values()), 1)
            self.assertNotIn("FORGE_REVIEW_DATABASE_URL", env)

    def test_attempt_selection_is_explicit_and_does_not_read_credentials_on_invalid_input(self):
        a = {"scope_root": str(self.repo), "test_binary": str(self.repo / "bin/application-faults.test")}
        with patch.object(run, "credential", return_value=self.dsn) as credentials:
            for phase, attempt in (("abort", "02"), ("sigterm", "03"), ("missing", "01")):
                with self.assertRaises(ValueError):
                    run.command(phase, a, attempt)
                with self.assertRaises(ValueError):
                    run.launch(phase, attempt)
            credentials.assert_not_called()
            for phase, attempt, name in (("sigterm", "01", "acceptance.json"), ("sigterm", "02", "acceptance-02.json"),
                                         ("logs", "02", "acceptance-02.json"), ("abort", "01", "acceptance-abort-01.json")):
                argv, env = run.command(phase, a, attempt)
                self.assertEqual(env[run.PHASES[phase][2]], str(self.repo / name))
                self.assertNotIn(self.secret, repr(argv))

    def test_log_execution_is_separate_from_sigterm_source_and_cleanup_has_no_database(self):
        a = {"scope_root": str(self.repo), "test_binary": str(self.repo / "bin/application-faults.test")}
        with patch.object(run, "credential", side_effect=AssertionError("cleanup must not read a credential")):
            argv, env = run.command("logs-cleanup", a, "02")
            self.assertEqual(env["FORGE_STRICT_LOGS_ACCEPTANCE"], str(self.repo / "acceptance-logs-cleanup-01.json"))
            self.assertNotIn("FORGE_TEST_DATABASE_URL", env)
            self.assertNotIn("FORGE_STRICT_LOGS_EXECUTION", env)
            for phase, attempt, execution in (("logs", "01", "02"), ("sigterm", "02", "02"), ("logs-cleanup", "01", "01"), ("logs-cleanup", "02", "02")):
                with self.assertRaises(ValueError):
                    run.command(phase, a, attempt, execution)
        self.credential()
        _, old = run.command("logs", a, "02")
        _, new = run.command("logs", a, "02", "02")
        self.assertEqual(old["FORGE_STRICT_LOGS_ACCEPTANCE"], str(self.repo / "acceptance-02.json"))
        self.assertEqual(old["FORGE_STRICT_LOGS_EXECUTION"], "01")
        self.assertEqual(new["FORGE_STRICT_LOGS_ACCEPTANCE"], str(self.repo / "acceptance-logs-02.json"))
        self.assertEqual(new["FORGE_STRICT_LOGS_EXECUTION"], "02")
        _, third = run.command("logs", a, "02", "03")
        self.assertEqual(third["FORGE_STRICT_LOGS_ACCEPTANCE"], str(self.repo / "acceptance-logs-03.json"))
        self.assertEqual(third["FORGE_STRICT_LOGS_EXECUTION"], "03")
        for phase, attempt, execution in (("logs", "01", "03"), ("sigterm", "02", "03"), ("logs-cleanup", "02", "03"), ("logs", "02", "04")):
            with self.assertRaises(ValueError):
                run.command(phase,a,attempt,execution)

    def test_new_log_report_does_not_accept_old_success_or_partial_cleanup(self):
        evidence = self.repo / "evidence"
        evidence.mkdir(mode=0o700)
        (evidence / "logs-01").mkdir(mode=0o700)
        cases = {name: {"passed": True} for name in ("L1", "L2-L3-default", "L3-bytes", "L3-count", "L4", "L5")}
        run.p.save(evidence / "logs-01/acceptance.json", {"passed": True, "cases": cases})
        with self.assertRaises(OSError):
            run.completed_report("logs", self.repo, "02", "02")
        (evidence / "logs-02").mkdir(mode=0o700)
        run.p.save(evidence / "logs-02/acceptance.json", {"passed": True, "cases": cases})
        run.completed_report("logs", self.repo, "02", "02")
        (evidence / "logs-01-cleanup").mkdir(mode=0o700)
        report = evidence / "logs-01-cleanup/report.json"
        run.p.save(report, {"passed": True, "released": True, "snapshot_verified": True})
        run.completed_report("logs-cleanup", self.repo, "02")
        report.write_text(run.json.dumps({"passed": True, "released": True}))
        with self.assertRaises(ValueError):
            run.completed_report("logs-cleanup", self.repo, "02")

    def test_l4_phases_cannot_be_confused_with_prior_combined_results(self):
        self.credential()
        a = {"scope_root": str(self.repo), "test_binary": str(self.repo / "application-faults.test")}
        for phase, test, name in (("logs-recovery", "TestStrictLogsL4Recovery", "acceptance-logs-03-recovery.json"),
                                  ("logs-l4", "TestStrictLogsTargetedL4Acceptance", "acceptance-logs-l4-01.json")):
            argv, env = run.command(phase, a, "02")
            self.assertIn("-test.run=^" + test + "$", argv)
            self.assertEqual(env["FORGE_STRICT_LOGS_ACCEPTANCE"], str(self.repo / name))
            self.assertNotIn("FORGE_STRICT_LOGS_EXECUTION", env)
            for attempt, execution in (("01", "01"), ("02", "02"), ("02", "03")):
                with self.assertRaises(ValueError):
                    run.command(phase, a, attempt, execution)
        evidence = self.repo / "evidence"
        (evidence / "logs-l4-01").mkdir(mode=0o700, parents=True)
        (evidence / "logs-03-recovery").mkdir(mode=0o700)
        recovery = {"passed": True, "run_id": "run_DKT2OOLEVCKYXHW5NGNS7BKBHJ", "released": True, "snapshot_verified": True,
                    "input_sha256_before": {"bound": "hash"}, "input_sha256_after": {"bound": "hash"}}
        rp = evidence / "logs-03-recovery/report.json"
        run.p.save(rp, recovery)
        run.completed_report("logs-recovery", self.repo, "02")
        for field in ("passed", "run_id", "released", "snapshot_verified", "input_sha256_after"):
            changed = dict(recovery); changed.pop(field)
            rp.write_text(run.json.dumps(changed))
            with self.assertRaises(ValueError): run.completed_report("logs-recovery", self.repo, "02")
        report = evidence / "logs-l4-01/acceptance.json"
        for cases in ({"L4": {"passed": True}}, {"L5": {"passed": True}}, {"L4": {"passed": False}}, {"L4": {"passed": True}, "L1": {"passed": True}}):
            report.write_text(run.json.dumps({"passed": True, "cases": cases})); report.chmod(0o600)
            if cases == {"L4": {"passed": True}}: run.completed_report("logs-l4", self.repo, "02")
            else:
                with self.assertRaises(ValueError): run.completed_report("logs-l4", self.repo, "02")

    def test_l4_preparation_is_exclusive_and_targeted_case_requires_completed_recovery(self):
        base = self.repo / "scope"; base.mkdir(mode=0o700)
        for leaf in ("evidence/logs-03", "evidence/logs-03-retained-observation", "runtime", "bin/" + "c"*40):
            (base / leaf).mkdir(mode=0o700, parents=True, exist_ok=True)
        names = ("forge-runner", "forge-worker", "application-faults.test")
        for name in names:
            p = base / "bin" / ("c"*40) / name; p.write_bytes(name.encode()); p.chmod(0o700)
        run.p.save(base / "evidence/logs-03/acceptance.json", {"passed": False, "cases": {name: {"passed": True} for name in ("L1", "L2-L3-default", "L3-bytes", "L3-count", "L5")}})
        original = {"scope_root": str(base), "runner_config": "unchanged", "test_binary": "old"}
        digest = run.p.digest
        def selected_digest(path):
            if path == base / "evidence/logs-03/manifest.json": return "8781bba443e5a83da96cb077eb18223facfeee3469de0b23fce6c37529eef97c"
            if path == base / "evidence/logs-03-retained-observation/manifest.json": return "7ace4cab71b3f4fbaba9f8ed58ae73dbfe202ab94d83bb4ecbe85bac6d8ab736"
            return digest(path)
        with patch.object(run, "inputs", return_value=(base, original)), patch.object(run.p, "digest", side_effect=selected_digest), patch.object(run, "credential") as credential:
            with self.assertRaises(OSError): run.prepare_l4("logs-l4", "c"*40)
            self.assertFalse((base / "acceptance-logs-l4-01.json").exists())
            result = run.prepare_l4("logs-recovery", "c"*40)
            self.assertFalse(result["executed"])
            self.assertEqual(run.private_json(base / "acceptance-logs-03-recovery.json")["runner_config"], "unchanged")
            with self.assertRaises(ValueError): run.prepare_l4("logs-recovery", "c"*40)
            (base / "evidence/logs-03-recovery").mkdir(mode=0o700)
            run.p.save(base / "evidence/logs-03-recovery/report.json", {"passed": True, "run_id": "run_DKT2OOLEVCKYXHW5NGNS7BKBHJ", "released": True, "snapshot_verified": True, "input_sha256_before": {"a":"b"}, "input_sha256_after": {"a":"b"}})
            run.p.save(base / "evidence/logs-03-recovery/manifest.json", {"preserved":"hash"})
            result = run.prepare_l4("logs-l4", "c"*40)
            seal = run.private_json(base / "logs-l4-continuation.json")
            self.assertEqual(seal["acceptance_sha256"], digest(base / "acceptance-logs-l4-01.json"))
            self.assertEqual(seal["recovery_manifest_sha256"], digest(base / "evidence/logs-03-recovery/manifest.json"))
            with self.assertRaises(ValueError): run.prepare_l4("logs-l4", "c"*40)
            credential.assert_not_called()

    def test_second_attempt_cannot_use_old_success_or_claim_terminal_abort(self):
        case = self.repo / "evidence/sigterm-01/worker-runner-sigterm"
        case.mkdir(mode=0o700, parents=True)
        run.p.save(case / "acceptance.json", {"passed": True})
        run.completed_report("sigterm", self.repo)
        with self.assertRaises(OSError):
            run.completed_report("sigterm", self.repo, "02")
        abort = self.repo / "evidence/sigterm-01/abort-before-runner"
        abort.mkdir(mode=0o700)
        path = abort / "report.json"
        run.p.save(path, {"passed": True, "status": "cancel_requested", "terminal": False})
        run.completed_report("abort", self.repo)
        for value in ({"passed": True}, {"passed": True, "status": "cancelled", "terminal": True}):
            path.write_text(run.json.dumps(value))
            with self.assertRaises(ValueError):
                run.completed_report("abort", self.repo)

    def test_continuation_preserves_original_and_requires_one_versioned_binary_set(self):
        base = self.repo / "scope"
        base.mkdir(mode=0o700)
        for relative in ("bin", "runtime", "evidence"):
            (base / relative).mkdir(mode=0o700, parents=True)
        host = "unix:///run/user/1000/test.sock"
        original = {"purpose": run.p.PURPOSE, "fixture_id": run.IDENTITY, "scope_root": str(base), "pool_root": str(base / "pool-root"),
                    "runner_config": str(base / "runtime/runner.json"), "evidence_dir": str(base / "evidence/sigterm-01")}
        revision = "a" * 40
        new_bin = base / "bin" / revision
        new_bin.mkdir(mode=0o700)
        for label, filename in (("runner", "forge-runner"), ("worker", "forge-worker"), ("test", "application-faults.test")):
            old = base / "bin" / filename
            old.write_bytes(b"old " + label.encode())
            old.chmod(0o700)
            new = new_bin / filename
            new.write_bytes(b"new " + label.encode())
            new.chmod(0o700)
            original[label + "_binary"] = str(old)
            original[label + "_sha256"] = run.p.digest(old)
        run.p.save(base / "acceptance.json", original)
        config = {key: str(base / "runtime" / leaf) for key, leaf in
                  {"root_dir": "engine", "journal_path": "journal.sqlite", "artifact_root": "artifacts", "signing_key_file": "runner.key"}.items()}
        config.update(allow_test_backend=False, docker_host=host)
        run.p.save(base / "runtime/runner.json", config)
        run.p.save(base / "runtime/configuration-observations.json", {"runner_config_sha256": run.p.digest(base / "runtime/runner.json")})
        retained = {path: path.read_bytes() for path in (base / "acceptance.json", base / "runtime/runner.json", base / "bin/forge-runner")}
        with patch.object(run.p, "read_preparation", return_value=(base, {"docker_host": host})), patch.object(run, "credential") as credential:
            for value in ("../bad", "a" * 39, "A" * 40, None):
                with self.assertRaises(ValueError):
                    run.prepare_continuation(value)
            result = run.prepare_continuation(revision)
            self.assertFalse(result["executed"])
            run.inputs("abort", "01")
            run.inputs("sigterm", "02")
            credential.assert_not_called()
            sigterm = base / "evidence/sigterm-02/worker-runner-sigterm"
            sigterm.mkdir(mode=0o700, parents=True)
            run.p.save(sigterm / "acceptance.json", {"passed": True})
            logs = base / "evidence/logs-01"
            logs.mkdir(mode=0o700)
            run.p.save(logs / "acceptance.json", {"passed": False})
            next_revision = "b" * 40
            next_bin = base / "bin" / next_revision
            next_bin.mkdir(mode=0o700)
            for old_path in new_bin.iterdir():
                executable = next_bin / old_path.name
                executable.write_bytes(b"logs revision " + old_path.read_bytes())
                executable.chmod(0o700)
            before_logs = (base / "acceptance-02.json").read_bytes()
            for value in (None, "b" * 39):
                with self.assertRaises(ValueError):
                    run.prepare_logs_continuation(value)
            result = run.prepare_logs_continuation(next_revision)
            self.assertFalse(result["executed"])
            run.inputs("logs-cleanup", "02")
            run.inputs("logs", "02", "02")
            self.assertEqual((base / "acceptance-02.json").read_bytes(), before_logs)
            credential.assert_not_called()
            with self.assertRaises(ValueError):
                run.prepare_logs_continuation(next_revision)
            with self.assertRaises(ValueError):
                run.prepare_continuation(revision)
            continuation = base / "acceptance-02.json"
            valid = continuation.read_bytes()
            a = run.json.loads(valid)
            for replacement in (str(base / "bin/forge-worker"), str(new_bin / "nested/forge-worker")):
                a["worker_binary"] = replacement
                continuation.write_text(run.json.dumps(a))
                with self.assertRaises(ValueError):
                    run.inputs("sigterm", "02")
            continuation.write_bytes(valid)
            executable = new_bin / "forge-worker"
            executable.unlink()
            executable.symlink_to(base / "bin/forge-worker")
            with self.assertRaises(ValueError):
                run.inputs("sigterm", "02")
        self.assertEqual({path: path.read_bytes() for path in retained}, retained)

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
