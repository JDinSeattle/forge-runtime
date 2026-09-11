#!/usr/bin/python3
"""No-network unit tests. Synthetic registration/ledger values are not model results."""
import copy
import importlib.util
import json
import os
import hashlib
from pathlib import Path
import tempfile
import subprocess
import unittest
from unittest.mock import patch

HERE = Path(__file__).absolute().parent


def module(name, filename):
    spec = importlib.util.spec_from_file_location(name, HERE / filename)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


common = module("eval_common_test", "common.py")
prepare = module("eval_prepare_test", "prepare.py")
evaluate = module("eval_run_test", "evaluate.py")
oracle = module("eval_oracle_test", "oracle.py")


def registration():
    # Deliberately synthetic, never passed to a provider or written as a quote.
    return {"provider": "openai", "model_id": "UNIT_TEST_ONLY", "config_id": "unit_test", "total_budget_microusd": 4000, "task_budgets_microusd": {case: 1000 for case in common.CASES}, "max_model_rounds": 3, "max_tool_calls": 6, "max_runtime_seconds": 120, "capabilities": {"tool_calling": True, "structured_output": False, "native_compaction": False, "tool_search": False, "native_async_tools": False, "context_window": 2048, "max_output_tokens": 64}, "model_spec": {"credential_group": "unit_test", "price_version": "unit_test_only", "input_price": 1, "output_price": 1, "cache_read_price": 0, "exact_pricing": True, "context_tokens": 1024, "max_output_tokens": 64, "request_timeout_ns": 1000000000}, "price_sources": ["https://example.invalid/unit-test-only"], "capability_sources": ["https://example.invalid/unit-test-only"]}


class EvaluationTests(unittest.TestCase):
    def test_prepare_writes_candidate_only_and_validation_never_runs_cli(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            common.save_json(root / "registration.json", registration())
            common.save_json(root / "platform-template.json", {"worker_id": "synthetic_worker", "fake_scripts": {"unused": []}})
            common.save_json(root / "runner-template.json", {"allow_test_backend": False, "runner_id": "synthetic_runner", "journal": "preserve-template-path"})
            command = ["python3", "-B", str(HERE / "prepare.py"), "--registration", str(root / "registration.json"), "--platform-template", str(root / "platform-template.json"), "--runner-template", str(root / "runner-template.json"), "--python-image", "synthetic@sha256:" + "a" * 64, "--go-image", "synthetic@sha256:" + "b" * 64, "--output", str(root / "candidate")]
            subprocess.check_output(command, text=True, timeout=90)
            _, _, _, platform = evaluate.load_bundle(root / "candidate")
            runner = json.loads((root / "candidate/runner.json").read_text())
            self.assertNotIn("fake_scripts", platform)
            self.assertEqual(runner["journal"], "preserve-template-path")
            self.assertEqual(set(runner["sources"]), {common.SOURCE_PREFIX + c for c in common.CASES})
            for profile in runner["profiles"].values():
                self.assertEqual(profile["memory_bytes"], 256 << 20)
                self.assertEqual(profile["tmpfs_executable"], profile["id"].startswith(common.SOURCE_PREFIX + "go-"))
            raw = subprocess.check_output(["python3", "-B", str(HERE / "evaluate.py"), "--bundle", str(root / "candidate"), "--output", str(root / "must-not-be-created")], text=True, timeout=10)
            self.assertIs(json.loads(raw)["no_network_or_provider_call"], True)
            self.assertFalse((root / "must-not-be-created").exists())
            with (root / "candidate/platform.json").open("a") as stream:
                stream.write(" ")
            with self.assertRaisesRegex(ValueError, "changed after preparation"):
                evaluate.load_bundle(root / "candidate")

    def test_production_model_spec_serialization_preserves_quote_zero_and_omission(self):
        registrations = []
        for exact, caches in ((True, {}), (True, {"cache_read_price": 0}), (False, {"cache_read_price": 0, "cache_write_5m_price": 0, "cache_write_1h_price": 0})):
            r = registration()
            r["model_spec"].pop("cache_read_price")
            r["model_spec"].update(exact_pricing=exact, **caches)
            if exact:
                r["model_spec"].update(input_price=0, output_price=0)
            registrations.append(prepare.validate_registration(r))
        raw = subprocess.check_output(["go", "run", str(HERE / "hash-sources.go"), "--model-spec-json"], input=json.dumps([r["model_spec"] for r in registrations]), cwd=common.REPO, env=dict(os.environ, GOCACHE="/tmp/forge-runtime-gocache", GOPROXY="off"), text=True, timeout=90)
        actual = json.loads(raw)
        for r, serialized in zip(registrations, actual, strict=True):
            self.assertEqual(serialized, r["model_spec"])
            quote = dict(serialized, schema_version=1, provider=r["provider"], model=r["model_id"])
            ledger = {"run": {"config_snapshot": {"provider": r["provider"], "model": r["model_id"]}}, "attempts": [{"attempt_id": "synthetic", "step_seq": 1, "attempt": 1, "provider": r["provider"], "model_id": r["model_id"], "pricing": quote, "status": "prepared", "error_code": None, "request_id": None, "usage": None, "response_persisted_at": None}], "reservations": []}
            self.assertEqual(evaluate.summarize_ledger(ledger, r)["attempts"][0]["pricing"], quote)
        self.assertNotIn("cache_read_price", actual[0])
        self.assertEqual(actual[1]["cache_read_price"], 0)
        self.assertIs(actual[2]["exact_pricing"], False)

    def test_oracle_requires_an_executed_assertion_not_just_exit_one(self):
        case = common.CASES[0]
        for raw in (b"", b"compile: no space left on device", b"null", json.dumps({"fixture": case, "suite": "target", "passed": False}).encode()):
            with self.assertRaises(ValueError):
                oracle.validate_proof(raw, 1, case, "source", "target")
        proof = {"fixture": case, "suite": "target", "case": 0, "passed": False}
        self.assertEqual(oracle.validate_proof(json.dumps(proof), 1, case, "source", "target"), proof)
        with self.assertRaises(ValueError):
            oracle.validate_proof(json.dumps(proof), 0, case, "source", "target")

    def test_v1_archive_reconstructs_original_failed_revision_hash(self):
        manifest = json.loads((HERE / "revisions/eval-v1-manifest.json").read_text())
        old = json.loads((HERE / "revisions/eval-v1-go-sources.json").read_text())["files"]
        rows = common.corpus_files()
        for row in rows:
            if row["path"] in old:
                raw = old[row["path"]].encode()
                row.update(sha256=hashlib.sha256(raw).hexdigest(), size=len(raw))
        self.assertEqual(rows, manifest["files"])
        with patch.object(common, "VERSION", "eval-v1"):
            self.assertEqual(common.corpus_digest(rows), manifest["sha256"])

    def test_fixed_corpus_has_four_disjoint_cases_and_exact_bytes(self):
        manifest = common.verify_corpus()
        self.assertEqual(len(manifest["case_order"]), 4)
        self.assertEqual(len(manifest["files"]), 20)
        self.assertFalse(set(manifest["case_order"]) & {"clamp", "expiry", "ranges", "go-ceil-div"})

    def test_corpus_digest_changes_with_bytes_order_or_executable_bit(self):
        original = common.corpus_files()
        changed = copy.deepcopy(original)
        changed[0]["executable"] = not changed[0]["executable"]
        self.assertNotEqual(common.corpus_digest(original), common.corpus_digest(changed))
        self.assertNotEqual(common.corpus_digest(original), common.corpus_digest(list(reversed(original))))

    def test_fake_missing_budget_overbudget_and_missing_prices_fail(self):
        for mutation in (
            lambda r: r.update(provider="fake"),
            lambda r: r.pop("total_budget_microusd"),
            lambda r: r.update(total_budget_microusd=3999),
            lambda r: r["model_spec"].pop("input_price"),
            lambda r: r.update(price_sources=[]),
            lambda r: r["capabilities"].update(tool_calling=False),
            lambda r: r["model_spec"].update(input_price=9223372036854775808),
            lambda r: r.update(api_key="must-not-enter-report"),
            lambda r: r["capabilities"].pop("native_compaction"),
            lambda r: r["model_spec"].update(cache_read_price=None),
            lambda r: r.update(capability_sources=[]),
        ):
            r = registration()
            mutation(r)
            with self.assertRaises(ValueError):
                prepare.validate_registration(r)
        self.assertEqual(prepare.validate_registration(registration())["model_id"], "UNIT_TEST_ONLY")

    def test_unknown_quote_needs_positive_bound_and_anthropic_cache_rates(self):
        r = registration()
        r["model_spec"].update(exact_pricing=False, input_price=0)
        with self.assertRaises(ValueError):
            prepare.validate_registration(r)
        r = registration()
        r["provider"] = "anthropic"
        with self.assertRaises(ValueError):
            prepare.validate_registration(r)

    def test_grader_selection_is_fixed(self):
        with self.assertRaises(ValueError):
            common.grade_command("../oracle", "target")
        with self.assertRaises(ValueError):
            common.grade_command(common.CASES[0], "operator-supplied-shell")
        self.assertNotIn("HOME=", " ".join(common.grade_command("go-midpoint", "target")))

    def test_report_keeps_failed_dispatched_unknown_reservation(self):
        r = registration()
        quote = dict(r["model_spec"], schema_version=1, provider=r["provider"], model=r["model_id"])
        base = {"step_seq": 1, "attempt": 1, "provider": r["provider"], "model_id": r["model_id"], "pricing": quote, "error_code": None, "request_id": None, "usage": None, "response_persisted_at": None}
        first = dict(base, attempt_id="known", status="completed")
        second = dict(base, attempt_id="unknown", attempt=2, status="failed", error_code="timeout")
        ledger = {"run": {"config_snapshot": {"provider": r["provider"], "model": r["model_id"]}}, "attempts": [first, second], "reservations": [{"id": "known", "status": "settled", "actual_microusd": 17, "microusd": 30, "dispatched_at": "2026-09-11T00:00:00Z"}, {"id": "unknown", "status": "unknown", "actual_microusd": None, "microusd": 90, "dispatched_at": "2026-09-11T00:00:00Z"}]}
        result = evaluate.summarize_ledger(ledger, r)
        self.assertEqual(result["known_ledger_cost_microusd"], 17)
        self.assertEqual(result["unknown_or_unsettled_reserved_microusd"], 90)
        self.assertEqual(result["conservative_ledger_exposure_microusd"], 107)
        self.assertEqual(result["real_provider_dispatches"], 2)
        self.assertIsNone(result["attempts"][1]["usage"])
        self.assertIsNone(result["vendor_billed_cost_microusd"])
        ledger["attempts"][1]["pricing"] = dict(quote, input_price=2)
        with self.assertRaisesRegex(ValueError, "immutable pricing"):
            evaluate.summarize_ledger(ledger, r)

    def test_unconfirmed_submission_never_disappears_from_budget_report(self):
        report = {"tasks": [], "submitted_or_unconfirmed_task_allocations": {common.CASES[0]: 1000}}
        evaluate.aggregate(report)
        self.assertEqual(report["unaudited_allocated_ceiling_microusd"], 1000)
        self.assertFalse(report["ledger_totals_complete"])
        self.assertEqual(report["verified_repairs"], 0)
        self.assertEqual(report["planned_tasks"], 4)

    def test_audit_rejects_nonlocal_or_implicit_database(self):
        for dsn in ("", "postgres://user@remote/forge", "postgres://user@127.0.0.1/another", "postgres://user@127.0.0.1/forge?options=unsafe"):
            with patch.dict(os.environ, {"FORGE_EVAL_AUDIT_DSN": dsn}), self.assertRaises(ValueError):
                evaluate.audit_environment()

    def test_report_creation_is_exclusive_and_atomic_replacement_is_explicit(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "report.json"
            common.save_json(path, {"one": 1})
            with self.assertRaises(FileExistsError):
                common.save_json(path, {"two": 2})
            self.assertEqual(json.loads(path.read_text()), {"one": 1})
            common.save_json(path, {"two": 2}, replace=True)
            self.assertEqual(json.loads(path.read_text()), {"two": 2})
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)


if __name__ == "__main__":
    unittest.main()
