import copy
import json
import os
import pathlib
import unittest

import audit


class ProgressAuditRegression(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        root = pathlib.Path(__file__).resolve().parents[2]
        directory = pathlib.Path(os.environ.get("FORGE_PROGRESS_AUDIT_FIXTURES", root / "benchmarks/results/no-progress-20260911/final"))
        cls.original = json.loads((directory / "TestProgressDurableClosedBatches_read.json").read_text())

    def test_valid_fixture(self):
        self.assertTrue(audit.audit(copy.deepcopy(self.original))["passed"])

    def test_changed_counter_rejected(self):
        data = copy.deepcopy(self.original)
        row = next(r for r in data["run_snapshots"] if (r.get("input_event") or {}).get("progress", {}).get("step_seq") == 1)
        row["body"]["progress"]["repeated_batches"] += 1
        with self.assertRaises(AssertionError):
            audit.audit(data)

    def test_receipt_change_rejected(self):
        data = copy.deepcopy(self.original)
        frame = next(r["input_event"]["progress"] for r in data["run_snapshots"] if (r.get("input_event") or {}).get("progress", {}).get("step_seq") == 1)
        ref = frame["observations"][0]["receipt_ref"]
        op = json.loads(data["artifact_bytes"][ref])
        op["after_hash"] = "f" * 64
        data["artifact_bytes"][ref] = json.dumps(op)
        with self.assertRaises(AssertionError):
            audit.audit(data)

    def test_ledger_argument_change_rejected(self):
        data = copy.deepcopy(self.original)
        item = next(e for e in data["effects"] if e["ordinal"] >= 0)
        item["args_hash"] = "e" * 64
        with self.assertRaises(AssertionError):
            audit.audit(data)


if __name__ == "__main__":
    unittest.main()
