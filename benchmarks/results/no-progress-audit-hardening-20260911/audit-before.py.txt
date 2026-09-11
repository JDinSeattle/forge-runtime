#!/usr/bin/env python3
"""Offline E42 audit: independently normalize retained receipts and replay counters.

This audits fixture records, not a live database or an independently built binary.
Only committed ContextBuilt inputs count; rejected/provisional reports are retained
but cannot advance the reconstructed counter.
"""
import base64
import copy
import hashlib
import json
import pathlib
import sys


def encode(value):
    raw = json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    for char, replacement in (("<", "\\u003c"), (">", "\\u003e"), ("&", "\\u0026"), ("\u2028", "\\u2028"), ("\u2029", "\\u2029")):
        raw = raw.replace(char, replacement)
    return raw.encode()


def digest(value):
    return hashlib.sha256(encode(value)).hexdigest()


def remember(items, item, limit):
    if item in items:
        return False
    items.append(item)
    del items[:-limit]
    return True


def fingerprint(op, verification=None):
    request = op["request"]
    assert digest(request["args"]) == request["args_hash"]
    kind, result = request["kind"], copy.deepcopy(op.get("result"))
    args = request["args_hash"]
    if kind == "read_file" and op["status"] == "succeeded":
        args = None
    uncertain = bool(op.get("result_truncated"))
    if kind in ("run_command", "verify") and result is not None:
        result["id"] = ""
        uncertain |= bool(result.get("truncated") or result.get("running"))
        if kind == "verify":
            uncertain |= len(base64.b64decode(result.get("output", ""))) > 4000
    if kind == "search_code" and result is not None:
        uncertain |= bool(result.get("truncated"))
    visible_bytes = len(encode(op["result"])) if "result" in op else 0
    if op.get("error"):
        visible_bytes += len(op["error"].encode()) + 8
    uncertain |= visible_bytes > 16000
    if uncertain:
        return None
    changing = kind in ("run_command", "verify", "apply_patch")
    value = digest({"Kind": kind, "Args": args, "Result": result,
                    "Status": op["status"], "Error": op.get("error", ""),
                    "InputTree": op["before_hash"] if changing else "",
                    "OutputTree": op["after_hash"] if changing else ""})
    if verification is not None:
        value = digest({"Result": value, "Baseline": verification["baseline_target_failed"],
                        "Target": verification["target_passed"], "Regression": verification["regression_passed"]})
    return value


def audit(data):
    assert data["passed"] is True
    run = data["run"]
    artifacts = data["artifact_bytes"]
    checked = []
    for row in sorted(data["run_snapshots"], key=lambda r: r["version"]):
        event, prior = row.get("input_event"), row.get("input_state")
        if not event or event["kind"] != "context_built" or not prior["limits"].get("max_no_progress_batches"):
            continue
        frame = event["progress"]
        raw = artifacts[event["progress_ref"]]
        assert json.loads(raw) == frame
        report_sha = hashlib.sha256(raw.encode()).hexdigest()
        identity = f'{run["tenant_id"]}:{run["id"]}:progress_report:{report_sha}'
        assert event["progress_ref"] == "artifact_" + hashlib.sha256(identity.encode()).hexdigest()
        assert frame["context_ref"] == event["output_ref"]
        assert frame["step_seq"] == prior["step_seq"]
        assert frame["policy_version"] == 1
        assert frame["step_seq"] not in checked
        checked.append(frame["step_seq"])
        if frame["step_seq"] == 0:
            assert not frame["observations"]
            expected = {"last_checked_step": 0, "message_seq": frame["message_seq"],
                        "repeated_batches": 0, "classification": "initial", "recent_evidence": [], "recent_trees": []}
        else:
            expected = copy.deepcopy(prior["progress"])
            novel, unknown = False, False
            verification = None
            if frame.get("verification_ref"):
                verification = json.loads(artifacts[frame["verification_ref"]])["evidence"]
                assert verification["trusted"]
            assert frame["attempt_id"] in [a["attempt_id"] for a in data["model_attempts"] if a["step_seq"] == frame["step_seq"] and a["status"] == "completed"]
            ledger = {e["operation_id"]: e for e in data["effects"] if e["step_seq"] == frame["step_seq"] and e.get("receipt_ref") and ((e["ordinal"] >= 0 and verification is None) or (e["ordinal"] in (-2, -3) and verification is not None))}
            assert set(ledger) == {o["effect_id"] for o in frame["observations"]}
            for item in frame["observations"]:
                receipt_bytes = artifacts[item["receipt_ref"]]
                op = json.loads(receipt_bytes)
                receipt_sha = hashlib.sha256(receipt_bytes.encode()).hexdigest()
                identity = f'{run["tenant_id"]}:{run["id"]}:operation_receipt:{receipt_sha}'
                assert item["receipt_ref"] == "artifact_" + hashlib.sha256(identity.encode()).hexdigest()
                assert op["request"]["operation_id"] == item["effect_id"]
                assert op["request"]["run_id"] == run["id"] and op["request"]["tenant_id"] == run["tenant_id"]
                binding = ledger[item["effect_id"]]
                assert binding["receipt_ref"] == item["receipt_ref"]
                assert op["request"]["workspace_id"] == run["id"]
                for request_key, ledger_key in (("args_hash", "args_hash"), ("epoch", "epoch"), ("kind", "kind"), ("expected_revision", "expected_revision"), ("policy_version", "policy_version")):
                    assert op["request"][request_key] == binding[ledger_key]
                assert op["status"] in ("succeeded", "failed", "cancelled")
                assert (op["before_hash"], op["after_hash"]) == (item["before_hash"], item["after_hash"])
                value = fingerprint(op, verification)
                assert (value is None) == bool(item.get("indeterminate"))
                assert value == item.get("fingerprint")
                if not expected["recent_trees"]:
                    remember(expected["recent_trees"], item["before_hash"], 32)
                novel |= remember(expected["recent_trees"], item["after_hash"], 32)
                if value is None:
                    unknown = True
                else:
                    novel |= remember(expected["recent_evidence"], value, 512)
            if frame["message_seq"] > expected["message_seq"]:
                classification = "message"
            elif novel:
                classification = "new_evidence"
            elif unknown:
                classification = "indeterminate"
            else:
                classification = "repeated"
            expected["repeated_batches"] = expected["repeated_batches"] + 1 if classification == "repeated" else 0
            expected.update(classification=classification, message_seq=frame["message_seq"], last_checked_step=frame["step_seq"])
        assert row["body"]["progress"] == expected
        if expected["repeated_batches"] >= prior["limits"]["max_no_progress_batches"]:
            assert row["body"]["status"] == "cancel_requested"
            assert row["body"]["stop_target"] == "budget_exhausted"
            assert row["body"]["step_seq"] == prior["step_seq"]
            assert row["body"]["model_rounds"] == prior["model_rounds"]
    extra = data["extra"]
    if "capacity_while_stop_unknown" in extra:
        for label, count in (("while_stop_unknown", 2), ("after_main_stop", 1), ("after_duplicate_stop", 1), ("after_sentinel_stop", 0)):
            sample = extra["capacity_" + label]
            assert sample["tenant_active"] == sample["runner_slots"] == count
            assert sum(a["slots"] for a in sample["allocations"] if a["state"] == "reserved") == count
    if "synthetic_unknown_microusd_after_stop" in extra:
        assert sum(r["microusd"] for r in data["quota_reservations"] if r["status"] == "unknown" and r["actual_microusd"] is None) == extra["synthetic_unknown_microusd_after_stop"] == 36864
    return {"run_id": run["id"], "committed_boundaries": len(checked), "passed": True}


if __name__ == "__main__":
    files = sorted(pathlib.Path(sys.argv[1]).glob("TestProgress*.json"))
    assert files, "no E42 reports found"
    print(json.dumps({"scope": "independent offline replay of retained fixture records", "reports": [audit(json.loads(p.read_text())) for p in files]}, indent=2))
