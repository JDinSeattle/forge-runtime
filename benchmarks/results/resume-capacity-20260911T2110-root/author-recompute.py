#!/usr/bin/env python3
"""Read-only E31 author recomputation; no database, runner or network access."""
import collections
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def read(path):
    return json.loads(path.read_text())


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def run_of(capture, identifier):
    return next(r for r in capture["runs"] if r["id"] == identifier)


def main():
    results = {}
    for name in ("http-resume", "finalize", "cli-resume"):
        folder = ROOT / name
        summary = read(folder / "summary.json")
        identifier = summary["run_id"]
        captures = {p.name: read(p) for p in folder.glob("*-postgres.json")}
        initial = captures["before-resume-postgres.json"]
        original = run_of(initial, identifier)
        sentinel, = (r for r in initial["runs"] if r["id"] != identifier)
        sentinel_allocation = next(a for a in initial["runner_allocations"] if a["run_id"] == sentinel["id"])
        original_effect, = initial["effects"]
        assert original["state"] == "needs_reconciliation" and original["version"] == 7
        assert original["lease_epoch"] == 1 and original["runner_id"] == "review_runner"
        assert original_effect["status"] == "unknown" and original_effect["epoch"] == 1
        assert original_effect["operation_id"] == identifier + "_original_effect"
        assert digest(bytes.fromhex(original_effect["canonical_args"][2:])) == original_effect["args_hash"]
        assert original_effect["receipt_ref"] is None
        for label, capture in captures.items():
            target = run_of(capture, identifier)
            assert len(capture["runs"]) == 2 and run_of(capture, sentinel["id"]) == sentinel
            assert next(a for a in capture["runner_allocations"] if a["run_id"] == sentinel["id"]) == sentinel_allocation
            assert len(capture["effects"]) == 1
            for key in ("tenant_id", "id", "project_id", "principal_id", "task", "base_commit", "runner_id", "workspace_id", "input_snapshot", "config_snapshot"):
                assert target[key] == original[key], (name, label, key)
            effect, = capture["effects"]
            for key in original_effect.keys() - {"status", "receipt_ref"}:
                assert effect[key] == original_effect[key], (name, label, key)
            expected = 1 if target["state"] == "completed" else 2
            assert capture["tenant_runtime"][0]["active_count"] == capture["runners"][0]["reserved_slots"] == expected
            active = [a for a in capture["runner_allocations"] if a["state"] != "released"]
            assert sum(a["slots"] for a in active) == expected
            assert any(a["run_id"] == sentinel["id"] and a["slots"] == 1 for a in active)
            for run in capture["runs"]:
                events = [e for e in capture["run_events"] if e["run_id"] == run["id"]]
                assert [e["seq"] for e in events] == list(range(1, run["next_event_seq"]))
                snapshots = [s for s in capture["run_snapshots"] if s["run_id"] == run["id"]]
                assert [s["version"] for s in snapshots] == list(range(1, run["version"] + 1))
        fixture = read(folder / "control-fixture-artifact.json")
        raw = fixture["bytes"].encode()
        assert len(raw) == fixture["ref"]["size"] == 74
        assert digest(raw) == fixture["ref"]["sha256"]
        assert json.loads(raw)["no_runner_or_model_executed"] is True
        role = read(folder / "role.json")
        assert role["production_CheckAPIRole"] == "passed"
        assert not role["current_role_superuser"] and not role["current_role_bypassrls"]
        if name == "http-resume":
            assert read(folder / "active-owner-response.json")["status"] == 409
            responses = read(folder / "concurrent-resume-responses.json")
            assert collections.Counter(r["status"] for r in responses) == {202: 1, 409: 11}
            accepted = next(r["body"] for r in responses if r["status"] == 202)
            assert accepted["id"] == identifier and accepted["state"]["version"] == 8
            assert run_of(captures["after-concurrent-resume-postgres.json"], identifier)["version"] == 8
            reviewed = read(folder / "reviewed-repeat-response.json")
            assert reviewed["status"] == 202 and reviewed["body"]["state"]["version"] == 9
            final = captures["final-postgres.json"]
            assert run_of(final, identifier)["version"] == 9
            assert sum(e["type"] == "run.resume_requested" for e in final["run_events"]) == 2
            assert all(c["effects"] == initial["effects"] for c in captures.values())
        elif name == "finalize":
            final = captures["after-terminal-repeats-postgres.json"]
            assert final == captures["after-finalize-postgres.json"]
            assert run_of(captures["before-finalize-postgres.json"], identifier)["version"] == 15
            terminal = run_of(final, identifier)
            assert terminal["state"] == "completed" and terminal["version"] == 16 and terminal["lease_epoch"] == 2
            assert final["effects"][0]["status"] == "succeeded" and final["effects"][0]["epoch"] == 1
            assert sum(e["type"] == "run.finished" and e["run_id"] == identifier for e in final["run_events"]) == 1
            assert summary["accepted"] == 1 and summary["conflicts"] == 11
        else:
            cli = read(folder / "cli.json")
            stdout = (folder / "cli.stdout").read_bytes()
            assert digest(stdout) == cli["stdout_sha256"]
            assert json.loads(stdout) == cli["stdout"]
            assert not (folder / "cli.stderr").read_bytes() and cli["exit_code"] == 0
            assert cli["stdout"]["id"] == identifier and cli["stdout"]["state"]["version"] == 8
            assert cli["pid"] == 1256410 and cli["argv"][-2:] == ["--version", "7"]
            assert cli["binary_sha256"] == read(ROOT / "execution.json")["binaries"]["forge"]
            final = captures["after-cli-resume-postgres.json"]
            assert final["effects"] == initial["effects"]
        target = run_of(final, identifier)
        results[name] = {"run_id": identifier, "sentinel_run_id": sentinel["id"], "version_before_after": [7, target["version"]],
                         "final_status": target["state"], "original_effect_dispatch_epoch": original_effect["epoch"],
                         "final_run_epoch": target["lease_epoch"], "capacity_before_after": [2, final["runners"][0]["reserved_slots"]],
                         "final_target_events": sum(e["run_id"] == identifier for e in final["run_events"]),
                         "final_all_events_including_sentinel": len(final["run_events"]), "captures_checked": len(captures)}
    source = read(ROOT / "source-identity.json")
    assert source["whole_tree_unchanged"] and source["go_sources_before_build"] == source["go_sources_after_execution"]
    assert digest((ROOT / "resume_capacity_test.go.txt").read_bytes()) == source["test_source_sha256"]
    for archived in (ROOT / "source-snapshot").rglob("*.go.txt"):
        original_path = str(archived.relative_to(ROOT / "source-snapshot"))[:-4]
        assert digest(archived.read_bytes()) == source["go_sources_before_build"][original_path]
    assert source["binaries"] == read(ROOT / "execution.json")["binaries"]
    assert read(ROOT / "execution.json")["exit_code"] == 0
    return {"evidence": "E31", "passed": True, "scope": "author recomputation of saved control fixture records; no additional execution or independent implementation review",
            "capture_semantics": "each exported capture is one PostgreSQL statement/MVCC snapshot; captures from different phases are separate",
            "go_source_hash_count": len(source["go_sources_before_build"]), "cases": results,
            "limits": ["Finalize concurrent rejection counts are retained Go assertions; only one committed terminal event exists in saved PG records",
                       "Artifact content explicitly declares no runner/model execution; declared stop/verification receipts are semantic fixtures",
                       "Effective API role is SET ROLE from administrator session; not a separately restricted LOGIN"]}


if __name__ == "__main__":
    print(json.dumps(main(), indent=2, sort_keys=True))
