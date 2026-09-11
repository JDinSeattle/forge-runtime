#!/usr/bin/env python3
"""Author's saved-record checks for E27; no DB, Docker or network access."""
import base64
import collections
import datetime
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
REPO = ROOT.parents[2]


def read(path):
    return json.loads(path.read_text())


def digest(value):
    return hashlib.sha256(value).hexdigest()


def ns(value):
    """RFC3339 to integer ns, retaining saved sub-microsecond timestamps."""
    value = value.replace("Z", "+00:00")
    base, zone = value[:-6], value[-6:]
    seconds, _, fraction = base.partition(".")
    parsed = datetime.datetime.fromisoformat(seconds + zone)
    return int(parsed.timestamp()) * 10**9 + int((fraction + "000000000")[:9])


def tree_hash(files):
    value = "".join(f"{len(name.encode())}:{name}:{item['sha256']}:{str(item['executable']).lower()}\n"
                    for name, item in sorted(files.items()))
    return digest(value.encode())


def audit_case(name):
    directory = ROOT / name
    report = read(directory / "acceptance.json")
    final = read(directory / "cleanup-released-postgres.json")
    completed = read(directory / "completed-postgres.json")
    run, = final["runs"]
    run_id, tenant = run["id"], run["tenant_id"]
    assert report["passed"] is True
    assert run["state"] == ("cancelled" if name == "F10_cancel_first" else "completed")
    assert run["snapshot"]["verification_status"] == "verified"
    assert run["snapshot"]["workspace_revision"] == run["snapshot"]["verification_revision"] == 4
    assert len(final["effects"]) == 7
    artifacts = {item["id"]: item for item in final["artifacts"]}
    bodies = {}
    total_bytes = 0
    assert len(list((directory / "artifacts").glob("*.json"))) == len(artifacts)
    for identifier, item in artifacts.items():
        raw = (directory / "artifacts" / (identifier + ".json")).read_bytes()
        assert item["state"] == "ready"
        assert item["tenant_id"] == tenant and item["run_id"] == run_id
        assert item["object_key"] == f"{tenant}/{run_id}/{item['sha256']}"
        assert len(raw) == item["byte_size"] and digest(raw) == item["sha256"]
        if item["kind"] != "patch":
            bodies[identifier] = json.loads(raw)
        total_bytes += len(raw)
    effects = {effect["operation_id"]: effect for effect in final["effects"]}
    for identifier, effect in effects.items():
        assert effect["tenant_id"] == tenant and effect["run_id"] == run_id
        body = bodies[effect["receipt_ref"]]
        request = body["request"]
        assert request["tenant_id"] == tenant and request["run_id"] == request["workspace_id"] == run_id
        for field in ("operation_id", "kind", "args", "args_hash", "expected_revision", "policy_version"):
            assert request[field] == effect[field], (name, identifier, field)
        canonical = bytes.fromhex(effect["canonical_args"][2:])
        assert digest(canonical) == effect["args_hash"] and json.loads(canonical) == request["args"]
        assert request["epoch"] == effect["epoch"] and not request["grant"]
        assert body["status"] == effect["status"]
        assert body["status"] == ("failed" if identifier.endswith("_0_initial_target") else "succeeded")
    original = read(directory / "original-operation.json")
    target = report["target_operation"]
    assert target == run_id + "_step_1_op_0"
    effect = effects[target]
    original_body = dict(original)
    original_body["receipt"] = bodies[effect["receipt_ref"]]["receipt"]
    assert original_body == bodies[effect["receipt_ref"]]
    assert original["receipt"]["sha256"] == artifacts[effect["receipt_ref"]]["sha256"]
    assert original["receipt"]["size"] == artifacts[effect["receipt_ref"]]["byte_size"]
    assert original["request"]["epoch"] == 2
    assert original["status"] == "succeeded" and original["result"]["exit_code"] == 0
    assert base64.b64decode(original["result"]["output"]) == (directory / "original-command.stdout").read_bytes() == b'{"counter": 1}\n'
    docker = [json.loads(line) for line in (directory / "original-docker-events.jsonl").read_text().splitlines()]
    actions = collections.Counter(event["Action"] for event in docker)
    assert actions == {"create": 1, "start": 1, "die": 1}
    assert len({event["Actor"]["ID"] for event in docker}) == 1
    assert all(event["Actor"]["Attributes"]["name"] == original["job_id"] for event in docker)
    assert next(event for event in docker if event["Action"] == "die")["Actor"]["Attributes"]["exitCode"] == "0"
    attempts, reservations = final["model_attempts"], final["quota_reservations"]
    assert len(attempts) == len(reservations) == 4
    assert {a["attempt_id"] for a in attempts} == {r["id"] for r in reservations}
    assert all(a["status"] == "completed" and a["provider"] == a["model_id"] == "fake" for a in attempts)
    assert all(r["status"] == "settled" and r["request_slot_released"] for r in reservations)
    assert sum(r["actual_microusd"] for r in reservations) == sum(r["actual_tokens"] for r in reservations) == 570
    assert run["snapshot"]["cost_microusd"] == 570
    quota, = final["provider_quotas"]
    assert quota["committed_tokens"] == quota["committed_microusd"] == 570
    assert quota["active_requests"] == quota["reserved_tokens"] == quota["reserved_microusd"] == 0
    assert final["tenant_runtime"][0]["active_count"] == final["runners"][0]["reserved_slots"] == 0
    assert len(final["runner_allocations"]) == 1 and final["runner_allocations"][0]["state"] == "released"
    verification = bodies[run["snapshot"]["verification_report_ref"]]
    assert verification["source_hash"] == run["base_commit"]
    for field in ("trusted", "baseline_target_failed", "target_passed", "regression_passed"):
        assert verification["evidence"][field]
    assert verification["evidence"]["workspace_revision"] == 4
    assert set(verification["receipts"]) == {e["receipt_ref"] for e in effects.values() if e["kind"] == "verify"}
    cleanup, = final["workspace_cleanup"]
    assert cleanup["phase"] == "released" and cleanup["id"] == report["cleanup"]["id"]
    snapshot = bodies[cleanup["snapshot_ref"]]
    assert snapshot["workspace"]["run_id"] == run_id and snapshot["workspace"]["tenant_id"] == tenant
    assert snapshot["workspace"]["stopped"] and snapshot["workspace"]["revision"] == 4
    assert ns(artifacts[cleanup["snapshot_ref"]]["created_at"]) < ns(cleanup["released_at"])
    for filename, item in snapshot["files"].items():
        assert digest(base64.b64decode(item["content"])) == item["sha256"]
    assert base64.b64decode(snapshot["files"]["app-counter.txt"]["content"]) == b"1\n"
    assert base64.b64decode(snapshot["files"]["app.py"]["content"]) == (REPO / "testdata/repairs/clamp/expected/app.py").read_bytes()
    final_diff = bodies[effects[run_id + "_4_final_diff"]["receipt_ref"]]
    assert tree_hash(snapshot["files"]) == final_diff["result"]["workspace_hash"]
    for capture in (completed, final):
        assert [e["seq"] for e in capture["run_events"]] == list(range(1, capture["runs"][0]["next_event_seq"]))
    assert len(completed["run_events"]) == report["events"]
    assert len(final["run_events"]) == report["events"] + 3
    assert sum(e["type"] == "workspace.released" for e in final["run_events"]) == 1
    assert report["repeated_cleanup_same_record"]
    for kind in ("api", "worker"):
        role = read(directory / (kind + "-role-preflight.json"))
        assert not role["superuser"] and not role["member_of_runs_owner"]
        assert not role["create_database"] and not role["create_role"]
        gate = "production_CheckAPIRole" if kind == "api" else "production_CheckWorkerRole"
        assert role[gate] == "passed"
    http = read(directory / "http-observations.json")
    result = {"run_id": run_id, "schema": report["schema"], "terminal_status": run["state"],
              "terminal_version": run["version"], "ready_artifacts": len(artifacts), "artifact_bytes": total_bytes,
              "artifact_kinds": dict(collections.Counter(a["kind"] for a in artifacts.values())),
              "events_before_cleanup": len(completed["run_events"]), "events_after_cleanup": len(final["run_events"]),
              "effects": dict(collections.Counter(e["status"] for e in effects.values())),
              "synthetic_microusd": 570, "fixture_tokens": 570, "target_operation": target,
              "target_dispatch_epoch": 2, "original_receipt_sha256": original["receipt"]["sha256"],
              "docker_actions": dict(actions), "cleanup_id": cleanup["id"],
              "http": [{k: h[k] for k in ("at", "method", "path", "status")} for h in http]}
    if name.startswith("F10"):
        paused = read(directory / "worker1-paused.json")
        boundary = read(directory / "before-cancel-race-postgres.json")
        assert paused["operation"]["status"] == "succeeded" and paused["operation"]["request"]["operation_id"] == run_id + "_4_final_diff"
        assert boundary["runs"][0]["version"] == 27
        assert next(e for e in boundary["effects"] if e["operation_id"].endswith("_final_diff"))["status"] == "in_flight"
        cancels = [h for h in http if h["path"].endswith("/cancel")]
        assert all(h["status"] == 202 for h in cancels)
        assert all(h["response"]["state"]["version"] == run["version"] for h in cancels[-2:])
        if name == "F10_cancel_first":
            assert cancels[0]["response"]["state"]["status"] == "cancel_requested"
            stop = bodies[run["snapshot"]["output_ref"]]
            assert stop["no_active_operations"] and stop["workspace"]["stopped"]
        result["cancel_responses"] = [{"version": h["response"]["state"]["version"], "status": h["response"]["state"]["status"]} for h in cancels]
    else:
        assert read(directory / "original-before-outage.json") == original
        before = read(directory / "before-outage-postgres.json")
        assert next(e for e in before["effects"] if e["operation_id"] == target)["status"] == "in_flight"
        assert next(e for e in before["effects"] if e["operation_id"] == target)["receipt_ref"] is None
        claim = next(s["input_event"] for s in final["run_snapshots"] if s.get("input_event") and s["input_event"].get("kind") == "claimed" and s["input_event"]["epoch"] == 3)
        assert claim["owner"] == "worker2-0" and ns(claim["at"]) == ns(report["replacement_claim_db_time"])
        assert read(directory / "worker1-ready.json")["pid"] != read(directory / "worker2-ready.json")["pid"]
        result["replacement_claim_db_time"] = claim["at"]
        result["worker1_sigkill_requested_at"] = report["worker1_sigkill_at"]
        result["worker1_pid"] = report["worker1_pid"]
        result["worker2_pid"] = report["worker2_pid"]
        result["original_lease_until"] = report["original_lease_until"]
        result["lease_until_to_claim_ns"] = ns(claim["at"]) - ns(report["original_lease_until"])
        assert result["lease_until_to_claim_ns"] >= 0
        if name == "F11":
            assert http[1]["status"] == http[2]["status"] == 500 and http[-1]["status"] == 200
            during = read(directory / "during-database-outage-postgres.json")
            assert len(during["runs"]) == 1 and len(during["effects"]) == 2
            assert sum(e["type"] == "run.created" for e in during["run_events"]) == 1
            assert report["operation_count_during_outage"] == 2
            result["proxy_cut_duration_ns"] = ns(report["database_restore"]["at"]) - ns(report["database_cut"]["at"])
        else:
            during = read(directory / "during-runner-outage-postgres.json")
            assert during["runs"][0]["state"] == "needs_reconciliation"
            assert next(e for e in during["effects"] if e["operation_id"] == target)["status"] == "unknown"
            assert during["runs"][0]["runner_id"] == run["runner_id"]
            assert during["runners"][0]["reserved_slots"] == during["tenant_runtime"][0]["active_count"] == 1
            assert during["runner_allocations"][0]["state"] == "reserved"
            result["not_before"] = during["runs"][0]["not_before"]
            result["recovery_trigger"] = "production Driver.Defer ended lease and set not_before; not an untouched natural-expiry experiment"
            result["runner_sigkill_requested_at"] = report["runner_sigkill_at"]
            result["runner1_pid"] = report["runner1_pid"]
            result["runner2_pid"] = report["runner2_pid"]
            result["not_before_to_claim_ns"] = ns(claim["at"]) - ns(result["not_before"])
            assert result["not_before_to_claim_ns"] >= 0
            assert sum(e["type"] == "run.reconciliation_wait" for e in during["run_events"]) == 1
            assert [h["status"] for h in http] == [200, 200, 200, 202]
            for filename in ("sse-observed.json", "sse-during-runner-outage.json"):
                events = read(directory / filename)
                assert [e["seq"] for e in events] == list(range(1, len(events) + 1))
                for event in events:
                    stored = final["run_events"][event["seq"] - 1]
                    assert event["type"] == stored["type"] and event["payload"] == stored["payload"]
                    assert ns(event["created_at"]) == ns(stored["created_at"])
                result[filename.replace(".json", "_count")] = len(events)
            assert read(directory / "sse-during-runner-outage.json")[-1]["type"] == "run.message_added"
    return result


def main():
    cases = {name: audit_case(name) for name in ("F10_cancel_first", "F10_complete_first", "F11", "F12")}
    compilation = read(ROOT / "compilation-provenance.json")
    execution = read(ROOT / "execution-provenance.json")
    for name in compilation["added_test_sources_sha256"]:
        assert digest((ROOT / "source-snapshot" / (name + ".txt")).read_bytes()) == compilation["added_test_sources_sha256"][name]
    for name in ("application_test.go", "continuation_test.go"):
        assert digest((ROOT / "source-snapshot" / (name + ".txt")).read_bytes()) == execution["scripts/faults/application/" + name]["sha256"]
    for archived in (ROOT / "source-snapshot/production").rglob("*.txt"):
        name = str(archived.relative_to(ROOT / "source-snapshot/production"))[:-4]
        assert digest(archived.read_bytes()) == compilation["go_sources_sha256"][name]
    assert compilation["binary_sha256"] == execution["bin/application-network.test"]["sha256"]
    first = read(ROOT.parent / "application-network-api-preflight-20260911/F11/http-observations.json")
    retry = read(ROOT.parent / "application-network-api-preflight-20260911-retry/F11/http-observations.json")
    assert [h["status"] for h in first] == [200, 202, 500, 500]
    assert [h["status"] for h in retry] == [200, 202, 500, 500, 200]
    preflight = {"original_http_statuses": [h["status"] for h in first],
                 "retry_http_statuses": [h["status"] for h in retry],
                 "retry_first_restore_get": retry[-2]["at"], "retry_success_get": retry[-1]["at"],
                 "retry_restore_get_observation_gap_ns": ns(retry[-1]["at"]) - ns(retry[-2]["at"]),
                 "scope": "private API plus PG proxy only; no runner/worker or workspace allocation"}
    return {"passed": True, "evidence": "E27", "audit_kind": "harness author saved-record recomputation; not an independent implementation review or second execution",
            "capture_semantics": "separate non-atomic PostgreSQL table reads; historical SQLite counts and rejected heartbeat are harness assertions",
            "production_base_commit": compilation["base_commit"], "go_sources_in_compilation_manifest": len(compilation["go_sources_sha256"]),
            "cases": cases, "preserved_preflight": preflight, "totals": {"ready_artifacts": sum(c["ready_artifacts"] for c in cases.values()),
            "artifact_bytes": sum(c["artifact_bytes"] for c in cases.values()), "events_after_cleanup": sum(c["events_after_cleanup"] for c in cases.values()),
            "synthetic_microusd": 2280, "fixture_tokens": 2280}}


if __name__ == "__main__":
    print(json.dumps(main(), indent=2, sort_keys=True))
