#!/usr/bin/env python3
"""Audit retained F03 evidence offline; never relabel or modify raw reports."""
import argparse
import copy
import datetime as dt
import hashlib
import json
from pathlib import Path


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def instant(value):
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))


def wire_fragments(native, frames):
    """Reconstruct only the text and tool fragments of this frozen F03 stream."""
    text, fragments, tools, closed = [], [], {}, set()
    for frame in frames:
        kind = frame["type"]
        if native == "openai":
            if kind == "response.output_text.delta":
                text.append(frame["delta"])
            elif kind == "response.output_item.added":
                item = frame["item"]
                require(item["type"] == "function_call" and item["arguments"] == "" and item["id"] not in tools, "invalid tool start")
                tools[item["id"]] = (item["call_id"], item["name"])
            elif kind == "response.function_call_arguments.delta":
                key = frame["item_id"]
                require(key in tools and key not in closed, "tool fragment outside open item")
                fragments.append((*tools[key], frame["delta"]))
            elif kind in ("response.function_call_arguments.done", "response.output_item.done"):
                item = frame["item"] if kind == "response.output_item.done" else frame
                key = item["id"] if kind == "response.output_item.done" else item["item_id"]
                require(key in tools and key not in closed, "tool completion outside open item")
                call_id, name = tools[key]
                require(item["name"] == name and item["arguments"] == "".join(delta for call, _, delta in fragments if call == call_id), "closed tool differs from sent fragments")
                if kind == "response.output_item.done":
                    require(item["type"] == "function_call" and item["call_id"] == call_id and item["status"] == "completed", "closed tool identity mismatch")
                    closed.add(key)
        else:
            if kind == "content_block_start":
                block = frame["content_block"]
                if block["type"] == "text":
                    text.append(block["text"])
                elif block["type"] == "tool_use":
                    require(frame["index"] not in tools and block["input"] == {}, "invalid tool start")
                    tools[frame["index"]] = (block["id"], block["name"])
            elif kind == "content_block_delta":
                delta = frame["delta"]
                if delta["type"] == "text_delta":
                    text.append(delta["text"])
                elif delta["type"] == "input_json_delta":
                    key = frame["index"]
                    require(key in tools and key not in closed, "tool fragment outside open block")
                    fragments.append((*tools[key], delta["partial_json"]))
            elif kind == "content_block_stop" and frame["index"] in tools:
                require(frame["index"] not in closed, "tool block closed twice")
                closed.add(frame["index"])
    require(set(tools.values()) == {("closed_tool", "read_file"), ("partial_tool", "apply_patch")} and len(tools) == 2, "wire tool identities differ")
    require({tools[key][0] for key in closed} == {"closed_tool"}, "wrong wire tool completion boundary")
    closed_args = "".join(delta for call, _, delta in fragments if call == "closed_tool")
    require(closed_args == '{"path":"never-read.txt"}', "closed wire tool arguments differ")
    return "".join(text), fragments


def audit(directory, report):
    require(report["passed"] is True and report["test_failed"] is False, "original execution failed")
    native = report["native"]
    require(native in ("openai", "anthropic"), "unknown native adapter")
    wire = report["wire_requests"]
    require(len(wire) == 1, "hidden native request replay")
    wire = wire[0]
    require(wire["path"] == ("/responses" if native == "openai" else "/v1/messages"), "wrong native route")
    require(wire["request"]["stream"] is True and wire["request"]["model"] == "f03-native", "not a native streamed request")
    require(wire["tcp_closed_before_final_chunk"] is True and not wire.get("error"), "no actual socket cut")
    require(instant(wire["request_at"]) <= instant(wire["flushed_at"]) <= instant(wire["closed_at"]), "wire timestamps out of order")
    frames = wire["frames"]
    forbidden = {"response.completed", "message_delta", "message_stop"}
    require(not any(frame["type"] in forbidden for frame in frames), "terminal turn/final usage was sent")
    text = "F03 provisional text: these proposed tools must not execute."
    if native == "openai":
        require(any(f["type"] == "response.output_item.done" and f["item"]["call_id"] == "closed_tool" for f in frames), "no closed tool item")
        require(frames[-1]["type"] == "response.function_call_arguments.delta", "wrong partial frame")
        partial = frames[-1]["delta"]
    else:
        require(any(f["type"] == "content_block_stop" and f["index"] == 1 for f in frames), "no closed tool item")
        require(frames[-1]["type"] == "content_block_delta", "wrong partial frame")
        partial = frames[-1]["delta"]["partial_json"]
        require(frames[0]["message"]["usage"]["input_tokens"] == 12, "missing initial nonfinal usage")
    try:
        json.loads(partial)
    except json.JSONDecodeError:
        pass
    else:
        raise AssertionError("partial tool arguments actually complete")
    wire_text, wire_tools = wire_fragments(native, frames)
    require(wire_text == text, "wire did not send the observed provisional text")
    adapter = report["adapter"]
    require(adapter["calls"] == 1 and adapter["error_kind"] == "stream_interrupted" and adapter["returned_error"], "wrong adapter completion")
    require(not adapter["returned_turn"].get("tool_calls") and not adapter["returned_turn"].get("native_state") and not adapter["returned_turn"]["usage"]["final"], "executable partial result escaped")
    events = adapter["events"]
    require([e["sequence"] for e in events] == list(range(1, len(events) + 1)), "adapter sequence gap")
    require(events[-1]["type"] == "model.attempt_failed" and events[-1]["usage"]["final"] is False, "no nonfinal failure usage")
    require(sum(e["type"] == "model.attempt_failed" for e in events) == 1, "multiple failed markers")
    require(not any(e["type"] == "model.attempt_completed" for e in events), "adapter completed")
    texts = [e for e in events if e["type"] == "model.text_delta"]
    tools = [e for e in events if e["type"] == "model.tool_arguments_delta"]
    require("".join(e.get("delta", "") for e in texts) == text and len(tools) == 2, "partial text/tools not observed")
    require(all(e["provisional"] for e in texts + tools), "fragment marked final")
    require({e["call_id"] for e in tools} == {"closed_tool", "partial_tool"}, "tool identities differ")
    require([(e["call_id"], e["tool_name"], e["delta"]) for e in tools] == wire_tools, "observer tool fragments differ from native wire")
    require(events[0]["type"] == "model.attempt_started" and events[-1]["error_kind"] == "stream_interrupted", "observer boundary mismatch")
    require(adapter["returned_turn"]["usage"] == events[-1]["usage"], "returned usage differs from observed failure")
    expected_input = {"value": 12, "known": True} if native == "anthropic" else {"value": 0, "known": False}
    require(events[-1]["usage"]["input"] == expected_input, "nonfinal observed input differs from native usage")
    require(report["runner"]["prepare_calls"] == 1 and not report["runner"]["other_operations"], "fragment reached runner")
    snapshots = report["snapshots"]
    phases = ["after_interrupted_driver_return", "new_lease_before_expiry", "natural_worker_lease_expired_request_still_live", "natural_request_deadline_expired_slot_only_released", "zero_cost_abandon_rejected"]
    require([s["phase"] for s in snapshots] == phases, "missing phases")
    require([instant(s["captured_at"]) for s in snapshots] == sorted(instant(s["captured_at"]) for s in snapshots), "snapshot timestamps unordered")
    original_attempt = snapshots[0]["attempts"][0]
    original_reservation = snapshots[0]["reservations"][0]
    original_run = snapshots[0]["run"]
    attempt_id = original_attempt["attempt_id"]
    tenant_id, run_id, step_seq = original_attempt["tenant_id"], report["run_id"], original_attempt["step_seq"]
    pricing = original_attempt["pricing"]
    require(original_attempt["run_id"] == run_id and original_run["tenant_id"] == tenant_id, "attempt is from another run or tenant")
    require(original_attempt["provider"] == pricing["provider"] == original_run["config_snapshot"]["provider"] == native, "native request and ledger provider differ")
    require(original_attempt["model_id"] == pricing["model"] == original_run["config_snapshot"]["model"] == wire["request"]["model"], "native request and ledger model differ")
    require(original_attempt["status"] == "failed" and original_attempt["error_code"] == "stream_interrupted" and original_attempt["attempt"] == 1, "attempt not incomplete")
    require(original_attempt["raw_ref"] is None and original_attempt["usage"] is None and original_attempt["failure_policy"]["retry"] is True, "complete turn/usage or missing durable retry policy")
    require(original_attempt["pricing"]["price_version"] == "synthetic-f03-v1", "wrong frozen price")
    require(original_attempt["pricing"]["context_tokens"] == 8192 and original_attempt["pricing"]["max_output_tokens"] == 1024, "wrong token bounds")
    require(original_attempt["price_version"] == pricing["price_version"] and pricing["input_price"] == 1_000_000 and pricing["output_price"] == 2_000_000, "frozen rate differs from reserved quote")
    require(wire["request"]["max_output_tokens" if native == "openai" else "max_tokens"] == pricing["max_output_tokens"], "native output bound differs from frozen quote")
    require(original_reservation["credential_group"] == pricing["credential_group"] == "f03-local", "reservation belongs to another credential group")
    quoted_cost = (pricing["context_tokens"] * pricing["input_price"] + 999_999) // 1_000_000 + (pricing["max_output_tokens"] * pricing["output_price"] + 999_999) // 1_000_000
    require(original_reservation["microusd"] == quoted_cost and original_reservation["tokens"] == pricing["context_tokens"] + pricing["max_output_tokens"], "reservation does not match frozen quote")
    require(original_reservation["id"] == attempt_id and original_reservation["run_id"] == report["run_id"] and original_reservation["tenant_id"] == original_attempt["tenant_id"], "reservation identity mismatch")
    require(instant(original_reservation["request_deadline"]) == instant(original_attempt["deadline"]), "request deadline changed")
    require(instant(original_reservation["dispatched_at"]) <= instant(wire["request_at"]), "HTTP dispatched before durable marker")
    require(instant(wire["closed_at"]) <= instant(adapter["finished_at"]) <= instant(snapshots[0]["captured_at"]), "adapter/ledger observation predates socket cut")
    if native == "openai":
        require(wire["attempt_header"] == attempt_id, "attempt header mismatch")
    require(all(e["attempt_id"] == attempt_id and e["run_id"] == run_id and e["step_id"] == str(step_seq) for e in events + [adapter["returned_turn"]]), "adapter identity mismatch")
    for i, snapshot in enumerate(snapshots):
        require(snapshot["attempts"] == [original_attempt], "attempt changed or retried")
        require(not snapshot["effects"], "effect was planned")
        require(len(snapshot["reservations"]) == len(snapshot["quotas"]) == 1, "extra ledger rows")
        reservation = snapshot["reservations"][0]
        expected = dict(original_reservation, request_slot_released=(i >= 3))
        require(reservation == expected, "unknown reservation changed beyond request slot")
        require(reservation["status"] == "unknown" and reservation["tokens"] == 9216 and reservation["microusd"] == 10240, "expense no longer retained")
        require(all(reservation.get(k) is None for k in ("settled_at", "actual_tokens", "actual_microusd", "settlement_kind")), "unknown became settled/actual")
        q = snapshot["quotas"][0]
        require(q["credential_group"] == reservation["credential_group"] and q["window_generation"] == reservation["window_generation"], "quota does not back this reservation")
        require((q["reserved_tokens"], q["reserved_microusd"], q["committed_tokens"], q["committed_microusd"], q["active_requests"]) == (9216, 10240, 0, 0, int(i < 3)), "provider quota refund or slot error")
        pg_events = snapshot["events"]
        require([e["seq"] for e in pg_events] == list(range(1, len(pg_events) + 1)), "durable event gap")
        require(all(e["run_id"] == run_id and e["tenant_id"] == tenant_id for e in pg_events), "event identity mismatch")
        require(sum(e["type"] == "model.started" for e in pg_events) == sum(e["type"] == "model.attempt_failed" for e in pg_events) == 1, "repeated/missing attempt")
        require(not any(e["type"] in ("model.completed", "tools.validated") or e["type"].startswith("effect.") for e in pg_events), "incomplete turn advanced")
        pg_text = [e["payload"] for e in pg_events if e["type"] == "text.delta"]
        require("".join(e["delta"] for e in pg_text) == text and all(e["provisional"] and e["attempt_id"] == attempt_id for e in pg_text), "provisional text not durable")
        started_event = next(e["payload"] for e in pg_events if e["type"] == "model.started")
        failed_event = next(e["payload"] for e in pg_events if e["type"] == "model.attempt_failed")
        require(started_event["attempt_id"] == failed_event["attempt_id"] == attempt_id and started_event["step_seq"] == step_seq and started_event["price_version"] == pricing["price_version"], "durable model events refer to another attempt")
        policy = original_attempt["failure_policy"]
        require(failed_event["error_code"] == policy["code"] == adapter["error_kind"] and failed_event["retry"] is policy["retry"] and failed_event["provisional"] is True and instant(failed_event["not_before"]) == instant(policy["not_before"]), "durable failure differs from adapter/persisted retry policy")
        require({a["kind"] for a in snapshot["artifacts"]} == {"baseline", "context"}, "completed response artifact present")
        require(all(a["tenant_id"] == tenant_id and a["run_id"] == run_id and a["state"] == "ready" for a in snapshot["artifacts"]), "artifact identity/state mismatch")
        require([a["id"] for a in snapshot["artifacts"] if a["kind"] == "context"] == [original_attempt["request_ref"]], "attempt input references another context")
        require(snapshot["run"]["id"] == report["run_id"] and snapshot["run"]["state"] == "running", "wrong live run")
        current = snapshot["run"]
        for key in ("tenant_id", "project_id", "principal_id", "parent_run_id", "task", "base_commit", "input_snapshot", "config_snapshot", "runner_id", "workspace_id", "created_at"):
            require(current[key] == original_run[key], "run identity/input changed: " + key)
        state = current["snapshot"]
        require(state["tenant_id"] == tenant_id and state["run_id"] == run_id and state["step_seq"] == step_seq and state["version"] == current["version"] and state["status"] == "running", "run snapshot identity mismatch")
        require(state["stage"] == ("model_step" if i == 0 else "adopt_workspace") and state["tool_calls"] == 0 and state["model_rounds"] == 1, "partial turn advanced or resampled")
    lease = report["natural_lease"]
    require(lease["epoch"] == 2 and lease["no_driver_or_heartbeat_started"] is True, "second claim conflated with resampling")
    require(original_run["lease_owner"] == "f03-first-worker" and original_run["lease_epoch"] == 1, "missing original worker claim")
    # Defer legitimately expires the first database lease without rewriting its
    # prior reducer snapshot. Only the new Claim must agree with its snapshot.
    for snapshot in snapshots[1:]:
        run, state_lease = snapshot["run"], snapshot["run"]["snapshot"]["lease"]
        require(run["lease_owner"] == state_lease["owner"] == "f03-no-heartbeat-worker", "summary replaced actual claim owner")
        require(run["lease_epoch"] == state_lease["epoch"] == lease["epoch"] == original_run["lease_epoch"] + 1, "summary replaced actual claim epoch")
        require(instant(run["lease_until"]) == instant(state_lease["until"]) == instant(lease["until"]), "summary replaced actual claim expiry")
    require(instant(snapshots[1]["captured_at"]) < instant(lease["until"]) <= instant(snapshots[2]["captured_at"]) < instant(lease["request_deadline"]) <= instant(snapshots[3]["captured_at"]), "lease/deadline phases not distinct natural windows")
    require(instant(lease["request_deadline"]) == instant(original_attempt["deadline"]), "different request deadline")
    require(report["early_request_slot_expirations"] == 0 and report["request_slot_expirations"] == [1, 0], "slot released early/multiple times")
    require(instant(original_attempt["failure_policy"]["not_before"]) <= instant(snapshots[1]["captured_at"]), "retry policy bypassed")
    hashes = report["manifest"]["source_sha256"]
    for source, digest in hashes.items():
        require(hashlib.sha256((directory / "source-snapshot" / (source + ".txt")).read_bytes()).hexdigest() == digest, "source hash mismatch: " + source)
    return {"passed": True, "native": native, "run_id": report["run_id"], "snapshots_checked": 5, "durable_event_counts": [len(s["events"]) for s in snapshots], "native_requests": 1, "failed_incomplete_attempts": 1, "effect_count": 0, "retained_tokens": 9216, "retained_microusd": 10240, "request_slot_expirations": [0, 1, 0], "source_hashes_checked": len(hashes), "binary_identity_retained": bool(report["manifest"].get("executing_binary_sha256")), "boundaries": "Actual local native HTTP/Driver/PG; synthetic provider bytes/rates, recording runner; no second model sample or Docker claim"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--output", type=Path, help="Write a new audit file; existing files are never replaced")
    args = parser.parse_args()
    directory = args.directory
    raw = (directory / "report.json").read_bytes()
    report = json.loads(raw)
    result = audit(directory, report)
    # Prove this independent oracle rejects three consequential false successes.
    for mutation in ("expense_refund", "second_attempt", "fragment_effect"):
        altered = copy.deepcopy(report)
        if mutation == "expense_refund":
            altered["snapshots"][-1]["quotas"][0]["reserved_microusd"] = 0
        elif mutation == "second_attempt":
            altered["adapter"]["calls"] = 2
        else:
            altered["snapshots"][-1]["effects"] = [{"kind": "apply_patch"}]
        try:
            audit(directory, altered)
        except AssertionError:
            pass
        else:
            raise AssertionError("mutated false success accepted: " + mutation)
    result["negative_oracle_controls"] = ["expense_refund", "second_attempt", "fragment_effect"]
    result["report_sha256"] = hashlib.sha256(raw).hexdigest()
    result["auditor_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    if args.output:
        with args.output.open("x") as stream:
            stream.write(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
