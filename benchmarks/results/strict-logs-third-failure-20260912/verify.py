"""Verify the portable historical archive. No network, subprocess or runtime access."""
from pathlib import Path
import base64
import datetime
import hashlib
import json
import struct
import tarfile
import zlib

ROOT = Path(__file__).resolve().parent
sha = lambda data: hashlib.sha256(data).hexdigest()
canonical = lambda value: json.dumps(value, sort_keys=True, separators=(",", ":"))
instant = lambda text: datetime.datetime.fromisoformat(text.replace("Z", "+00:00"))
checks = []


def check(condition, label):
    if not condition:
        raise AssertionError(label)
    checks.append(label)


index = json.loads((ROOT / "archive-manifest.json").read_bytes())
archive = ROOT / "raw-evidence.tar.gz"
check(sha(archive.read_bytes()) == index["tar_sha256"], "tar hash")
by_member = {row["archived_member"]: row for row in index["files"] if "archived_member" in row}
tf = tarfile.open(archive, "r:gz")
members = tf.getmembers()
check(all(member.isfile() and not member.name.startswith("/") and ".." not in member.name.split("/") for member in members), "only safe regular members")
check(len(members) == len(by_member) == 613 and {member.name for member in members} == set(by_member), "exact archive inventory")

# Keep the largest individual JSON only; the full uncompressed archive is 505 MB.
for row in index["files"]:
    name = row.get("archived_member", row.get("archived_file"))
    data = tf.extractfile(name).read() if "archived_member" in row else (ROOT / name).read_bytes()
    check(sha(data) == row["archived_sha256"] and len(data) == row["archived_bytes"], "archived bytes " + name)
    if row["projection"] is None:
        check(row["source_sha256"] == row["archived_sha256"] and row["source_bytes"] == row["archived_bytes"], "byte-identical source " + name)
    else:
        value = json.loads(data)
        check(name in {"raw/logs-03/L1/fixture.json", "raw/logs-03/L4/fixture.json"}, "only the two declared fixture projections")
        check("worker_config_sha256" not in value and value["_archive_projection"] == row["projection"], "explicit projection marker " + name)
        check(row["projection"]["removed_fields"] == ["worker_config_sha256"] and row["projection"]["original_file_sha256"] == row["source_sha256"], "enclosing original file binding " + name)

raw = lambda name: tf.extractfile("raw/" + name).read()
j = lambda name: json.loads(raw(name))
for prefix, count in [("logs-03/", 602), ("logs-03-retained-observation/", 6)]:
    manifest = j(prefix + "manifest.json")
    check(len(manifest) == count and set(manifest) | {"manifest.json"} == {name.removeprefix("raw/" + prefix) for name in by_member if name.startswith("raw/" + prefix)}, "original exact manifest " + prefix)
    check(sha(raw(prefix + "manifest.json")) == index["original_manifests"][prefix[:-1]]["sha256"], "original manifest unchanged " + prefix)
    for name, expected in manifest.items():
        check(by_member["raw/" + prefix + name]["source_sha256"] == expected, "original manifest source hash " + prefix + name)

C = "logs-03/"
O = "logs-03-retained-observation/"
acceptance, pre, fixture = j(C + "acceptance.json"), j(C + "preflight.json"), j(C + "L4/fixture.json")
host, host_intent = j("host-logs-03/result.json"), j("host-logs-03/intent.json")
check(acceptance["passed"] is False and set(acceptance["cases"]) == {"L1", "L2-L3-default", "L3-bytes", "L3-count", "L5"} and all(case["passed"] is True for case in acceptance["cases"].values()), "original failure and five recorded passed cases")
check(acceptance["input_sha256_before"] == acceptance["input_sha256_after"], "frozen inputs unchanged")
check(host["exit_code"] == 1 and host["unit"] == host_intent["unit"] and host["after"]["LoadState"] == "not-found" and host["after"]["MainPID"] == "0", "host failure and own unit exit")
check("bounded wait failed: actual spool write failure" in raw("host-logs-03/execution.log").decode() and "FAIL: TestStrictLogsCombinedAcceptance (77.14s)" in raw("host-logs-03/execution.log").decode(), "original test failure and duration")
pins = pre["acceptance"]
source_revision = "bdff9b1b511ae1c38327b8441ac973bb4db262e2"
check(all(Path(pins[key]).parent.name == source_revision for key in ["test_binary", "runner_binary", "worker_binary"]), "executed versioned binary paths")
check(pins["test_sha256"] == "fc4937bf5ad4e7a00cb55f16272e47bb9fd11f6223a0cced896982c3cab3c041", "test binary recorded SHA")
for kind in ["test", "runner", "worker"]:
    check(acceptance["input_sha256_before"][pins[kind + "_binary"]] == pins[kind + "_sha256"], "binary hash bound to before/after inputs " + kind)

before, after = j(C + "L4-before-pressure-journal.json"), j(O + "journal.json")
observation, pressure, pause = j(O + "report.json"), j(C + "L4-pressure.json"), j(C + "L4-worker-pause.json")
operation_id, run_id = fixture["target_operation"], fixture["run_id"]
check(operation_id == run_id + "_step_1_op_0" and run_id == "run_DKT2OOLEVCKYXHW5NGNS7BKBHJ", "exact L4 intent")
check(before["identity"] == after["identity"] == pre["journal"]["identity"] == "b010dd7440a54c58601967342f397faf686b7e306061af9f49809ee342b0ec8e" and before["version"] == after["version"] == 5, "same journal identity and schema")
old_operation = next(row for row in before["tables"]["operations"] if row["id"] == operation_id)
new_operation = next(row for row in after["tables"]["operations"] if row["id"] == operation_id)
request = json.loads(old_operation["request_json"])
check(old_operation["status"] == "running" and old_operation["error"] == "" and new_operation["status"] == observation["operation_status"] == "unknown" and new_operation["error"] == observation["operation_error"] == "log_capture_incomplete", "running to unknown incomplete capture")
check(old_operation["dispatch_started"] == old_operation["docker_start_intent"] == 1 and old_operation["cancel_requested"] == new_operation["cancel_requested"] == 0, "one original dispatch and no journal Cancel")
check(new_operation["result_json"] == new_operation["receipt_json"] == "{}", "unsettled outcome preserved")


def normalized(table, rows):
    result = []
    for original in rows:
        row = dict(original)
        if table == "operations":
            request_json = json.loads(row["request_json"])
            check(request_json.pop("grant", "") == "", "no retained capability in journal")
            row["request_json"] = canonical(request_json)
        result.append(row)
    return sorted(result, key=canonical)


check(set(before["tables"]) == set(after["tables"]), "same captured authority tables")
for table in before["tables"]:
    old_rows = normalized(table, before["tables"][table])
    new_rows = normalized(table, after["tables"][table])
    if table == "operations":
        row = next(row for row in old_rows if row["id"] == operation_id)
        row["status"], row["error"] = "unknown", "log_capture_incomplete"
        old_rows.sort(key=canonical)
    check(old_rows == new_rows, "only target status/error changed: " + table)
workspace = next(row for row in after["tables"]["workspaces"] if row["id"] == run_id)
lease = next(row for row in after["tables"]["volume_leases"] if row["workspace_id"] == run_id)
check(workspace["epoch"] == request["epoch"] == pause["lease_epoch"] == 2 and workspace["released"] == workspace["stopped"] == lease["released"] == 0 and workspace["active_operation"] == operation_id, "old workspace still allocated and not sealed")
check(lease["slot_id"] == "slot-001" and len(after["tables"]["volume_slots"]) == 4, "exact fixed pool and retained slot")
log_row = next(row for row in after["tables"]["operation_logs"] if row["operation_id"] == operation_id)
check(log_row["cleanup_state"] == "retained", "unresolved log container remains retained")

check("no space left on device" in pressure["errno"] and pressure["sync_error"] == "<nil>" and pressure["written_bytes"] == 228589568, "large pressure write ENOSPC and Sync")
available_before = pressure["after"]["Bavail"] * pressure["after"]["Bsize"]
check(available_before == 430080 and observation["pressure"]["available_bytes"] == 364544, "positive available bytes at both historical samples")
for key in ["path", "device", "inode", "allocated_bytes"]:
    check(pressure[key] == observation["pressure"][key], "same retained pressure file " + key)
check(pressure["written_bytes"] == observation["pressure"]["bytes"] and pressure["effective_uid"] == pressure["owner_uid"] == 0 and observation["pressure"]["uid"] == 1000 and pre["uid_map"].splitlines()[0].split() == ["0", "1000", "1"], "pressure bytes and mapped/host ownership")
pause_seconds = (instant(pause["continued_at"]) - instant(pause["stop_sent_at"])).total_seconds()
remaining = (instant(pause["lease_until"]) - instant(pause["continued_at"])).total_seconds()
check(pause["watchdog"] is False and 0 < pause_seconds < 18 and remaining > 0, "pause within watchdog and lease")

data = raw(O + operation_id + ".spool")
metadata = j(O + operation_id + ".spool.meta")
summary = metadata["summary"]
position = sequence = 0
stream_payloads = {1: bytearray(), 2: bytearray()}
interleaved = bytearray()
while position < len(data):
    header = data[position:position + 32]
    check(len(header) == 32 and header[:4] == b"FLG1" and header[4] in stream_payloads and struct.unpack(">Q", header[8:16])[0] == sequence, "frame header " + str(sequence))
    size = struct.unpack(">I", header[16:20])[0]
    payload = data[position + 32:position + 32 + size]
    check(len(payload) == size and (zlib.crc32(payload) & 0xffffffff) == struct.unpack(">I", header[20:24])[0], "frame payload/CRC " + str(sequence))
    stream_payloads[header[4]].extend(payload)
    interleaved.extend(payload)
    position += 32 + size
    sequence += 1
check(position == len(data) == 29206 and sequence == 184 and len(interleaved) == 23318, "complete valid retained frame prefix")
check(stream_payloads[1] == b"SL_STDOUT\x00\xff" + b"X" * (91 * 128) and stream_payloads[2] == b"SL_STDERR\x00\xfe" + b"Y" * (91 * 128), "exact stream prefixes and emitted iteration count")
check(base64.b64decode(metadata["preview"], validate=True) == interleaved and metadata["data_hash"] == sha(data), "preview and spool content hashes")
check(summary["binding_hash"] == sha(old_operation["request_json"].encode()) and summary["operation_id"] == operation_id, "exact original request binding including empty grant and UTC deadline")
check(summary["retained_bytes"] == len(data) and summary["retained_payload"] == len(interleaved) and summary["records"] == sequence and summary["stdout_seen"] == summary["stderr_seen"] == 11659, "metadata counters match raw frames")
check(summary["policy"] == {"entry_bytes": 16384, "operation_bytes": 524288, "run_bytes": 16777216, "max_operations": 32, "preview_bytes": 65536}, "unchanged default log limits")
check(summary["reason"] == "runner_shutdown" and summary["complete"] is False and summary["truncated"] is True and summary["dropped_known"] is False and summary["termination_requested"] is False and summary["termination_observed"] is False, "shutdown gap, no spool-I/O or policy-stop proof")
for name, description in observation["files"].items():
    check(sha(raw(O + name)) == description["sha256"] and len(raw(O + name)) == description["bytes"] and description["device"] == pressure["device"], "exact same-volume captured file " + name)

docker = j(O + "docker-inspect.json")
check(len(docker) == 1, "single exact container inspected")
docker = docker[0]
check(docker["Id"] == log_row["container_id"] == observation["container_id"] and docker["Name"] == "/" + old_operation["job_id"] and docker["Config"]["Labels"]["forge.operation_id"] == operation_id, "Docker and journal intent identity")
check([docker["Path"]] + docker["Args"] == request["args"]["command"] and docker["State"] == observation["docker_state"], "actual Docker command and recorded state")
check(docker["State"]["ExitCode"] == 0 and docker["State"]["Running"] is False and docker["State"]["OOMKilled"] is False and docker["State"]["Pid"] == 0, "natural zero exit, no OOM")
runner_exit = j(C + "runner-08-stop.json")
check(runner_exit["exit_code"] == 0 and runner_exit["exe_sha256"] == pins["runner_sha256"] and pause["identity"]["exe_sha256"] == pins["worker_sha256"], "recorded process binary identities")
natural_seconds = (instant(docker["State"]["FinishedAt"]) - instant(docker["State"]["StartedAt"])).total_seconds()
after_runner_seconds = (instant(docker["State"]["FinishedAt"]) - instant(runner_exit["exited_at"])).total_seconds()
check(natural_seconds >= 23 and after_runner_seconds > 0 and instant(docker["State"]["FinishedAt"]) < instant(request["deadline"]), "natural program duration; exit after runner shutdown and before command deadline")

postgres = j(O + "postgres.json")
check(postgres["schema"] == j(C + "private-schema.json")["schema"] == "appfault_strictlogs_zg4ub4tnaruvpaojcj5dmmeqpc" and int(postgres["schema_oid"]) == 852843 and postgres["database"] == "forge" and postgres["current_user"] == "forge_admin" and postgres["superuser"] is True, "recorded read-only admin sample namespace/OID")
check(len(postgres["runs"]) == 2, "only this failed suite's two API runs")
run = next(row for row in postgres["runs"] if row["id"] == run_id)
snapshot, pending = run["snapshot"], run["snapshot"]["pending_effect"]
check(run["tenant"] == request["tenant_id"] == fixture["tenant"] and run["workspace"] == run_id and run["epoch"] == request["epoch"] == snapshot["lease"]["epoch"] == 2 and run["owner"] == pause["lease_owner"], "same PG tenant/run/workspace/epoch/owner")
check(run["state"] == snapshot["status"] == "running" and run["version"] == snapshot["version"] == 9 and snapshot["stage"] == "execute_effects", "unreconciled current run snapshot")
check(pending["operation_id"] == operation_id and pending["args"] == request["args"] and pending["args_hash"] == request["args_hash"] and pending["dispatch_epoch"] == request["epoch"] and pending["expected_revision"] == request["expected_revision"] and pending["status"] == "in_flight", "pending effect exact immutable intent")
check(instant(snapshot["lease"]["until"]) == instant(pause["lease_until"]) and instant(snapshot["limits"]["deadline"]) == instant(request["deadline"]), "lease and command deadline match across representations")
check(instant(postgres["now"]) > instant(pause["lease_until"]) and instant(postgres["now"]) > instant(request["deadline"]), "both expired by this later observation, not by paused interval")
check(all(postgres[key] == 1 for key in ["active", "runner_reserved", "allocations", "unsettled_effects"]) and all(postgres[key] == 0 for key in ["requests", "unsettled_reservations", "cleanup"]), "retained allocation and pending effect, no active request or cleanup")
check(postgres["attempts"] == 4 and postgres["artifacts"] == 34 and snapshot["cost_microusd"] == 150 and snapshot["model_rounds"] == snapshot["tool_calls"] == 1, "actual recorded finite synthetic work")
check(len(postgres["quotas"]) == 1 and postgres["quotas"][0]["credential_group"] == "application-faults-fake" and postgres["quotas"][0]["committed_microusd"] == 570 and postgres["quotas"][0]["reserved_microusd"] == 0, "synthetic committed charges retained")
check(observation["unit"] == host["unit"] and observation["unit_state"]["LoadState"] == "not-found" and observation["unit_state"]["MainPID"] == "0", "later own unit absent")
for suffix in ["forge-observe-l4-retained.py", "isolated_state.py"]:
    expected = next(value for path, value in observation["source_hashes"].items() if path.endswith("/" + suffix))
    check(sha((ROOT / ("observed-source-" + suffix + ".txt")).read_bytes()) == expected, "exact execution-recorded observer source " + suffix)
initial = json.loads((ROOT / "initial-static-review.json").read_bytes())
for path, expected in initial["source_sha256"].items():
    check(sha((ROOT / (path.replace("/", "__") + ".txt")).read_bytes()) == expected, "executed Git source excerpt " + path)
tf.close()
print(json.dumps({
    "archive_audit_passed": True,
    "checks": len(checks),
    "original_suite_passed": False,
    "original_manifest_members": 602,
    "observation_manifest_members": 6,
    "raw_members": 613,
    "projected_members": 2,
    "passed_case_records": sorted(acceptance["cases"]),
    "not_completed": "L4",
    "executed_source_revision": source_revision,
    "observation_at": observation["observed_at"],
    "spool": {"bytes": len(data), "records": sequence, "payload_bytes": len(interleaved), "each_stream_payload": 11659, "torn_tail": False, "reason": summary["reason"], "sha256": sha(data)},
    "pressure": {"available_bytes_after_large_write_failure": available_before, "available_bytes_at_later_observation": observation["pressure"]["available_bytes"], "same_file_retained": True},
    "process": {"natural_command_seconds": natural_seconds, "container_exit_after_runner_seconds": after_runner_seconds, "container_exit_code": 0, "oom": False, "pause_seconds": pause_seconds, "lease_remaining_at_resume_seconds": remaining},
    "diagnosis": "The 1 MiB pressure-write ENOSPC left 430080 B available. Actual small spool writes succeeded; the captured prefix ended on runner shutdown, and the original command naturally exited 0 later. No actual spool I/O failure or log-policy stop was established.",
    "cleanup_constraints": ["Exact target operation remains unknown/log_capture_incomplete with no result/receipt; Docker exit 0 does not make its verification/log complete.", "Reconcile the same operation and preserve truthful incomplete logs, durable receipt/publication and charged synthetic usage before Stop/seal/release. Do not infer release from expired SQL lease or dead container.", "The later SQL capture contains current run snapshots and aggregate ledger counts; it is not a full individual effect/attempt/allocation/event history. Cleanup must capture and compare exact relevant rows before and after.", "Original aggregate false and its manifest remain unchanged. A separately frozen full targeted L4 can be explicitly composed with the five existing case records only after old retained work is safely settled and identity/config compatibility is checked."],
    "scope": "Portable offline recomputation of historical raw only, with exact recorded UTC timestamps. No current live-state assertion, no resource/credential/model calls, no acceptance status promotion."
}, indent=2, sort_keys=True))
