#!/usr/bin/env python3
"""Read-only, independent raw P06 recomputation; no Docker, database or live RPC.

Runs against attempt-02 by default. --write records first audit/manifest once;
later invocations verify them. Large executable files remain locally retained.
"""
import argparse
import base64
import calendar
import collections
import copy
import datetime
import hashlib
import ipaddress
import json
import math
import pathlib
import re
import subprocess


def sha(data):
    return hashlib.sha256(data).hexdigest()


def load(path):
    return json.loads(path.read_text())


def utc_ns(value):
    # Preserve all nine fractional digits; datetime alone truncates to micros.
    match = re.fullmatch(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d{1,9}))?(Z|[+-]\d\d:\d\d)", value)
    assert match, value
    date = datetime.datetime.fromisoformat(match[1] + match[3].replace("Z", "+00:00"))
    return calendar.timegm(date.utctimetuple()) * 10**9 + int((match[2] or "").ljust(9, "0"))


def ns(stamp):
    return stamp["since_experiment_start_ns"]


def canonical(value):
    # This fixture's args contain no floats. Match Go's canonical JSON escaping.
    value = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    for old, new in [("<", "\\u003c"), (">", "\\u003e"), ("&", "\\u0026"), ("\u2028", "\\u2028"), ("\u2029", "\\u2029")]:
        value = value.replace(old, new)
    return value.encode()


def intent(value):
    value = copy.deepcopy(value)
    assert value.pop("grant", "") == "", "bearer grant leaked"
    value["deadline"] = utc_ns(value["deadline"])
    assert sha(canonical(value["args"])) == value["args_hash"]
    return value


def net_facts(facts):
    network = facts["network"]
    devices = {d["name"]: d for d in network["interfaces"]}
    assert len(devices) == len(network["interfaces"])
    assert sorted(devices) == sorted(facts["net_devices"])
    assert any(d["flags"] & 8 and d["flags"] & 1 for d in devices.values())
    for device in devices.values():
        if device["flags"] & 8:
            assert device["flags"] & 1
            assert all(ipaddress.IPv4Address(a).is_loopback for a in device["ipv4"])
        else:
            assert not device["flags"] & 1 and device["operstate"] == "down" and not device["ipv4"]
    ipv4 = []
    for line in network["ipv4_route_table"].splitlines()[1:]:
        fields = line.split()
        assert len(fields) == 11
        decode = lambda s: str(ipaddress.IPv4Address(bytes.fromhex(s)[::-1]))
        ipv4.append(dict(interface=fields[0], destination=decode(fields[1]), gateway=decode(fields[2]), mask=decode(fields[7]), flags=int(fields[3], 16)))
    ipv6 = []
    for line in network["ipv6_route_table"].splitlines():
        fields = line.split()
        assert len(fields) == 10
        decode = lambda s: str(ipaddress.IPv6Address(bytes.fromhex(s)))
        ipv6.append(dict(interface=fields[9], destination=decode(fields[0]), prefix=int(fields[1], 16), next_hop=decode(fields[4]), flags=int(fields[8], 16)))
    assert ipv4 == network["ipv4_routes"] and ipv6 == network["ipv6_routes"]
    for route in ipv4 + ipv6:
        device = devices[route["interface"]]
        active = bool(route["flags"] & 1) and not route["flags"] & 0x200
        if not active:
            continue
        assert device["flags"] & 8
        if "mask" in route:
            subnet = ipaddress.IPv4Network((route["destination"], route["mask"]), strict=False)
            assert subnet.subnet_of(ipaddress.IPv4Network("127.0.0.0/8"))
            assert ipaddress.IPv4Address(route["gateway"]).is_unspecified
        else:
            assert route["destination"] == "::1" and route["prefix"] == 128 and route["next_hop"] == "::"
    return network


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--directory", type=pathlib.Path, default=pathlib.Path(__file__).parent / "attempt-02")
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    root = args.directory.resolve()
    report = load(root / "report.json")
    assert report["passed"] and report["ProcessCommands"] == 20 and report["RoundsPerMode"] == 5
    assert len(report["Samples"]) == 20 and len(report["Pairs"]) == 10 and len(report["Workspaces"]) == 2
    assert report["RunnerConfigArgumentMatches"]
    assert report["GRPCConnectionPeers"]
    assert all(p["PID"] == report["RunnerPID"] and p["MatchesRunnerPID"] for p in report["GRPCConnectionPeers"])
    assert sha((root / "command.py").read_bytes()) == report["ProgramSHA256"]
    for name, digest in report["Sources"].items():
        assert sha((root / "source" / (name + ".txt")).read_bytes()) == digest
    assert report["Sources"]["internal/runnerclient/execution_benchmark_test.go"] == "eb820cece4bc79dda4c5fd259b69c42fd10f2c1e9a15a68b4c256d346e4fabf2"
    binaries = load(root / "retained-local-executables.json")
    for binary in binaries["executables"]:
        data = pathlib.Path(binary["path"]).read_bytes()
        assert len(data) == binary["bytes"] and sha(data) == binary["sha256"]
        key = "TestBinarySHA256" if binary["name"] == "runnerclient.test" else "RunnerExecutableSHA256"
        assert binary["sha256"] == report[key]
    build = root.parent.parent / "runner-execution-build-20260911-attempt2"
    identity = load(build / "identity.json")
    before, after = load(build / "source-before.json"), load(build / "source-after.json")
    assert before == after and len(before) == identity["source_files"] == 267
    assert identity["binary_sha256"] == report["TestBinarySHA256"]
    assert load(build / "execution.json")["exit_code"] == 0 and load(build / "execution.json")["source_and_binary_unchanged"]
    assert identity["base_commit"] == "a0b3a5da4c09181cf600e7e4f54d83df93387282"
    # Reconstruct every fixed build input from Git, except the retained harness.
    repo = root.parents[3]
    for name, digest in before.items():
        if name == "internal/runnerclient/execution_benchmark_test.go":
            assert digest == report["Sources"][name]
        else:
            data = subprocess.check_output(["git", "show", identity["base_commit"] + ":" + name], cwd=repo)
            assert sha(data) == digest, name
    object_keys = set()

    def artifact(ref, expected_kind, tenant, run):
        assert (ref["kind"], ref["tenant_id"], ref["run_id"]) == (expected_kind, tenant, run)
        assert ref["object_key"] == "/".join([tenant, run, ref["sha256"]])
        suffix = ".snapshot" if expected_kind == "workspace_snapshot" else ".json"
        data = (root / "objects" / (ref["sha256"] + suffix)).read_bytes()
        assert len(data) == ref["size"] and sha(data) == ref["sha256"]
        object_keys.add(ref["sha256"] + suffix)
        return json.loads(data)

    def operation(op, expected):
        assert intent(op["request"]) == intent(expected)
        req = op["request"]
        stored = artifact(op["receipt"], "operation_receipt", req["tenant_id"], req["run_id"])
        advertised = copy.deepcopy(op)
        advertised.pop("receipt")
        self_ref = stored.pop("receipt")
        assert not any(self_ref.values()), "receipt self-reference must be empty"
        assert advertised == stored
        assert load(root / "operations" / (req["operation_id"] + ".json")) == op

    samples = {s["ID"]: s for s in report["Samples"]}
    assert len(samples) == 20
    docker_ids, networks, normal_checks = set(), [], []
    measures = collections.defaultdict(list)
    last_docker = {}
    workspaces = {w["ID"]: w for w in report["Workspaces"]}
    for sample in samples.values():
        req, final = sample["Request"], sample["Final"]
        assert req["tenant_id"] == "p06-execution" and req["run_id"] == req["workspace_id"] == sample["Workspace"]
        assert req["workspace_id"] in workspaces and req["epoch"] == 1
        assert req["operation_id"] == sample["ID"] and req["kind"] == "run_command"
        assert req["args"]["command"] == ["python", "-I", "-B", "-c", (root / "command.py").read_text(), sample["Marker"], sample["Mode"]]
        assert sample["StartAck"] == sample["StartReturn"] and sample["StartStatus"] == "prepared"
        operation(final, req)
        assert sample["ReceiptFile"] == "objects/" + final["receipt"]["sha256"] + ".json"
        assert sample["TypedFile"] == "operations/" + sample["ID"] + ".json"
        job = final["result"]
        assert job["started"] and not job["running"] and not job["truncated"] and not final.get("result_truncated", False)
        assert job["id"] == final["job_id"]
        output = base64.b64decode(job["output"], validate=True).decode()
        assert "P06_ISOLATION_FAILURE" not in output
        lines = output.splitlines()
        ready = [json.loads(line.removeprefix("P06_READY ")) for line in lines if line.startswith("P06_READY ")]
        assert len(ready) == 1
        facts = ready[0]
        assert facts["marker"] == sample["Marker"] and facts["uid"] == facts["gid"] == 1000
        assert facts["memory_max"] == "268435456" and facts["pids_max"] == "64"
        assert facts["cpu_max"] == "100000 100000" and int(facts["cap_eff"], 16) == 0 and facts["no_new_privs"] == "1"
        assert not facts["docker_socket"] and facts["root_readonly"] and facts["tmp_bytes"] == 67108864
        networks.append(net_facts(facts))
        observations = sample["Observations"]
        docker = [o for o in observations if o.get("Docker")]
        assert docker
        for ob in observations:
            assert ns(ob["End"]) >= ns(ob["Begin"])
        for ob in docker:
            d = ob["Docker"]
            assert d["Name"].lstrip("/") == final["job_id"] and not d["State"]["OOMKilled"]
            assert d["Config"]["Image"] == report["Profile"]["image"] and d["Config"]["User"] == "1000:1000"
            h = d["HostConfig"]
            assert h["NetworkMode"] == "none" and h["ReadonlyRootfs"] and h["CapDrop"] == ["ALL"]
            assert h["Memory"] == h["MemorySwap"] == 268435456 and h["PidsLimit"] == 64 and h["NanoCpus"] == 10**9
            assert "no-new-privileges" in h["SecurityOpt"] and "noexec" in h["Tmpfs"]["/tmp"]
        assert len({o["Docker"]["ID"] for o in docker}) == 1
        final_d = docker[-1]["Docker"]
        assert not final_d["State"]["Running"]
        last_docker[sample["ID"]] = final_d
        docker_ids.add(final_d["ID"])
        running = [o for o in docker if o["Docker"]["State"]["Running"]]
        assert running[0]["End"] == sample["FirstRunningObservation"]
        stopped = [o for o in docker if not o["Docker"]["State"]["Running"] and not o["Docker"]["State"]["FinishedAt"].startswith("0001")]
        assert stopped[0]["End"] == sample["FirstStoppedObservation"]
        active_logs = [o for o in observations if o["Kind"] == "daemon_logs_while_running" and "P06_READY " in o.get("Output", "")]
        assert active_logs and active_logs[0]["End"] == sample["FirstDaemonLog"]
        first_log = active_logs[0]
        assert any(ns(o["End"]) <= ns(first_log["Begin"]) for o in running)
        assert any(o["Kind"] == "docker_running_after_first_log" and o["Docker"]["State"]["Running"] and ns(o["Begin"]) >= ns(first_log["End"]) for o in docker)
        typed_logs = [o for o in observations if o["Kind"] == "typed_inspect" and "P06_READY " in o.get("Output", "")]
        assert typed_logs and typed_logs[0]["End"] == sample["FirstTypedInspectLog"]
        assert typed_logs[0]["Status"] in ["succeeded", "cancelled"]
        read = sample["ReadOperation"]
        read_req = read["request"]
        assert all(read_req[k] == req[k] for k in ["tenant_id", "run_id", "workspace_id", "epoch", "policy_version"])
        assert read_req["operation_id"] == sample["ID"] + "-read" and read_req["kind"] == "read_file"
        assert read_req["args"] == {"path": "p06-shared-name.txt"} and read_req["expected_revision"] == final["after_revision"]
        operation(read, read_req)
        assert read["status"] == "succeeded" and read["result"]["content"] == sample["Marker"] == sample["VerifiedContent"]
        assert sha(sample["Marker"].encode()) == read["result"]["sha256"]
        assert read["before_hash"] == read["after_hash"] == final["after_hash"] and read["after_revision"] == final["after_revision"]
        assert final["after_revision"] == req["expected_revision"] + 1
        if sample["Mode"] == "normal":
            assert final["status"] == "succeeded" and job["exit_code"] == 0 and sample["CancelReply"] is None
            done = [json.loads(line.removeprefix("P06_DONE ")) for line in lines if line.startswith("P06_DONE ")]
            assert len(done) == 1 and done[0]["marker"] == sample["Marker"] and done[0]["checks"] > 0
            normal_checks.append(done[0]["checks"])
        else:
            assert sample["Mode"] == "cancel" and final["status"] == "cancelled" and job["exit_code"] == 137
            assert sample["ChildDaemonLogObserved"] and "P06_CHILD " + sample["Marker"] in lines
            assert intent(sample["CancelReply"]["request"]) == intent(req) and sample["CancelReply"]["status"] == "cancelled"
            assert sample["CancelReply"] == final
            assert ns(sample["CancelBegin"]) > ns(sample["FirstDaemonLog"])
            assert len(sample["BeforeTasks"].split()) >= 3 and not sample["AfterTasks"].strip() and sample["CgroupRemoved"]
            assert final_d["ID"] in sample["CgroupPath"]
            assert ns(sample["TasksAfterCancelAt"]) >= ns(sample["FirstStoppedObservation"])
            top = [o for o in observations if o["Kind"] == "docker_top_before_cancel"]
            assert len(top) == 1 and ns(top[0]["End"]) < ns(sample["CancelBegin"])
            tasks = [line.split() for line in top[0]["Output"].splitlines()[1:]]
            assert len(tasks) == 3 and {row[0] for row in tasks} == set(sample["BeforeTasks"].split())
            python = [row for row in tasks if row[2] == "python"]
            assert len(python) == 2 and any(child[1] == parent[0] for child in python for parent in python if child is not parent)
        intervals = [("direct_start_ack_ms", "StartBegin", "StartAck"), ("start_to_first_observed_running_upper_bound_ms", "StartBegin", "FirstRunningObservation"), ("start_to_active_daemon_log_ms", "StartBegin", "FirstDaemonLog"), ("start_to_terminal_typed_inspect_log_ms", "StartBegin", "FirstTypedInspectLog")]
        if sample["Mode"] == "cancel":
            intervals += [("cancel_rpc_ack_ms", "CancelBegin", "CancelAck"), ("cancel_to_first_observed_stopped_upper_bound_ms", "CancelBegin", "FirstStoppedObservation")]
        for label, start, end in intervals:
            delta = ns(sample[end]) - ns(sample[start])
            assert delta >= 0
            measures[sample["Mode"] + "/" + label].append(delta / 10**6)
    assert len(docker_ids) == 20
    pairs = []
    for pair in report["Pairs"]:
        a, b = [samples[s] for s in pair["IDs"]]
        assert a["Mode"] == b["Mode"] == pair["Mode"] and a["Round"] == b["Round"] == pair["Round"]
        da, db = [last_docker[s["ID"]] for s in (a, b)]
        mount = lambda d: [m["Source"] for m in d["Mounts"] if m["Destination"] == "/workspace" and m["RW"]]
        assert len(mount(da)) == len(mount(db)) == 1 and mount(da) != mount(db)
        start = max(utc_ns(d["State"]["StartedAt"]) for d in (da, db))
        finish = min(utc_ns(d["State"]["FinishedAt"]) for d in (da, db))
        overlap = finish - start
        assert overlap > 0 and overlap == pair["DaemonOverlapNS"] and pair["BothRunning"] and pair["DistinctMountSources"]
        detail = {"mode": pair["Mode"], "round": pair["Round"], "ids": pair["IDs"], "daemon_overlap_ns": overlap}
        if pair["Mode"] == "cancel":
            peer = pair["BObservationAfterAStopped"]
            assert pair["BRunningAfterAStopped"] and peer["Docker"]["ID"] == db["ID"] and peer["Docker"]["State"]["Running"]
            assert ns(peer["Begin"]) > ns(a["TasksAfterCancelAt"]) >= ns(a["FirstStoppedObservation"])
            assert ns(b["CancelBegin"]) >= ns(peer["End"])
            detail.update(a_cgroup_empty_ns=ns(a["TasksAfterCancelAt"]), peer_running_begin_ns=ns(peer["Begin"]), peer_running_end_ns=ns(peer["End"]), b_cancel_begin_ns=ns(b["CancelBegin"]))
        pairs.append(detail)
    for workspace in workspaces.values():
        assert not workspace["Unknown"]
        stop, snapshot = workspace["Stop"], workspace["Snapshot"]
        for value in [stop["workspace"], snapshot["workspace"]]:
            assert all(value[k] == workspace["Prepared"][k] for k in ["tenant_id", "run_id", "id", "epoch"])
            assert value["stopped"] and not value.get("active_operation") and value["revision"] == 11
        assert stop["no_active_operations"] and stop["workspace"] == snapshot["workspace"]
        stored = artifact(stop["ref"], "workspace_stop", "p06-execution", workspace["ID"])
        assert stored["workspace"] == stop["workspace"] and stored["no_active_operations"] and not any(stored["ref"].values())
        stored = artifact(snapshot["artifact"], "workspace_snapshot", "p06-execution", workspace["ID"])
        assert stored["workspace"] == snapshot["workspace"]
        lines = []
        for name, file in sorted(stored["files"].items()):
            assert sha(base64.b64decode(file["content"], validate=True)) == file["sha256"]
            lines.append(f"{len(name.encode())}:{name}:{file['sha256']}:{str(file['executable']).lower()}\n")
        assert sha("".join(lines).encode()) == snapshot["hash"]
        assert workspace["Released"] == {"workspace_id": workspace["ID"], "released": True}
        owned = [s for s in samples.values() if s["Workspace"] == workspace["ID"]]
        assert sorted(s["Request"]["expected_revision"] for s in owned) == list(range(1, 11))
        assert base64.b64decode(stored["files"]["p06-shared-name.txt"]["content"]).decode() == next(s["Marker"] for s in owned if s["Mode"] == "cancel" and s["Round"] == 4)
    assert object_keys == {p.name for p in (root / "objects").iterdir()} and len(object_keys) == 44
    assert len(list((root / "operations").iterdir())) == 40
    distributions = {}
    for key, values in sorted(measures.items()):
        values.sort()
        def quantile(q):
            at = q * (len(values) - 1)
            lo, hi = math.floor(at), math.ceil(at)
            return values[lo] + (values[hi] - values[lo]) * (at - lo)
        calculated = dict(Count=len(values), MinMS=values[0], P50MS=quantile(.5), P95MS=quantile(.95), MaxMS=values[-1])
        assert calculated["Count"] == 10
        assert all(math.isclose(value, report["Distributions"][key][name], abs_tol=1e-9) for name, value in calculated.items())
        distributions[key] = calculated
    assert set(distributions) == set(report["Distributions"])
    summary = dict(passed=True, scope="Independent Python recomputation of the single observed execution; not a second OS observation or independent implementation review.", artifacts=44, typed_operations=40, process_commands=20, unique_containers=20, normal_commands=10, cancelled_commands=10, workspaces_released=2, normal_check_counts=normal_checks, build_inputs=267, runner_pid=report["RunnerPID"], runner_sha256=report["RunnerExecutableSHA256"], test_binary_sha256=report["TestBinarySHA256"], uds_peers=report["GRPCConnectionPeers"], harness_duration_ns=utc_ns(report["EndedUTC"])-utc_ns(report["StartedUTC"]), wrapper_duration_seconds=load(build/"execution.json")["elapsed_seconds"], unique_network_facts=list({json.dumps(n, sort_keys=True) for n in networks}), pairs=pairs, distributions=distributions, limitations=["Start replies are not separately retained in full; direct Start path and intent check are source-reviewed, final/Cancel/typed/receipt intents are recomputed.", "Stop/Snapshot/Release replies and frozen code order are retained; separate cleanup RPC timestamps are not captured.", "Runner before/after process identity checks are source-enforced; the report retains final identity and actual SO_PEERCRED samples, not two independent process snapshots.", "No PostgreSQL, model, paid API, restart, stress-test repetition, network packet probe, or global pool audit."])
    # A set above is stable here (all 20 inventories match); sort if extended.
    summary["unique_network_facts"].sort()
    audit_path = root / "audit.json"
    if audit_path.exists():
        assert load(audit_path) == summary, "stored audit changed"
    elif args.write:
        audit_path.write_text(json.dumps(summary, indent=2) + "\n")
    else:
        raise AssertionError("initial audit missing; use --write once")
    manifest = {str(p.relative_to(root)): dict(bytes=p.stat().st_size, sha256=sha(p.read_bytes())) for p in sorted(root.rglob("*")) if p.is_file() and p.name != "manifest.json"}
    manifest_path = root / "manifest.json"
    if manifest_path.exists():
        assert load(manifest_path) == manifest, "manifest mismatch"
    elif args.write:
        manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    else:
        raise AssertionError("initial manifest missing")
    print(json.dumps(dict(passed=True, artifacts=44, typed_operations=40, process_commands=20, pairs=10, manifest_files=len(manifest), build_inputs=267)))


if __name__ == "__main__":
    main()
