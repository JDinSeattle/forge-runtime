#!/usr/bin/env python3
"""Exercise the actual CLI/API/worker/runner chain; no provider credentials."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
import uuid

parser = argparse.ArgumentParser()
parser.add_argument("--fixture", choices=["clamp", "expiry", "ranges", "go-ceil-div", "all"], default="all")
parser.add_argument("--resume", type=Path, help="resume artifact collection in an existing evidence directory")
args = parser.parse_args()
repo = Path(__file__).resolve().parent.parent
state = repo / "var/local"
env = os.environ.copy()
for line in (state / "client.env").read_text().splitlines():
    key, value = line.split("=", 1)
    if key not in {"FORGE_API_URL", "FORGE_TOKEN", "FORGE_TENANT"}:
        raise ValueError("unexpected client environment entry")
    env[key] = value
config = json.loads((state / "platform.json").read_text())
stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
output = args.resume.resolve() if args.resume else repo / "benchmarks/results" / ("repairs-" + stamp)
if args.resume and not output.is_dir():
    raise ValueError("--resume requires an existing evidence directory; it never submits new runs")
output.mkdir(mode=0o700, exist_ok=bool(args.resume))
command = [str(repo / "bin/forge"), "--state-dir", str(state / "client-receipts")]


def call(*arguments, timeout=30):
    result = subprocess.run(command + list(arguments), env=env, cwd=repo, text=True,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
    if result.returncode:
        raise RuntimeError("CLI failed: " + result.stderr)
    return json.loads(result.stdout)


fixtures = ["clamp", "expiry", "ranges"] if args.fixture == "all" else [args.fixture]
if args.resume:
    for fixture in fixtures:
        if not (output / fixture / "submission.json").is_file():
            raise ValueError("--resume requires a saved submission.json for every selected fixture; no runs were submitted")
report = {"started_at": stamp, "scope": "fake model; real CLI, HTTP, PostgreSQL, gRPC, SQLite and rootless Docker", "fixtures": []}
try:
    for fixture in fixtures:
        directory = output / fixture
        directory.mkdir(exist_ok=bool(args.resume))
        started = time.monotonic()
        if (directory / "submission.json").exists():
            submitted = json.loads((directory / "submission.json").read_text())
        else:
            project = call("project", "create", "--name", fixture + "-" + stamp,
                           "--source", fixture, "--profile", config["sources"][fixture]["profile_id"])
            submitted = call("run", "submit", project["id"], "--task-file", str(repo / "testdata/repairs" / fixture / "task.txt"),
                             "--base", config["sources"][fixture]["hash"], "--config", "demo", "--idempotency-key", str(uuid.uuid4()))
        run_id = submitted["run_id"]
        print(json.dumps({"fixture": fixture, "run_id": run_id, "stage": "submitted"}), flush=True)
        (directory / "submission.json").write_text(json.dumps(submitted, indent=2) + "\n")
        with (directory / "events.jsonl").open("w") as stream:
            watched = subprocess.run(command + ["run", "watch", run_id], env=env, cwd=repo, text=True,
                                     stdout=stream, stderr=subprocess.PIPE, timeout=330)
        if watched.returncode:
            raise RuntimeError("watch failed: " + watched.stderr)
        run = call("run", "get", run_id)
        artifacts = call("run", "artifacts", run_id, "--limit", "1000")
        (directory / "run.json").write_text(json.dumps(run, indent=2) + "\n")
        (directory / "artifacts.json").write_text(json.dumps(artifacts, indent=2) + "\n")
        for artifact in artifacts:
            if artifact["kind"] in {"baseline", "verification", "patch", "patch_manifest", "operation_receipt"}:
                destination = directory / artifact["id"]
                if not destination.exists():
                    call("run", "download", artifact["id"], "--output", str(destination))
                assert hashlib.sha256(destination.read_bytes()).hexdigest() == artifact["sha256"]
        assert run["state"]["status"] == "completed", run["state"]
        assert run["state"]["verification_status"] == "verified", run["state"]
        patch = directory / run["state"]["output_ref"]
        baseline = next(a for a in artifacts if a["kind"] == "baseline")
        assert json.loads((directory / baseline["id"]).read_text())["target_failed"] is True
        # Reproduce the exported artifact on another checkout using Git itself.
        reproduced = directory / "reproduced"
        # Run outside this repository: git apply inside an unrelated nested
        # directory may silently skip paths outside that directory's prefix.
        with tempfile.TemporaryDirectory(prefix="forge-patch-replay-", dir="/tmp") as temporary:
            checkout = Path(temporary) / "checkout"
            shutil.copytree(repo / "testdata/repairs" / fixture / "source", checkout)
            subprocess.run(["git", "apply", "--check", str(patch)], cwd=checkout, check=True)
            subprocess.run(["git", "apply", str(patch)], cwd=checkout, check=True)
            shutil.copytree(checkout, reproduced, dirs_exist_ok=True)
        expected = repo / "testdata/repairs" / fixture / "expected"
        expected_files = {p.relative_to(expected).as_posix(): p.read_bytes() for p in expected.rglob("*") if p.is_file()}
        reproduced_files = {p.relative_to(reproduced).as_posix(): p.read_bytes() for p in reproduced.rglob("*") if p.is_file()}
        assert reproduced_files == expected_files, "exported patch differs from the complete expected source snapshot"
        events = [json.loads(line) for line in (directory / "events.jsonl").read_text().splitlines()]
        finished = next(e["created_at"] for e in events if e["type"] == "run.finished")
        elapsed = (datetime.datetime.fromisoformat(finished) - datetime.datetime.fromisoformat(run["created_at"])).total_seconds()
        item = {"fixture": fixture, "run_id": run_id, "database_observed_seconds": elapsed,
                "status": "completed", "verification": "verified", "baseline_target_failed": True,
                "exported_patch_reapplied": True, "source_files": sorted(expected_files), "patch_sha256": hashlib.sha256(patch.read_bytes()).hexdigest()}
        report["fixtures"].append(item)
        print(json.dumps(item), flush=True)
finally:
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print("Evidence: " + str(output), flush=True)
