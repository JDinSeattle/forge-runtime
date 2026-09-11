#!/usr/bin/python3
"""Check fixed source/oracle behavior using actual isolated grader containers; no LLM."""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location("eval_common", Path(__file__).absolute().parent / "common.py")
common = importlib.util.module_from_spec(spec)
spec.loader.exec_module(common)


def validate_proof(raw, exit_code, case, variant, suite):
    expected = 1 if variant == "source" and suite == "target" else 0
    try:
        proof = json.loads(raw)
    except (ValueError, UnicodeDecodeError):
        raise ValueError("grader did not emit one structured result; build/start errors are not target-failure evidence")
    if not isinstance(proof, dict) or proof.get("fixture") != case or proof.get("suite") != suite:
        raise ValueError("structured grader result does not identify the selected suite")
    if expected == 1:
        if proof.get("passed") is not False or type(proof.get("case")) is not int or proof["case"] < 0:
            raise ValueError("expected target failure requires an executed, identified grader assertion")
    elif type(proof.get("passed")) is not int or proof["passed"] <= 0:
        raise ValueError("expected success requires a positive executed assertion count")
    if exit_code != expected:
        raise ValueError("source/oracle did not satisfy the frozen target/regression distinction")
    return proof


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--docker-host", required=True)
    parser.add_argument("--python-image", required=True)
    parser.add_argument("--go-image", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if not args.docker_host.startswith("unix:///"):
        raise ValueError("explicit local Unix Docker socket required")
    for image in (args.python_image, args.go_image):
        if not re.fullmatch(r"[a-z0-9][a-z0-9._/:+-]*@sha256:[0-9a-f]{64}", image):
            raise ValueError("explicit pinned images required")
    manifest = common.verify_corpus()
    output = Path(args.output).absolute()
    if output.resolve() != output:
        raise ValueError("evidence path contains a symlink")
    os.umask(0o077)
    output.mkdir(mode=0o700)
    docker = ["docker", "--host", args.docker_host]
    report = {"scope": "fixed corpus oracle validation only; no model quality result or provider call", "corpus_sha256": manifest["sha256"], "cases": [], "passed": False}
    images = {}
    for image in (args.python_image, args.go_image):
        raw = json.loads(subprocess.check_output(docker + ["image", "inspect", image], text=True))[0]
        images[image] = {"id": raw["Id"], "repo_digests": raw["RepoDigests"]}
    report["images"] = images
    try:
        for case in common.CASES:
            for variant in ("source", "oracle"):
                for suite in ("target", "regression"):
                    image = args.python_image if case.startswith("py-") else args.go_image
                    name = "forge-eval-" + os.urandom(6).hex()
                    tmpfs = "/tmp:rw,nosuid,nodev,size=67108864,mode=1777" + (",noexec" if case.startswith("py-") else ",exec")
                    command = docker + ["run", "--name", name, "--pull=never", "--network=none", "--read-only", "--user=1000:1000", "--cap-drop=ALL", "--security-opt=no-new-privileges", "--pids-limit=64", "--memory=256m", "--cpus=1", "--tmpfs", tmpfs, "--workdir=/workspace", "--mount", "type=bind,src=" + str(common.CORPUS / case / variant) + ",dst=/workspace,readonly", "--mount", "type=bind,src=" + str(common.CORPUS / "graders") + ",dst=/forge-tests,readonly", image] + common.grade_command(case, suite)
                    started = time.monotonic()
                    try:
                        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=55)
                        (output / f"{case}-{variant}-{suite}.stdout").write_bytes(result.stdout)
                        (output / f"{case}-{variant}-{suite}.stderr").write_bytes(result.stderr)
                        expected = 1 if variant == "source" and suite == "target" else 0
                        item = {"case": case, "variant": variant, "suite": suite, "exit_code": result.returncode, "expected_exit": expected, "seconds": time.monotonic() - started}
                        report["cases"].append(item)
                        try:
                            item["grader"] = validate_proof(result.stdout, result.returncode, case, variant, suite)
                        except ValueError as error:
                            item["failure_type"] = "infrastructure_or_grader_contract_failure"
                            item["failure"] = str(error)
                            raise
                        print(json.dumps(item), flush=True)
                    finally:
                        # Only the unique container created for this one case.
                        subprocess.run(docker + ["rm", "--force", name], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        report["passed"] = True
    finally:
        common.save_json(output / "report.json", report)


if __name__ == "__main__":
    main()
