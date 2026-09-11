#!/usr/bin/env python3
"""Run already-built control acceptance binaries with private loopback PG env.

This does not compile, start services, or access the Docker/workspace pool.
The Go tests create/drop their own unique schema using the existing review helper.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--build-record", required=True, type=Path)
    parser.add_argument("--env-file", required=True, type=Path)
    parser.add_argument("--evidence", required=True, type=Path)
    args = parser.parse_args()
    repo = Path(__file__).resolve().parents[2]
    record = args.build_record.resolve()
    identity = json.loads((record / "source-identity.json").read_text())
    commands = json.loads((record / "command.json").read_text())["argv"]
    assert identity["whole_tree_unchanged"], "build did not retain a stable source set"
    binaries = []
    for command in commands[:2]:
        binary = Path(command[command.index("-o") + 1])
        assert digest(binary) == identity["binaries"][binary.name], "binary changed after build"
        binaries.append(binary)
    assert args.env_file.stat().st_mode & 0o077 == 0, "private environment file must be owner-only"
    env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "LANG": "C.UTF-8", "TZ": "UTC"}
    for line in args.env_file.read_text().splitlines():
        key, separator, value = line.removeprefix("export ").strip().partition("=")
        if separator and key in ("FORGE_REVIEW_DATABASE_URL", "FORGE_REVIEW_ALLOW_FIXTURES"):
            parsed = shlex.split(value)
            assert len(parsed) == 1, "invalid private environment entry"
            env[key] = parsed[0]
    assert env.get("FORGE_REVIEW_DATABASE_URL") and env.get("FORGE_REVIEW_ALLOW_FIXTURES") == "1"
    output = args.evidence.resolve()
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    env.update(FORGE_REVIEW_FORGE_BINARY=str(binaries[0]), FORGE_REVIEW_RESUME_EVIDENCE=str(output))
    for name in ("source-identity.json", "resume_capacity_test.go.txt", "forge-build-info.log", "review.test-build-info.log"):
        shutil.copyfile(record / name, output / name)
    command = [str(binaries[1]), "-test.v", "-test.run", "^TestReview(HTTPResumePreservesIdentityAndCapacity|ConcurrentFinalizeDoesNotReleaseSentinelCapacity|CLIResumeSucceedsThroughRealHTTP)$", "-test.timeout=2m"]
    (output / "command.json").write_text(json.dumps({"argv": command, "build_record": str(record), "private_environment_source": str(args.env_file.resolve()), "environment_values_saved": False, "runner_docker_or_live_services_used": False}, indent=2) + "\n")
    with (output / "test.log").open("w") as log:
        result = subprocess.run(command, cwd=repo, env=env, stdout=log, stderr=subprocess.STDOUT, timeout=130)
    (output / "execution.json").write_text(json.dumps({"exit_code": result.returncode, "binaries": {p.name: digest(p) for p in binaries}, "launcher_sha256": digest(Path(__file__).resolve()), "scope": "control fixture: real TCP HTTP/CLI/PG, no real external effect or verifier"}, indent=2) + "\n")
    print(json.dumps({"evidence": str(output), "exit_code": result.returncode}))
    print((output / "test.log").read_text())
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
