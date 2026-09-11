#!/usr/bin/env python3
"""Build frozen test binaries, or run them against private loopback PostgreSQL.

No live service, runner socket, workspace pool, paid provider or secret output.
Run mode does not compile. It only uses verified binaries from build mode.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import tempfile
import time
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[2]
OWNED = ["internal/application/text_batch.go", "internal/application/text_batch_test.go",
         "internal/application/text_batch_integration_test.go", "internal/application/model.go",
         "internal/application/driver.go", "internal/configuration/config.go",
         "internal/configuration/text_batch_test.go", "cmd/forge-worker/main.go",
         "internal/application/text_batch_acceptance.py"]

GO_TOOL_ENV = ("HOME", "XDG_CACHE_HOME", "GOPATH", "GOROOT", "GOCACHE",
               "GOMODCACHE", "GOTMPDIR", "TMPDIR")


def tool_environment():
    # F03 invokes the local `go tool buildid` observer. It needs a cache location
    # even though the acceptance uses already-built binaries. Carry only these
    # noncredential path settings, never the complete parent environment.
    env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "LANG": "C.UTF-8", "TZ": "UTC"}
    for key in GO_TOOL_ENV:
        if os.environ.get(key):
            env[key] = os.environ[key]
    env.setdefault("GOCACHE", "/tmp/forge-runtime-gocache")
    # Disable persisted user Go settings, module lookups and toolchain downloads.
    env.update(GOENV="off", GOTOOLCHAIN="local", GOPROXY="off", GOSUMDB="off")
    return env


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def source_hashes():
    names = subprocess.check_output(["git", "ls-files", "-co", "--exclude-standard"], cwd=ROOT, text=True).splitlines()
    return {name: digest(ROOT / name) for name in sorted(set(names))
            if (name.endswith((".go", ".sql")) or name in ("go.mod", "go.sum"))
            and not name.startswith(("benchmarks/results/", "var/"))}


def build(output):
    before = source_hashes()
    binary_dir = Path(tempfile.mkdtemp(prefix="forge-text-batch-binaries-"))
    env = os.environ.copy()
    env.update(GOCACHE="/tmp/forge-runtime-gocache", GOPROXY="off")
    specs = [("application.test", "./internal/application"), ("configuration.test", "./internal/configuration"), ("benchmarks.test", "./benchmarks")]
    commands = [["go", "test", "-race", "-c", "-o", str(binary_dir / name), package] for name, package in specs]
    commands.append(["go", "build", "-o", str(binary_dir / "forge-worker"), "./cmd/forge-worker"])
    results = []
    for number, command in enumerate(commands):
        with (output / f"build-{number}.log").open("w") as log:
            proc = subprocess.run(command, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT, timeout=120)
        results.append({"argv": command, "exit_code": proc.returncode})
        save(output / "commands.json", results)
        if proc.returncode:
            raise SystemExit(f"build failed; see {output / f'build-{number}.log'}")
    after = source_hashes()
    identity = {"commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
                "source_before": before, "source_after": after, "whole_source_unchanged": before == after,
                "binaries": {p.name: {"path": str(p), "sha256": digest(p)} for p in binary_dir.iterdir()}}
    save(output / "identity.json", identity)
    for name in OWNED:
        dest = output / "source" / (name + ".txt")
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / name, dest)
    if before != after:
        raise SystemExit("source changed during build; preserve this record and build again")
    print(json.dumps({"build_record": str(output), "source_count": len(before), "binary_count": len(identity["binaries"])}))


def run(output, build_record, env_file):
    identity = json.loads((build_record / "identity.json").read_text())
    assert identity["whole_source_unchanged"], "unstable build"
    binaries = {}
    for name, row in identity["binaries"].items():
        binary = Path(row["path"])
        assert digest(binary) == row["sha256"], "binary changed after build"
        binaries[name] = binary
    # The original F03 harness snapshots live source. Reject mismatch so its
    # snapshots cannot be accidentally attributed to another executable.
    assert source_hashes() == identity["source_after"], "source changed since build; build a new record"
    assert env_file.stat().st_mode & 0o077 == 0, "environment file must be owner-only"
    private = {}
    for line in env_file.read_text().splitlines():
        key, sep, value = line.removeprefix("export ").strip().partition("=")
        if sep and key in ("FORGE_REVIEW_DATABASE_URL", "FORGE_REVIEW_ALLOW_FIXTURES"):
            fields = shlex.split(value)
            assert len(fields) == 1, "malformed private environment entry"
            private[key] = fields[0]
    assert private.get("FORGE_REVIEW_ALLOW_FIXTURES") == "1", "private fixtures not explicitly allowed"
    dsn = private["FORGE_REVIEW_DATABASE_URL"]
    parsed = urlparse(dsn)
    assert parsed.hostname == "127.0.0.1" and parsed.path == "/forge", "refusing nonlocal/nonfixture database"
    env = tool_environment()
    env.update(FORGE_TEST_DATABASE_URL=dsn, FORGE_RUN_MODEL_STREAM_FAULT="1",
               FORGE_TEXT_BATCH_EVIDENCE_DIR=str(output / "postgres"))
    shutil.copytree(build_record, output / "build-record")
    shutil.copyfile(Path(__file__).resolve(), output / "launcher.py")
    save(output / "execution-start.json", {"build_record": str(build_record), "launcher_sha256": digest(Path(__file__).resolve()),
                                           "environment_values_saved": False, "go_tool_environment_keys": sorted(tool_environment()),
                                           "scope": "private PG/FakeProvider or native-protocol loopback fixtures; no paid calls or real task containers"})
    specs = [("application", ROOT, "^TestTextBatch"), ("configuration", ROOT, "^TestLoadOperatorTextBatch"),
             ("benchmarks", ROOT / "benchmarks", "^Test(NativeStreamInterruptedLedgerEvidence|InterruptedLedgerOracle)")]
    original = set((ROOT / "benchmarks/results/local").glob("model-stream-interrupted-*"))
    results = []
    for name, cwd, pattern in specs:
        command = [str(binaries[name + ".test"]), "-test.v", "-test.run", pattern, "-test.timeout=90s"]
        started = time.time_ns()
        with (output / (name + ".log")).open("w") as log:
            proc = subprocess.run(command, cwd=cwd, env=env, stdout=log, stderr=subprocess.STDOUT, timeout=100)
        results.append({"argv": command, "cwd": str(cwd), "started_unix_ns": started, "finished_unix_ns": time.time_ns(), "exit_code": proc.returncode})
        save(output / "execution.json", results)
        if proc.returncode:
            break
    new = set((ROOT / "benchmarks/results/local").glob("model-stream-interrupted-*")) - original
    for path in sorted(new):
        shutil.copytree(path, output / "native-f03" / path.name)
    save(output / "source-after-execution.json", source_hashes())
    print(json.dumps({"evidence": str(output), "exit_codes": [row["exit_code"] for row in results], "f03_original_directories": [str(p) for p in sorted(new)]}))
    return next((row["exit_code"] for row in results if row["exit_code"]), 0)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("build", "run"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--build-record", type=Path)
    parser.add_argument("--env-file", type=Path)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    if args.mode == "build":
        build(output)
        return 0
    if not args.build_record or not args.env_file:
        parser.error("run requires --build-record and --env-file")
    return run(output, args.build_record.resolve(), args.env_file.resolve())


if __name__ == "__main__":
    raise SystemExit(main())
