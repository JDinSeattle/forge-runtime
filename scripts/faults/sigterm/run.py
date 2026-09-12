#!/usr/bin/python3
"""Run one frozen lifecycle acceptance in its own delegated user service.

This entry point never mounts volumes, stops existing services, cleans Docker
containers, reinitializes a journal, or automatically retries an uncertain run.
The private database credential is read inside the service, not put in argv.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
from pathlib import Path
import re
import secrets
import shlex
import stat
import subprocess
import sys
import time
from urllib.parse import urlsplit, parse_qs

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location("lifecycle_prepare", Path(__file__).with_name("prepare.py"))
p = importlib.util.module_from_spec(spec)
spec.loader.exec_module(p)
IDENTITY = "lr20260912_a"
PHASES = {
    "sigterm": ("TestRealWorkerRunnerSIGTERM", "FORGE_RUN_WORKER_RUNNER_SIGTERM", "FORGE_WORKER_RUNNER_SIGTERM_ACCEPTANCE", 210),
    "logs": ("TestStrictLogsCombinedAcceptance", "FORGE_RUN_STRICT_LOGS_COMBINED", "FORGE_STRICT_LOGS_ACCEPTANCE", 600),
    "abort": ("TestAbortUnstartedLifecycle", "FORGE_ABORT_UNSTARTED_LIFECYCLE", "FORGE_WORKER_RUNNER_SIGTERM_ACCEPTANCE", 90),
    "logs-cleanup": ("TestStrictLogsRetainedCleanup", "FORGE_RUN_STRICT_LOGS_CLEANUP", "FORGE_STRICT_LOGS_ACCEPTANCE", 180),
}


def private_json(path):
    p.directory(path.parent, private=True)
    st = path.lstat()
    if not stat.S_ISREG(st.st_mode) or st.st_uid != os.getuid() or st.st_nlink != 1 or st.st_mode & 0o077 or st.st_size > 1 << 20:
        raise ValueError("private owned JSON input required")
    return json.loads(path.read_bytes())


def manifest_name(phase, attempt, logs_execution="01"):
    if phase not in PHASES or attempt not in ("01", "02") or (phase == "abort" and attempt != "01") or logs_execution not in ("01", "02"):
        raise ValueError("explicit supported lifecycle phase and attempt required")
    if phase == "logs-cleanup":
        if attempt != "02" or logs_execution != "01":
            raise ValueError("cleanup belongs only to the failed logs01 on SIGTERM02")
        return "acceptance-logs-cleanup-01.json"
    if logs_execution != "01":
        if phase != "logs" or attempt != "02":
            raise ValueError("logs execution02 requires the completed SIGTERM02 source")
        return "acceptance-logs-02.json"
    return "acceptance-abort-01.json" if phase == "abort" else ("acceptance.json" if attempt == "01" else "acceptance-02.json")


def inputs(phase="sigterm", attempt="01", logs_execution="01"):
    name = manifest_name(phase, attempt, logs_execution)
    base, preparation = p.read_preparation(IDENTITY)
    a = private_json(base / name)
    expected = {"purpose": p.PURPOSE, "fixture_id": IDENTITY, "scope_root": str(base), "pool_root": str(base / "pool-root"),
                "runner_config": str(base / "runtime/runner.json"), "evidence_dir": str(base / ("evidence/sigterm-" + attempt))}
    binary_dir = base / "bin"
    if name != "acceptance.json":
        if not isinstance(a, dict) or not isinstance(a.get("test_binary"), str):
            raise ValueError("versioned frozen executable required")
        binary_dir = Path(a["test_binary"]).parent
        if binary_dir.parent != base / "bin" or not re.fullmatch(r"[a-f0-9]{40}", binary_dir.name):
            raise ValueError("versioned frozen executable must select one full revision")
    for label, filename in (("runner", "forge-runner"), ("worker", "forge-worker"), ("test", "application-faults.test")):
        path = binary_dir / filename
        p.directory(path.parent, private=True)
        st = path.lstat()
        if not stat.S_ISREG(st.st_mode) or st.st_uid != os.getuid() or st.st_mode & 0o022 or not st.st_mode & stat.S_IXUSR:
            raise ValueError("frozen owned executable required")
        expected[label + "_binary"] = str(path)
        expected[label + "_sha256"] = p.digest(path)
    if a != expected:
        raise ValueError("acceptance scope or executable identity changed")
    c = private_json(base / "runtime/runner.json")
    observations = private_json(base / "runtime/configuration-observations.json")
    if p.digest(base / "runtime/runner.json") != observations["runner_config_sha256"]:
        raise ValueError("runner configuration changed")
    expected_paths = {"root_dir": "engine", "journal_path": "journal.sqlite", "artifact_root": "artifacts", "signing_key_file": "runner.key"}
    if any(c.get(key) != str(base / "runtime" / leaf) for key, leaf in expected_paths.items()):
        raise ValueError("runner storage outside the dedicated scope")
    if c.get("allow_test_backend") is not False or c.get("docker_host") != preparation["docker_host"]:
        raise ValueError("production backend identity required")
    return base, a


def prepare_continuation(revision):
    if not isinstance(revision, str) or not re.fullmatch(r"[a-f0-9]{40}", revision):
        raise ValueError("full reviewed source revision required")
    base, original = inputs()
    binary_dir = p.directory(base / "bin" / revision, private=True)
    updated = dict(original)
    for label, filename in (("runner", "forge-runner"), ("worker", "forge-worker"), ("test", "application-faults.test")):
        path = binary_dir / filename
        st = path.lstat()
        if not stat.S_ISREG(st.st_mode) or st.st_uid != os.getuid() or st.st_mode & 0o022 or not st.st_mode & stat.S_IXUSR:
            raise ValueError("frozen owned executable required")
        updated[label + "_binary"] = str(path)
        updated[label + "_sha256"] = p.digest(path)
    paths = [base / "acceptance-abort-01.json", base / "acceptance-02.json"]
    if any(os.path.lexists(path) for path in paths):
        raise ValueError("continuation manifests already exist; retain and inspect")
    p.save(paths[0], updated)
    updated["evidence_dir"] = str(base / "evidence/sigterm-02")
    p.save(paths[1], updated)
    return {"prepared": [str(path) for path in paths], "executed": False}


def prepare_logs_continuation(revision):
    if not isinstance(revision, str) or not re.fullmatch(r"[a-f0-9]{40}", revision):
        raise ValueError("full reviewed source revision required")
    base, historical = inputs("sigterm", "02")
    completed_report("sigterm", base, "02")
    failed = private_json(base / "evidence/logs-01/acceptance.json")
    if failed.get("passed") is not False:
        raise ValueError("the retained failed logs01 is required")
    binary_dir = p.directory(base / "bin" / revision, private=True)
    updated = dict(historical)
    for label, filename in (("runner", "forge-runner"), ("worker", "forge-worker"), ("test", "application-faults.test")):
        path = binary_dir / filename
        st = path.lstat()
        if not stat.S_ISREG(st.st_mode) or st.st_uid != os.getuid() or st.st_nlink != 1 or st.st_mode & 0o022 or not st.st_mode & stat.S_IXUSR:
            raise ValueError("frozen owned executable required")
        updated[label + "_binary"] = str(path)
        updated[label + "_sha256"] = p.digest(path)
    if updated == historical:
        raise ValueError("logs continuation requires new reviewed binaries")
    paths = [base / "acceptance-logs-cleanup-01.json", base / "acceptance-logs-02.json"]
    if any(os.path.lexists(path) for path in paths):
        raise ValueError("logs continuation manifests already exist; retain and inspect")
    for path in paths:
        p.save(path, updated)
    return {"prepared": [str(path) for path in paths], "executed": False}


def credential():
    path = p.REPO / "var/local/review-database.env"
    p.directory(path.parent, private=True)
    st = path.lstat()
    if not stat.S_ISREG(st.st_mode) or st.st_uid != os.getuid() or st.st_nlink != 1 or st.st_mode & 0o077 or st.st_size > 16384:
        raise ValueError("private database environment file required")
    values = []
    try:
        for line in path.read_text().splitlines():
            key, sep, value = line.strip().removeprefix("export ").partition("=")
            if sep and key == "FORGE_REVIEW_DATABASE_URL":
                parsed = shlex.split(value)
                if len(parsed) != 1:
                    raise ValueError()
                values.append(parsed[0])
        if len(values) != 1:
            raise ValueError()
        u = urlsplit(values[0])
        if u.scheme not in ("postgres", "postgresql") or u.hostname != "127.0.0.1" or u.port != 32773 or u.path != "/forge" or u.fragment:
            raise ValueError()
        query = parse_qs(u.query, keep_blank_values=True)
        if set(query) - {"sslmode"} or not u.username or not u.password:
            raise ValueError()
    except (ValueError, UnicodeError):
        raise ValueError("database credential must select the dedicated loopback /forge database") from None
    return values[0]


def command(phase, a, attempt="01", logs_execution="01"):
    name = manifest_name(phase, attempt, logs_execution)
    test, optin, acceptance, timeout = PHASES[phase]
    env = {"PATH": "/usr/local/bin:/usr/bin:/bin", "LC_ALL": "C", "TMPDIR": "/tmp",
           "HOME": str(Path(a["scope_root"])),
           "XDG_RUNTIME_DIR": "/run/user/1000", "FORGE_METRICS_LISTEN": "127.0.0.1:0",
           optin: "1", acceptance: str(Path(a["scope_root"]) / name)}
    if phase != "logs-cleanup":
        env["FORGE_TEST_DATABASE_URL"] = credential()
    if phase == "logs":
        env["FORGE_STRICT_LOGS_EXECUTION"] = logs_execution
    return [a["test_binary"], "-test.run=^" + test + "$", "-test.timeout=" + str(timeout) + "s", "-test.v"], env


def completed_report(phase, base, attempt="01", logs_execution="01"):
    manifest_name(phase, attempt, logs_execution)
    relative = {"sigterm": "evidence/sigterm-" + attempt + "/worker-runner-sigterm/acceptance.json",
                "logs": "evidence/logs-" + logs_execution + "/acceptance.json", "abort": "evidence/sigterm-01/abort-before-runner/report.json",
                "logs-cleanup": "evidence/logs-01-cleanup/report.json"}[phase]
    report = private_json(base / relative)
    if not isinstance(report, dict) or report.get("passed") is not True:
        raise ValueError("actual acceptance did not record a successful final report")
    if phase == "abort" and (report.get("status") != "cancel_requested" or report.get("terminal") is not False):
        raise ValueError("abort must record nonterminal cancellation intent, not lifecycle success")
    if phase == "logs-cleanup" and (report.get("released") is not True or report.get("snapshot_verified") is not True):
        raise ValueError("cleanup requires a verified archived snapshot and released workspace")
    if phase == "logs":
        cases = report.get("cases")
        expected = {"L1", "L2-L3-default", "L3-bytes", "L3-count", "L4", "L5"}
        if not isinstance(cases, dict) or set(cases) != expected or any(not isinstance(case, dict) or case.get("passed") is not True for case in cases.values()):
            raise ValueError("all six actual log case results are required")


def mapped_child(phase, attempt="01", logs_execution="01"):
    manifest_name(phase, attempt, logs_execution)
    # UID 0 is expected only inside a full subordinate mapping. Host-root and
    # unmapped execution are rejected before opening a credential or a DB.
    uid = Path("/proc/self/uid_map").read_text().split()
    gid = Path("/proc/self/gid_map").read_text().split()
    if os.getuid() != 0 or len(uid) != 6 or len(gid) != 6 or uid[:3] != ["0", "1000", "1"] or gid[:3] != ["0", "1000", "1"] or uid[3] != "1" or gid[3] != "1" or int(uid[5]) < 65536 or int(gid[5]) < 65536:
        raise ValueError("dedicated full subordinate UID/GID map required")
    base, a = inputs(phase, attempt, logs_execution)
    argv, env = command(phase, a, attempt, logs_execution)
    # Go returns zero even when a -test.run expression matches no test. Check
    # the actual frozen binary and the final case report, not just its status.
    listed = subprocess.run([a["test_binary"], "-test.list=^" + PHASES[phase][0] + "$"], env=env, cwd=base,
                            capture_output=True, text=True, timeout=15)
    if listed.returncode or listed.stdout.splitlines() != [PHASES[phase][0]]:
        raise ValueError("frozen binary does not contain the exact acceptance test")
    # The Go test performs live pool/owner/UUID/process preflights. A failed
    # test retains its exact identities; this wrapper performs no Docker action.
    code = subprocess.run(argv, env=env, cwd=base).returncode
    if code == 0:
        completed_report(phase, base, attempt, logs_execution)
    return code


def unit_state(unit):
    result = subprocess.run(["/usr/bin/systemctl", "--user", "show", unit, "--property=LoadState,ActiveState,SubState,MainPID,ControlGroup,InvocationID"],
                            capture_output=True, text=True, timeout=10)
    state = dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)
    if result.returncode and state.get("LoadState") != "not-found":
        raise ValueError("could not inspect dedicated unit")
    return state


def launch(phase, attempt="01", logs_execution="01"):
    name = manifest_name(phase, attempt, logs_execution)
    if os.getuid() != 1000 or os.getuid() != os.geteuid():
        raise ValueError("launch from the ordinary UID 1000 host session")
    base, a = inputs(phase, attempt, logs_execution)
    # Validate privately now; do not log or put this value in a unit property.
    if phase != "logs-cleanup":
        credential()
    suffix = "-02" if attempt == "02" else ""
    if phase == "logs" and logs_execution == "02":
        suffix += "-retry"
    out = base / "evidence" / ("host-" + phase + suffix)
    p.directory(out.parent, private=True)
    out.mkdir(mode=0o700)  # Exclusive: no automatic retry or evidence overwrite.
    unit = "forge-lifecycle-" + IDENTITY + "-" + phase + "-" + secrets.token_hex(8) + ".service"
    state_dir = Path("/run/user/1000") / unit.removesuffix(".service")
    if os.path.lexists(state_dir) or unit_state(unit).get("LoadState") != "not-found":
        raise ValueError("dedicated new unit and RootlessKit state required")
    maximum = PHASES[phase][3] + 30
    argv = ["/usr/bin/systemd-run", "--user", "--unit=" + unit, "--collect", "--wait", "--pipe",
            "--property=Delegate=yes", "--property=KillMode=control-group", "--property=RuntimeMaxSec=" + str(maximum) + "s", "--property=TimeoutStopSec=15s",
            "--working-directory=" + str(base), "/usr/bin/rootlesskit", "--propagation=rslave", "--state-dir=" + str(state_dir),
            "/usr/bin/python3", "-I", str(Path(__file__).absolute()), "child", "--phase", phase, "--attempt", attempt, "--logs-execution", logs_execution]
    p.save(out / "intent.json", {"unit": unit, "argv": argv, "phase": phase, "attempt": attempt, "logs_execution": logs_execution,
        "input_sha256": {str(Path(__file__).absolute()): p.digest(Path(__file__).absolute()),
                         str(Path(__file__).with_name("prepare.py").absolute()): p.digest(Path(__file__).with_name("prepare.py")),
                         str(base / name): p.digest(base / name),
                         "/usr/bin/rootlesskit": p.digest(Path("/usr/bin/rootlesskit"))},
        "scope": "only this new transient service; original services and uncertain Docker work untouched"})
    fd = os.open(out / "execution.log", os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    started = time.monotonic()
    with os.fdopen(fd, "wb") as log:
        client = subprocess.Popen(argv, stdout=log, stderr=subprocess.STDOUT)
        try:
            code = client.wait(timeout=maximum + 45)
        except subprocess.TimeoutExpired:
            # The unit owns its timeout independently of this waiting client.
            # No fallback stop of existing services or force-removal of jobs.
            client.terminate()
            try:
                client.wait(timeout=5)
            except subprocess.TimeoutExpired:
                client.kill()
                client.wait(timeout=5)
            code = 124
    state = unit_state(unit)
    p.save(out / "result.json", {"exit_code": code, "elapsed_seconds": time.monotonic() - started, "unit": unit, "after": state,
                                "scope": "launcher exit status only; consult actual case assertions and retained recovery descriptor"})
    if state.get("LoadState") != "not-found" and (state.get("ActiveState") not in ("inactive", "failed") or state.get("MainPID") != "0"):
        raise ValueError("dedicated unit termination not confirmed; retain scope and inspect exact unit")
    print(json.dumps({"exit_code": code, "evidence": str(out), "unit": unit}, sort_keys=True))
    return code


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("launch", "child", "prepare-continuation", "prepare-logs-continuation"))
    parser.add_argument("--phase", choices=PHASES)
    parser.add_argument("--attempt", default="01", choices=("01", "02"))
    parser.add_argument("--logs-execution", default="01", choices=("01", "02"))
    parser.add_argument("--revision")
    args = parser.parse_args()
    if args.action in ("prepare-continuation", "prepare-logs-continuation"):
        if args.phase is not None or args.attempt != "01" or args.logs_execution != "01":
            parser.error("preparation does not execute a phase")
        prepare = prepare_continuation if args.action == "prepare-continuation" else prepare_logs_continuation
        print(json.dumps(prepare(args.revision), sort_keys=True))
        return 0
    if args.phase is None or args.revision is not None:
        parser.error("launch/child require a phase and select existing manifests")
    return launch(args.phase, args.attempt, args.logs_execution) if args.action == "launch" else mapped_child(args.phase, args.attempt, args.logs_execution)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        print("refused: " + str(error), file=sys.stderr)
        sys.exit(1)
