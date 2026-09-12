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
}


def private_json(path):
    p.directory(path.parent, private=True)
    st = path.lstat()
    if not stat.S_ISREG(st.st_mode) or st.st_uid != os.getuid() or st.st_nlink != 1 or st.st_mode & 0o077 or st.st_size > 1 << 20:
        raise ValueError("private owned JSON input required")
    return json.loads(path.read_bytes())


def inputs():
    base, preparation = p.read_preparation(IDENTITY)
    a = private_json(base / "acceptance.json")
    expected = {"purpose": p.PURPOSE, "fixture_id": IDENTITY, "scope_root": str(base), "pool_root": str(base / "pool-root"),
                "runner_config": str(base / "runtime/runner.json"), "evidence_dir": str(base / "evidence/sigterm-01")}
    for label, filename in (("runner", "forge-runner"), ("worker", "forge-worker"), ("test", "application-faults.test")):
        path = base / "bin" / filename
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


def command(phase, a):
    test, optin, acceptance, timeout = PHASES[phase]
    env = {"PATH": "/usr/local/bin:/usr/bin:/bin", "LC_ALL": "C", "TMPDIR": "/tmp",
           "XDG_RUNTIME_DIR": "/run/user/1000", "FORGE_METRICS_LISTEN": "127.0.0.1:0",
           "FORGE_TEST_DATABASE_URL": credential(), optin: "1", acceptance: str(Path(a["scope_root"]) / "acceptance.json")}
    return [a["test_binary"], "-test.run=^" + test + "$", "-test.timeout=" + str(timeout) + "s", "-test.v"], env


def completed_report(phase, base):
    relative = "evidence/sigterm-01/worker-runner-sigterm/acceptance.json" if phase == "sigterm" else "evidence/logs-01/acceptance.json"
    report = private_json(base / relative)
    if not isinstance(report, dict) or report.get("passed") is not True:
        raise ValueError("actual acceptance did not record a successful final report")
    if phase == "logs":
        cases = report.get("cases")
        expected = {"L1", "L2-L3-default", "L3-bytes", "L3-count", "L4", "L5"}
        if not isinstance(cases, dict) or set(cases) != expected or any(not isinstance(case, dict) or case.get("passed") is not True for case in cases.values()):
            raise ValueError("all six actual log case results are required")


def mapped_child(phase):
    # UID 0 is expected only inside a full subordinate mapping. Host-root and
    # unmapped execution are rejected before opening a credential or a DB.
    uid = Path("/proc/self/uid_map").read_text().split()
    gid = Path("/proc/self/gid_map").read_text().split()
    if os.getuid() != 0 or len(uid) != 6 or len(gid) != 6 or uid[:3] != ["0", "1000", "1"] or gid[:3] != ["0", "1000", "1"] or uid[3] != "1" or gid[3] != "1" or int(uid[5]) < 65536 or int(gid[5]) < 65536:
        raise ValueError("dedicated full subordinate UID/GID map required")
    base, a = inputs()
    argv, env = command(phase, a)
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
        completed_report(phase, base)
    return code


def unit_state(unit):
    result = subprocess.run(["/usr/bin/systemctl", "--user", "show", unit, "--property=LoadState,ActiveState,SubState,MainPID,ControlGroup,InvocationID"],
                            capture_output=True, text=True, timeout=10)
    state = dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)
    if result.returncode and state.get("LoadState") != "not-found":
        raise ValueError("could not inspect dedicated unit")
    return state


def launch(phase):
    if os.getuid() != 1000 or os.getuid() != os.geteuid():
        raise ValueError("launch from the ordinary UID 1000 host session")
    base, a = inputs()
    # Validate privately now; do not log or put this value in a unit property.
    credential()
    out = base / "evidence" / ("host-" + phase)
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
            "/usr/bin/python3", "-I", str(Path(__file__).absolute()), "child", "--phase", phase]
    p.save(out / "intent.json", {"unit": unit, "argv": argv, "phase": phase,
        "input_sha256": {str(Path(__file__).absolute()): p.digest(Path(__file__).absolute()),
                         str(Path(__file__).with_name("prepare.py").absolute()): p.digest(Path(__file__).with_name("prepare.py")),
                         str(base / "acceptance.json"): p.digest(base / "acceptance.json"),
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
    parser.add_argument("action", choices=("launch", "child"))
    parser.add_argument("--phase", required=True, choices=PHASES)
    args = parser.parse_args()
    return launch(args.phase) if args.action == "launch" else mapped_child(args.phase)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        print("refused: " + str(error), file=sys.stderr)
        sys.exit(1)
