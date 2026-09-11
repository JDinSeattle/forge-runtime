#!/usr/bin/python3
"""Run exactly one prepared source or target process in a mapped namespace."""
import argparse
import importlib.util
import os
from pathlib import Path
import subprocess
import sys

sys.dont_write_bytecode = True
HERE = Path(__file__).absolute().parent
spec = importlib.util.spec_from_file_location("recovery_rehearse", HERE / "rehearse.py")
rehearse = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rehearse)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("source", "target"))
    parser.add_argument("--id", required=True)
    args = parser.parse_args()
    base = rehearse.scope(args.id)
    maps = [tuple(map(int, line.split())) for line in Path("/proc/self/uid_map").read_text().splitlines()]
    if os.geteuid() != 0 or len(maps) < 2 or maps[0][0] != 0 or maps[0][1] == 0 or maps[0][2] != 1:
        raise ValueError("use a fresh full subordinate UID/GID mapped RootlessKit namespace, never host root")
    expected = base / ("source-evidence.json" if args.phase == "source" else "recovery-report.json")
    if expected.exists():
        raise ValueError("this phase already produced evidence; refuse a replay")
    config = base / (args.phase + "-config.json")
    if not config.is_file():
        raise ValueError("prepare/restore configuration is missing")
    env = dict(os.environ, FORGE_RECOVERY_CONFIG=str(config))
    test = "TestRecoverySourceProcess" if args.phase == "source" else "TestRecoveryRestoredProcess"
    result = subprocess.run([str(rehearse.REPO / "bin/recovery.test"), "-test.run=^" + test + "$", "-test.v"], cwd=rehearse.REPO, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    log = base / (args.phase + "-process.log")
    with log.open("x") as f:
        f.write(result.stdout)
        f.flush()
        os.fsync(f.fileno())
    rehearse.sync_directory(base)
    print(result.stdout, end="")
    expected_exit = 86 if args.phase == "source" else 0
    if result.returncode != expected_exit or not expected.is_file():
        raise RuntimeError(f"{args.phase} exited {result.returncode}; expected {expected_exit} and synced evidence; preserve this fixture")
    print(f"{args.phase} phase complete (child exit {result.returncode}); evidence: {expected}")


if __name__ == "__main__":
    main()
