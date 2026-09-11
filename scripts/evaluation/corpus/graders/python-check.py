"""Operator-owned finite tests; candidate code is never imported in this parent."""
import json
from pathlib import Path
import resource
import subprocess
import sys
import tempfile

case, suite = sys.argv[1:]
if suite not in ("target", "regression"):
    raise SystemExit(2)
cases = json.loads(Path("/forge-tests/cases.json").read_text())[case]


def limit_child():
    resource.setrlimit(resource.RLIMIT_FSIZE, (16384, 16384))
    resource.setrlimit(resource.RLIMIT_CPU, (2, 2))


for index, (arguments, expected) in enumerate(cases[suite]):
    with tempfile.TemporaryDirectory(prefix="eval-check-") as temporary:
        with open(temporary + "/out", "w+b") as stdout, open(temporary + "/err", "w+b") as stderr:
            try:
                child = subprocess.run([sys.executable, "-I", "-B", "/forge-tests/invoke.py"], input=json.dumps({"function": cases["function"], "args": arguments}).encode(), stdout=stdout, stderr=stderr, timeout=3, preexec_fn=limit_child)
                stdout.seek(0)
                actual = json.loads(stdout.read(16385))
            except (subprocess.TimeoutExpired, ValueError, TypeError):
                actual = None
                child = None
    if child is None or child.returncode != 0 or actual != expected:
        print(json.dumps({"fixture": case, "suite": suite, "case": index, "passed": False}), flush=True)
        raise SystemExit(1)
print(json.dumps({"fixture": case, "suite": suite, "passed": len(cases[suite])}), flush=True)
