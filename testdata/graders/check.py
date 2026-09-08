"""Operator-owned fixed tests. Never import candidate code in this process."""
import json
import subprocess
import sys

CASES = {
    "clamp": {
        "function": "clamp",
        "target": [([5, 0, 10], {"value": 5}), ([-1, 0, 10], {"value": 0}), ([12, 0, 10], {"value": 10})],
        "regression": [([0, 0, 10], {"value": 0}), ([10, 0, 10], {"value": 10}), ([2, 5, 5], {"value": 5}), ([0, 5, 2], {"error": "ValueError"})],
    },
    "expiry": {
        "function": "is_expired",
        "target": [([100, 10, 110], {"value": True}), ([100, 0, 100], {"value": True})],
        "regression": [([100, 10, 109], {"value": False}), ([100, 10, 111], {"value": True}), ([100, -1, 100], {"error": "ValueError"})],
    },
    "ranges": {
        "function": "merge_ranges",
        "target": [([[[1, 3], [3, 5]]], {"value": [[1, 5]]}), ([[[3, 4], [1, 3], [4, 7]]], {"value": [[1, 7]]})],
        "regression": [([[]], {"value": []}), ([[[1, 10], [2, 3]]], {"value": [[1, 10]]}), ([[[1, 2], [4, 6]]], {"value": [[1, 2], [4, 6]]}), ([[[4, 2]]], {"error": "ValueError"})],
    },
}

fixture, suite = sys.argv[1:]
cases = CASES[fixture]
for index, (args, expected) in enumerate(cases[suite]):
    child = subprocess.run(
        [sys.executable, "-I", "-B", "/forge-tests/invoke.py"],
        input=json.dumps({"function": cases["function"], "args": args}),
        text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=3,
    )
    try:
        actual = json.loads(child.stdout)
    except (ValueError, TypeError):
        actual = {"invalid_candidate_output": True}
    if child.returncode != 0 or actual != expected:
        print(json.dumps({"fixture": fixture, "suite": suite, "case": index, "expected": expected, "actual": actual, "exit_code": child.returncode}), flush=True)
        raise SystemExit(1)
print(json.dumps({"fixture": fixture, "suite": suite, "passed": len(cases[suite])}), flush=True)
