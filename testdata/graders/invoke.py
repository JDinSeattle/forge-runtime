"""Untrusted candidate executes in a child; the parent owns expectations."""
import importlib.util
import json
import sys

spec = importlib.util.spec_from_file_location("candidate", "/workspace/app.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
request = json.load(sys.stdin)
try:
    answer = getattr(module, request["function"])(*request["args"])
    json.dump({"value": answer}, sys.stdout)
except ValueError:
    json.dump({"error": "ValueError"}, sys.stdout)
