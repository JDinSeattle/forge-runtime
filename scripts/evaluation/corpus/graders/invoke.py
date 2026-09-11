"""Candidate runs only in this bounded child; expectations live in the parent."""
import copy
import importlib.util
import json
import sys

request = json.load(sys.stdin)
spec = importlib.util.spec_from_file_location("candidate", "/workspace/app.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
before = copy.deepcopy(request["args"])
try:
    answer = getattr(module, request["function"])(*request["args"])
    if request["function"] == "latest_by_id":
        if request["args"] != before:
            raise RuntimeError("input mutation")
        if any(item is original for item in answer for original in request["args"][0]):
            raise RuntimeError("result dictionaries alias input")
    json.dump({"value": answer}, sys.stdout, ensure_ascii=False)
except ValueError:
    json.dump({"error": "ValueError"}, sys.stdout)
