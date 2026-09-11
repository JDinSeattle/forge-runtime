#!/usr/bin/python3
"""Generate new candidate configs from explicit operator model/budget inputs; never deploy."""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import re
import sys

sys.dont_write_bytecode = True
HERE = Path(__file__).absolute().parent
spec = importlib.util.spec_from_file_location("eval_common", HERE / "common.py")
common = importlib.util.module_from_spec(spec)
spec.loader.exec_module(common)


def positive(value):
    return type(value) is int and 0 < value <= 9223372036854775807


def validate_registration(r):
    allowed = {"provider", "model_id", "config_id", "total_budget_microusd", "task_budgets_microusd", "max_model_rounds", "max_tool_calls", "max_runtime_seconds", "capabilities", "model_spec", "price_sources", "capability_sources"}
    if not isinstance(r, dict) or set(r) != allowed:
        raise ValueError("registration must contain only the documented fields, never credentials")
    if r.get("provider") not in ("openai", "anthropic") or not isinstance(r.get("model_id"), str) or not r["model_id"].strip() or not common.valid_id(r.get("config_id")):
        raise ValueError("explicit supported native provider/model/config IDs required; fake is forbidden")
    if not positive(r.get("total_budget_microusd")) or set(r.get("task_budgets_microusd", {})) != set(common.CASES):
        raise ValueError("an explicit total and allocation for every fixed task are required")
    if not all(positive(value) for value in r["task_budgets_microusd"].values()) or sum(r["task_budgets_microusd"].values()) > r["total_budget_microusd"]:
        raise ValueError("task allocations exceed total budget or contain nonpositive values")
    for name, ceiling in (("max_model_rounds", 1000), ("max_tool_calls", 10000), ("max_runtime_seconds", 86400)):
        if not positive(r.get(name)) or r[name] > ceiling:
            raise ValueError("explicit bounded run limit required: " + name)
    model, caps = r.get("model_spec", {}), r.get("capabilities", {})
    model_fields = {"credential_group", "price_version", "input_price", "output_price", "cache_read_price", "cache_write_5m_price", "cache_write_1h_price", "exact_pricing", "context_tokens", "max_output_tokens", "request_timeout_ns"}
    bool_caps = {"tool_calling", "structured_output", "native_compaction", "tool_search", "native_async_tools"}
    if not isinstance(model, dict) or set(model) - model_fields or not isinstance(caps, dict) or set(caps) != bool_caps | {"context_window", "max_output_tokens"} or any(type(caps.get(key)) is not bool for key in bool_caps):
        raise ValueError("documented model fields and every explicit capability flag are required")
    if not all(isinstance(model.get(key), str) and model[key].strip() for key in ("credential_group", "price_version")):
        raise ValueError("credential group and immutable price version required")
    if type(model.get("exact_pricing")) is not bool:
        raise ValueError("exact_pricing must be explicitly supplied")
    for key in ("input_price", "output_price"):
        if type(model.get(key)) is not int or not 0 <= model[key] <= 9223372036854775807 or (not model["exact_pricing"] and model[key] == 0):
            raise ValueError("explicit nonnegative token rates or positive conservative bounds required")
    for key in ("cache_read_price", "cache_write_5m_price", "cache_write_1h_price"):
        if key in model and (type(model[key]) is not int or not 0 <= model[key] <= 9223372036854775807):
            raise ValueError("optional cache rates must be nonnegative integers")
        if r["provider"] == "anthropic" and model["exact_pricing"] and key not in model:
            raise ValueError("exact Anthropic quote requires every cache rate")
    for key in ("context_tokens", "max_output_tokens", "request_timeout_ns"):
        if not positive(model.get(key)):
            raise ValueError("explicit positive model limit required: " + key)
    if caps.get("tool_calling") is not True or not positive(caps.get("context_window")) or not positive(caps.get("max_output_tokens")):
        raise ValueError("exact tool-capable native model registry required")
    if model["context_tokens"] + model["max_output_tokens"] > caps["context_window"] or model["max_output_tokens"] > caps["max_output_tokens"]:
        raise ValueError("requested bounds exceed registered native context/output limits")
    for key in ("price_sources", "capability_sources"):
        if not isinstance(r.get(key), list) or not r[key] or not all(isinstance(source, str) and source.startswith("https://") for source in r[key]):
            raise ValueError("operator's " + key + " URLs are required; no rates or capabilities are inferred")
    worst_input = max(model.get(key, 0) for key in ("input_price", "cache_read_price", "cache_write_5m_price", "cache_write_1h_price"))
    reservation = (model["context_tokens"] * worst_input + model["max_output_tokens"] * model["output_price"] + 999999) // 1000000
    if reservation > min(r["task_budgets_microusd"].values()):
        raise ValueError("at least one task cannot afford even one conservative request reservation")
    return r


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registration", required=True)
    parser.add_argument("--platform-template", required=True)
    parser.add_argument("--runner-template", required=True)
    parser.add_argument("--python-image", required=True)
    parser.add_argument("--go-image", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)
    manifest = common.verify_corpus()
    registration = validate_registration(json.loads(Path(args.registration).read_text()))
    platform = json.loads(Path(args.platform_template).read_text())
    runner = json.loads(Path(args.runner_template).read_text())
    if runner.get("allow_test_backend", False):
        raise ValueError("real evaluation cannot use TestBackend")
    for image in (args.python_image, args.go_image):
        if not re.fullmatch(r"[a-z0-9][a-z0-9._/:+-]*@sha256:[0-9a-f]{64}", image):
            raise ValueError("explicit digest-pinned Python and Go images required")
    hashes = json.loads(common.local_output(["go", "run", str(HERE / "hash-sources.go"), str(common.CORPUS)], cwd=common.REPO, env=common.go_environment()))
    platform["sources"], runner["sources"], runner["profiles"] = {}, {}, {}
    for case in common.CASES:
        identity = common.SOURCE_PREFIX + case
        source = str(common.CORPUS / case / "source")
        platform["sources"][identity] = {"path": source, "hash": hashes[case], "profile_id": identity, "has_target": True}
        runner["sources"][identity] = source
        runner["profiles"][identity] = {"id": identity, "image": args.python_image if case.startswith("py-") else args.go_image, "verify_command": common.grade_command(case, "regression"), "target_command": common.grade_command(case, "target"), "tmpfs_executable": case.startswith("go-"), "trusted_tests_dir": str(common.CORPUS / "graders"), "memory_bytes": 256 << 20, "workspace_quota_bytes": 256 << 20, "cpus": 1, "pids": 64, "user": "1000:1000"}
    platform.pop("fake_scripts", None)
    platform["configs"] = {registration["config_id"]: {"provider": registration["provider"], "model": registration["model_id"], "max_model_rounds": registration["max_model_rounds"], "max_tool_calls": registration["max_tool_calls"], "max_cost_microusd": max(registration["task_budgets_microusd"].values()), "max_runtime_seconds": registration["max_runtime_seconds"]}}
    platform["models"] = {registration["provider"] + "/" + registration["model_id"]: registration["model_spec"]}
    platform["providers"] = {registration["provider"]: {registration["model_id"]: registration["capabilities"]}}
    output = Path(args.output).absolute()
    if output.resolve() != output:
        raise ValueError("candidate configuration directory must not contain symlinks")
    os.umask(0o077)
    output.mkdir(mode=0o700)
    for name, value in (("platform.json", platform), ("runner.json", runner), ("registration.json", registration), ("corpus-manifest.json", manifest)):
        common.save_json(output / name, value)
    common.save_json(output / "candidate.json", {"candidate_only": True, "corpus_sha256": manifest["sha256"], "source_hashes": hashes, "platform_sha256": common.file_hash(output / "platform.json"), "runner_sha256": common.file_hash(output / "runner.json"), "registration_sha256": common.file_hash(output / "registration.json")})
    print("Candidate files written to " + str(output) + "; no config installed, no service restarted, no credential read, no provider called.")


if __name__ == "__main__":
    main()
