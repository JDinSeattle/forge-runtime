#!/usr/bin/python3
"""Explicit paid-evaluation runner. Default validates locally; --collect never submits."""
import argparse
import collections
import datetime
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from urllib.parse import unquote, urlsplit
import uuid

sys.dont_write_bytecode = True
HERE = Path(__file__).absolute().parent
spec = importlib.util.spec_from_file_location("eval_common", HERE / "common.py")
common = importlib.util.module_from_spec(spec)
spec.loader.exec_module(common)
spec = importlib.util.spec_from_file_location("eval_prepare", HERE / "prepare.py")
prepare = importlib.util.module_from_spec(spec)
spec.loader.exec_module(prepare)
TERMINAL = {"completed", "failed", "cancelled", "budget_exhausted"}
BLOCKED = {"waiting_approval", "needs_reconciliation"}


def timestamp():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def seconds(start, end):
    if not start or not end:
        return None
    return (datetime.datetime.fromisoformat(end) - datetime.datetime.fromisoformat(start)).total_seconds()


def load_bundle(directory):
    manifest = common.verify_corpus()
    seal = json.loads((directory / "candidate.json").read_text())
    if manifest["sha256"] != seal["corpus_sha256"]:
        raise ValueError("candidate belongs to a different corpus")
    for kind in ("platform", "runner", "registration"):
        if common.file_hash(directory / (kind + ".json")) != seal[kind + "_sha256"]:
            raise ValueError("candidate configuration changed after preparation: " + kind)
    registration = prepare.validate_registration(json.loads((directory / "registration.json").read_text()))
    platform = json.loads((directory / "platform.json").read_text())
    if platform.get("fake_scripts") or "fake" in platform["providers"]:
        raise ValueError("fake scripts/provider cannot produce evaluation results")
    return manifest, seal, registration, platform


def audit_environment():
    raw = os.environ.get("FORGE_EVAL_AUDIT_DSN", "")
    u = urlsplit(raw)
    if u.scheme not in ("postgres", "postgresql") or u.hostname != "127.0.0.1" or u.path != "/forge" or u.query not in ("", "sslmode=disable"):
        raise ValueError("FORGE_EVAL_AUDIT_DSN must explicitly select the loopback /forge application database")
    return dict(os.environ, PGHOST=u.hostname, PGPORT=str(u.port or 5432), PGDATABASE="forge", PGUSER=unquote(u.username or ""), PGPASSWORD=unquote(u.password or ""), PGSSLMODE="disable", PGOPTIONS="-c default_transaction_read_only=on -c search_path=public")


def audit_run(tenant, run_id, directory):
    if not common.valid_id(tenant) or not common.valid_id(run_id):
        raise ValueError("invalid audit identity")
    result = subprocess.run(["psql", "-X", "-qAt", "-v", "ON_ERROR_STOP=1", "-v", "tenant=" + tenant, "-v", "run=" + run_id], input=(HERE / "audit.sql").read_text(), env=audit_environment(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=20)
    (directory / "audit.stderr").write_text(result.stderr)
    if result.returncode:
        raise RuntimeError("read-only ledger audit failed; no further task is submitted")
    result = json.loads(result.stdout)
    if not isinstance(result.get("run"), dict) or result["run"].get("id") != run_id or result["run"].get("tenant_id") != tenant:
        raise ValueError("audit role/database cannot see the exact evaluation run")
    common.save_json(directory / "ledger.json", result, replace=True)
    return result


def audit_preflight():
    query = "SELECT bool_and(has_table_privilege(current_user,t,'SELECT')) FROM (VALUES ('runs'),('model_attempts'),('quota_reservations'),('artifacts'),('run_events')) required(t);"
    result = subprocess.run(["psql", "-X", "-qAt", "-v", "ON_ERROR_STOP=1"], input=query, env=audit_environment(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=20)
    if result.returncode or result.stdout.strip() != "t":
        raise ValueError("read-only audit connection/SELECT permissions failed before any run submission")


def summarize_ledger(ledger, registration):
    expected_quote = dict(registration["model_spec"], schema_version=1, provider=registration["provider"], model=registration["model_id"])
    config = ledger["run"]["config_snapshot"]
    if config["provider"] != registration["provider"] or config["model"] != registration["model_id"]:
        raise ValueError("server accepted a different provider/model configuration")
    reservations = {row["id"]: row for row in ledger["reservations"]}
    attempts = []
    known_cost = unknown_cost = 0
    for row in ledger["attempts"]:
        if row["provider"] != registration["provider"] or row["model_id"] != registration["model_id"] or row["pricing"] != expected_quote:
            raise ValueError("attempt provider/model or immutable pricing differs from operator-approved inputs")
        reservation = reservations.pop(row["attempt_id"], None)
        charged = None
        unknown = False
        if reservation:
            if reservation["status"] == "settled" and reservation["actual_microusd"] is not None:
                charged = reservation["actual_microusd"]
                known_cost += charged
            else:
                unknown = True
                unknown_cost += reservation["microusd"]
        attempts.append({"attempt_id": row["attempt_id"], "step_seq": row["step_seq"], "attempt": row["attempt"], "status": row["status"], "error_code": row["error_code"], "provider_request_id": row["request_id"], "usage": row["usage"], "pricing": row["pricing"], "reservation": reservation, "known_ledger_cost_microusd": charged, "cost_unknown_or_unsettled": unknown, "dispatch_to_response_persisted_seconds": seconds(reservation.get("dispatched_at") if reservation else None, row["response_persisted_at"])})
    if reservations:
        raise ValueError("ledger has reservation(s) without a matching attempt")
    return {"attempts": attempts, "known_ledger_cost_microusd": known_cost, "unknown_or_unsettled_reserved_microusd": unknown_cost, "conservative_ledger_exposure_microusd": known_cost + unknown_cost, "real_provider_dispatches": sum(bool(row["reservation"] and row["reservation"]["dispatched_at"]) for row in attempts), "vendor_invoice_verified": False, "vendor_billed_cost_microusd": None}


class CLI:
    def __init__(self, client_env, output):
        env = dict(os.environ)
        entries = {}
        for line in client_env.read_text().splitlines():
            key, value = line.split("=", 1)
            if key not in {"FORGE_API_URL", "FORGE_TOKEN", "FORGE_TENANT"} or key in entries:
                raise ValueError("client env must contain only the three plain CLI entries")
            entries[key] = value
        if set(entries) != {"FORGE_API_URL", "FORGE_TOKEN", "FORGE_TENANT"} or not common.valid_id(entries["FORGE_TENANT"]):
            raise ValueError("incomplete client environment")
        u = urlsplit(entries["FORGE_API_URL"])
        if u.scheme != "http" or u.hostname != "127.0.0.1":
            raise ValueError("evaluation harness requires the explicitly configured local API")
        env.update(entries)
        self.env, self.tenant = env, entries["FORGE_TENANT"]
        self.command = [str(common.REPO / "bin/forge"), "--state-dir", str(output / "cli-receipts")]

    def call(self, *args):
        result = subprocess.run(self.command + list(args), env=self.env, cwd=common.REPO, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=30)
        if result.returncode:
            raise RuntimeError("CLI operation failed or result unknown: " + args[0] + " " + args[1])
        return json.loads(result.stdout)


def collect_task(cli, case, directory, registration, wait_seconds):
    submitted = json.loads((directory / "submission.json").read_text())
    run_id = submitted["run_id"]
    if not common.valid_id(run_id):
        raise ValueError("invalid submitted run identity")
    started = time.monotonic()
    with (directory / "events.jsonl").open("ab") as events, (directory / "watch.stderr").open("ab") as diagnostics:
        watch = subprocess.Popen(cli.command + ["run", "watch", run_id], env=cli.env, cwd=common.REPO, stdout=events, stderr=diagnostics)
        try:
            while True:
                run = cli.call("run", "get", run_id)
                if run["state"]["status"] in TERMINAL | BLOCKED or time.monotonic() - started >= wait_seconds:
                    break
                time.sleep(1)
        finally:
            watch.terminate()
            try:
                watch.wait(timeout=5)
            except subprocess.TimeoutExpired:
                watch.kill()
                watch.wait(timeout=5)
    common.save_json(directory / "run.json", run, replace=True)
    artifacts, cursor = [], ""
    while True:
        page = cli.call("run", "artifacts", run_id, "--limit", "1000", "--after", cursor)
        artifacts.extend(page)
        if len(artifacts) > 10000:
            raise ValueError("artifact collection limit reached")
        if len(page) < 1000:
            break
        if page[-1]["id"] <= cursor:
            raise ValueError("artifact cursor did not advance")
        cursor = page[-1]["id"]
    common.save_json(directory / "artifacts.json", artifacts, replace=True)
    baseline = verification = None
    wanted = {"baseline", "verification", "verification_report", "patch", "patch_manifest", "operation_receipt", "model_response", "model_request"}
    for artifact in artifacts:
        if artifact["kind"] not in wanted:
            continue
        if not common.valid_id(artifact["id"]):
            raise ValueError("invalid artifact identity")
        path = directory / artifact["id"]
        if not path.exists():
            cli.call("run", "download", artifact["id"], "--output", str(path))
        if common.file_hash(path) != artifact["sha256"]:
            raise ValueError("downloaded artifact checksum mismatch")
        if artifact["kind"] == "baseline":
            baseline = json.loads(path.read_text())
        if artifact["kind"] == "verification_report" and artifact["id"] == run["state"].get("verification_report_ref"):
            verification = json.loads(path.read_text())
    ledger = audit_run(cli.tenant, run_id, directory)
    billing = summarize_ledger(ledger, registration)
    limit = registration["task_budgets_microusd"][case]
    violation = ledger["run"]["config_snapshot"]["max_cost_microusd"] != limit or billing["conservative_ledger_exposure_microusd"] > limit
    evidence = verification["evidence"] if verification else {}
    verified = run["state"]["status"] == "completed" and run["state"]["verification_status"] == "verified" and baseline is not None and baseline.get("target_failed") is True and all(evidence.get(key) is True for key in ("trusted", "baseline_target_failed", "target_passed", "regression_passed")) and evidence.get("workspace_revision") == run["state"]["workspace_revision"] == run["state"]["verification_revision"] and billing["real_provider_dispatches"] > 0
    item = {"case": case, "run_id": run_id, "status": run["state"]["status"], "verification_status": run["state"]["verification_status"], "baseline_target_failed": baseline.get("target_failed") if baseline else None, "target_passed": evidence.get("target_passed"), "regression_passed": evidence.get("regression_passed"), "verified_repair": verified, "budget_violation": violation, "task_budget_microusd": limit, "collection_elapsed_seconds": time.monotonic() - started, "database_created_to_finished_seconds": seconds(ledger["run"]["created_at"], ledger["finished_at"]), "billing": billing}
    common.save_json(directory / "task-report.json", item, replace=True)
    return item


def aggregate(report):
    reports = report["tasks"]
    report["outcome_counts"] = dict(collections.Counter(item["status"] for item in reports))
    report["verified_repairs"] = sum(item["verified_repair"] for item in reports)
    report["planned_tasks"] = len(common.CASES)
    report["unsubmitted_or_uncollected_tasks"] = len(common.CASES) - len(reports)
    report["known_ledger_cost_microusd"] = sum(item["billing"]["known_ledger_cost_microusd"] for item in reports)
    report["unknown_or_unsettled_reserved_microusd"] = sum(item["billing"]["unknown_or_unsettled_reserved_microusd"] for item in reports)
    collected = {item["case"] for item in reports}
    committed = report.get("submitted_or_unconfirmed_task_allocations", {})
    report["submitted_or_unconfirmed_budget_ceiling_microusd"] = sum(committed.values())
    report["unaudited_allocated_ceiling_microusd"] = sum(value for case, value in committed.items() if case not in collected)
    report["conservative_accounted_exposure_microusd"] = report["known_ledger_cost_microusd"] + report["unknown_or_unsettled_reserved_microusd"] + report["unaudited_allocated_ceiling_microusd"]
    report["ledger_totals_complete"] = set(committed).issubset(collected)
    report["vendor_invoice_verified"] = False
    report["vendor_billed_cost_microusd"] = None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", required=True)
    parser.add_argument("--client-env")
    parser.add_argument("--output", required=True)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--execute", action="store_true", help="explicitly submit up to four real-provider runs within preallocated caps")
    mode.add_argument("--collect", action="store_true", help="only fetch evidence for saved run IDs; never submit, approve or retry")
    args = parser.parse_args()
    bundle, output = Path(args.bundle).absolute(), Path(args.output).absolute()
    manifest, seal, registration, platform = load_bundle(bundle)
    if not args.execute and not args.collect:
        print(json.dumps({"validated_only": True, "corpus_sha256": manifest["sha256"], "provider": registration["provider"], "model_id": registration["model_id"], "total_budget_microusd": registration["total_budget_microusd"], "task_budgets_microusd": registration["task_budgets_microusd"], "no_network_or_provider_call": True}, indent=2))
        return
    if not args.client_env:
        raise ValueError("--client-env is required for CLI operations")
    audit_environment()  # Fail before any acceptance if audit credentials are absent.
    if output.resolve() != output:
        raise ValueError("evidence output cannot contain symlinks")
    os.umask(0o077)
    if args.execute:
        output.mkdir(mode=0o700)
        common.save_json(output / "batch.json", {"created_at": timestamp(), "corpus_sha256": manifest["sha256"], "seal": seal, "registration": registration, "task_order": list(common.CASES), "allocated_total_microusd": sum(registration["task_budgets_microusd"].values()), "total_budget_microusd": registration["total_budget_microusd"]})
    batch = json.loads((output / "batch.json").read_text())
    if batch["seal"] != seal or batch["registration"] != registration:
        raise ValueError("collect must use the exact original corpus/model/prices/budget")
    cli = CLI(Path(args.client_env), output)
    audit_preflight()
    committed = {case: registration["task_budgets_microusd"][case] for case in common.CASES if (output / case / "submission-intent.json").is_file()}
    report = {"scope": "real-provider CLI and trusted-grader evaluation; finite held-out-from-demo corpus, no training-contamination claim", "started_at": batch["created_at"], "collected_at": timestamp(), "corpus_sha256": manifest["sha256"], "provider": registration["provider"], "model_id": registration["model_id"], "total_budget_microusd": batch["total_budget_microusd"], "allocated_total_microusd": batch["allocated_total_microusd"], "submitted_or_unconfirmed_task_allocations": committed, "tasks": [], "complete": False}
    try:
        for case in common.CASES:
            directory = output / case
            if args.execute:
                directory.mkdir(mode=0o700)
                source_id = common.SOURCE_PREFIX + case
                source = platform["sources"][source_id]
                project = cli.call("project", "create", "--name", source_id + "-" + uuid.uuid4().hex[:8], "--source", source_id, "--profile", source["profile_id"])
                common.save_json(directory / "project.json", project)
                intent = {"idempotency_key": str(uuid.uuid4()), "project_id": project["id"], "budget_microusd": registration["task_budgets_microusd"][case], "submitted_at": timestamp()}
                common.save_json(directory / "submission-intent.json", intent)
                report["submitted_or_unconfirmed_task_allocations"][case] = intent["budget_microusd"]
                aggregate(report)
                common.save_json(output / "report.json", report, replace=True)
                submitted = cli.call("run", "submit", project["id"], "--task-file", str(common.CORPUS / case / "task.txt"), "--base", source["hash"], "--config", registration["config_id"], "--idempotency-key", intent["idempotency_key"], "--max-cost-microusd", str(intent["budget_microusd"]), "--max-rounds", str(registration["max_model_rounds"]), "--max-tools", str(registration["max_tool_calls"]), "--max-runtime-seconds", str(registration["max_runtime_seconds"]))
                common.save_json(directory / "submission.json", submitted)
            if not (directory / "submission.json").is_file():
                report["stopped_reason"] = "missing confirmed run ID; inspect saved submission intent and CLI receipt, never allocate a new key blindly"
                break
            item = collect_task(cli, case, directory, registration, registration["max_runtime_seconds"] + 60 if args.execute else 0)
            report["tasks"].append(item)
            aggregate(report)
            common.save_json(output / "report.json", report, replace=True)
            print(json.dumps({"case": case, "run_id": item["run_id"], "status": item["status"], "verified_repair": item["verified_repair"]}), flush=True)
            if item["budget_violation"]:
                report["stopped_reason"] = "observed cap/ledger budget violation; raw billing retained and no further submission"
                break
            if item["status"] not in TERMINAL:
                report["stopped_reason"] = "run requires operator attention; no approval, cancellation, retry or next-task submission was performed"
                break
        report["complete"] = len(report["tasks"]) == len(common.CASES) and all(item["status"] in TERMINAL and not item["budget_violation"] for item in report["tasks"])
    except Exception as error:
        report["stopped_reason"] = type(error).__name__ + ": " + str(error)
        raise
    finally:
        aggregate(report)
        common.save_json(output / "report.json", report, replace=True)


if __name__ == "__main__":
    main()
