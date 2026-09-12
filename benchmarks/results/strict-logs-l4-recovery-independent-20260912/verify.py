"""Verify archived review evidence only. No tests, subprocesses or live resources."""
from pathlib import Path
import collections
import datetime
import hashlib
import json

ROOT = Path(__file__).resolve().parent
sha = lambda data: hashlib.sha256(data).hexdigest()
read = lambda name: (ROOT / name).read_bytes()
j = lambda name: json.loads(read(name))
instant = lambda value: datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
checks = []


def check(condition, label):
    if not condition:
        raise AssertionError(label)
    checks.append(label)


index = j("archive-manifest.json")
check(len(index["files"]) == 31 and index["byte_identical_files"] == 31, "exact preserved file count")
for row in index["files"]:
    data = read(row["archived_path"])
    check(row["projection"] is None and sha(data) == row["source_sha256"] == row["archived_sha256"] and len(data) == row["bytes"], "byte-identical source " + row["archived_path"])
manifest = j("raw/independent-manifest.json")
check(sha(read("raw/independent-manifest.json")) == index["original_manifest_sha256"] == "607d1d7b6c56212f32b8590d76578fcde09a6258dc44f65d258e71fe739a671d", "pinned original manifest bytes")
check(len(manifest) == 28 and {str(p.relative_to(ROOT / "raw")) for p in (ROOT / "raw").rglob("*") if p.is_file()} == set(manifest) | {"independent-manifest.json"}, "original 28-member closed inventory")
for name, expected in manifest.items():
    check(sha(read("raw/" + name)) == expected, "original manifest member " + name)
check(read("review-v2.json") == read("raw/delta-v2/review-v2.json") and sha(read("review-v2.json")) == index["review_v2_sha256"] == "fd2374794f93c386ea5dd0fdb94dea2e562cefa304dcac5fa83c101d4b139ef0", "outside and inside v2 report exactly match")

review1, review2 = j("raw/review-v1.json"), j("review-v2.json")
check(review1["status"] == "TWO_CONFIRMED_P2_BEFORE_FIX" and {finding["id"] for finding in review1["findings"]} == {"cleanup-total-vs-unreleased", "sql-lease-overlay-vs-snapshot"}, "original two failures preserved")
check(review2["status"] == "PASS_SCOPED_OFFLINE_REVIEW" and review2["remaining_confirmed_P1_P2"] == [] and {finding["id"] for finding in review2["closed_findings"]} == {finding["id"] for finding in review1["findings"]}, "v2 closes the same two findings")
check(review2["reviewed_commit"] == "0cc916cdeb9fd6def5ce5dca198924152cf37ce5" and review2["original_reviewed_commit"] == review1["commit"] == "e9793ea654d4c2e43d410eca8fab3c1a65e4fe98", "two reviewed commit identities")
for report, prefix in [(review1, "raw/"), (review2, "raw/delta-v2/")]:
    for name, expected in report["source_sha256"].items():
        check(sha(read(prefix + Path(name).name + ".txt")) == expected, "reviewed source " + prefix + name)
check(j("raw/delta-v2/identity.json")["source_sha256"] == review2["source_sha256"] and review2["source_unchanged_after_tests"] is True, "v2 source identity and recorded stability")


def test_counts(path, passes):
    entries = [json.loads(line) for line in read(path).splitlines() if line.strip()]
    counts = collections.Counter(entry["Action"] for entry in entries if "Test" in entry and entry["Action"] in {"pass", "skip", "fail"})
    check(dict(counts) == {"pass": passes, "skip": 1}, "ordinary/race finite counts " + path)
    check([entry["Test"] for entry in entries if entry["Action"] == "skip"] == ["TestStrictLogsL4Recovery"], "actual recovery remained opt-in skipped " + path)
    check(entries[-1]["Action"] == "pass" and not any(entry["Action"] == "fail" for entry in entries), "package result " + path)
    return dict(counts)


before_ordinary = test_counts("raw/ordinary.log", 38)
before_race = test_counts("raw/race.log", 38)
after_ordinary = test_counts("raw/delta-v2/ordinary.log", 58)
after_race = test_counts("raw/delta-v2/race.log", 58)
for prefix, command_file in [("raw/", "raw/commands-v1.json"), ("raw/delta-v2/", "raw/delta-v2/commands.json")]:
    commands = j(command_file)
    check("FORGE_RUN_STRICT_LOGS_L4_RECOVERY" not in commands["env"] and commands["env"]["GOPROXY"] == "off" and commands["env"]["GOSUMDB"] == "off", "offline opt-in/env boundary " + prefix)
    for command in commands["commands"]:
        check(sha(read(prefix + command["name"] + ".log")) == command["sha256"], "command/log binding " + prefix + command["name"])
        check(command["code"] == (1 if command["name"] == "lease-before-negative" else 0), "recorded exit status " + prefix + command["name"])
check(read("raw/vet.log") == read("raw/delta-v2/vet.log") == b"", "original empty vet logs and command exit records retained")

before_log, after_log = read("raw/lease-before-negative.log").decode(), read("raw/delta-v2/lease-after.log").decode()
check("--- FAIL: TestIndependentL4UnchangedSQLLeaseProjection" in before_log and "original snapshot changed since retained observation" in before_log, "original saved-SQL counterexample fails")
check("--- PASS: TestIndependentL4UnchangedSQLLeaseProjection" in after_log and "--- FAIL" not in after_log, "same saved-SQL counterexample passes after correction")
for prefix, path in [("raw/", "raw/overlay-v1.json"), ("raw/delta-v2/", "raw/delta-v2/overlay.json")]:
    replacements = j(path)["Replace"]
    check(len(replacements) == 1 and next(iter(replacements)).endswith("/strict_logs_l4_recovery_offline_test.go") and next(iter(replacements.values())).endswith("/lease-counterexample_test.go.txt"), "bounded original overlay mapping " + prefix)
    check(b"TestIndependentL4UnchangedSQLLeaseProjection" in read(prefix + "lease-counterexample_test.go.txt"), "counterexample source retained " + prefix)

lease_manifest = j("raw/lease-manifest.json")
for original, archived in [("observer.py.txt", "lease-observer.py.txt"), ("postgres.json", "lease-postgres.json"), ("report.json", "lease-report.json")]:
    check(sha(read("raw/" + archived)) == lease_manifest[original], "original saved observer member " + original)
lease, postgres = j("raw/lease-report.json"), j("raw/lease-postgres.json")
check(len(postgres["runs"]) == 1 and int(postgres["schema_oid"]) == lease["schema_oid"] == 852843 and postgres["schema"] == "appfault_strictlogs_zg4ub4tnaruvpaojcj5dmmeqpc", "exact recorded private SQL authority")
row = postgres["runs"][0]
check(row["id"] == lease["run_id"] == "run_DKT2OOLEVCKYXHW5NGNS7BKBHJ" and row["tenant"] == "sl-L4-xsra53ckdacsus345hmddtehsj" and row["epoch"] == 2 and row["version"] == 9 and row["state"] == "running" and row["owner"] == "sl-L4-0", "saved lease row identity")
sql_until, snapshot_until = instant(row["sql_lease_until"]), instant(row["snapshot"]["lease"]["until"])
check(sql_until == instant(lease["sql_lease_until"]) and snapshot_until == instant(lease["snapshot_lease_until"]) and sql_until > snapshot_until, "independent raw SQL versus snapshot lease distinction")
check((sql_until - snapshot_until).total_seconds() == 12.959337, "recorded heartbeat lease difference")
for log in [before_log, after_log]:
    check("2026-09-12T18:24:59.737432Z" in log and "2026-09-12T11:24:46.778095-07:00" in log, "same recorded lease sample in before/after logs")
for suffix, path in [("forge-observe-l4-lease.py", "raw/lease-observer.py.txt"), ("isolated_state.py", "context/observer-isolated-state.py.txt")]:
    expected = next(value for source, value in lease["source_hashes"].items() if source.endswith("/" + suffix))
    check(sha(read(path)) == expected, "recorded observer source " + suffix)
cleanup = j("raw/evidence-recompute-v1.json")
check(cleanup["retained_old_unreleased_cleanup_count"] == 0 and len(cleanup["actual_l1_cleanup_rows"]) == 1 and cleanup["actual_l1_cleanup_rows"][0]["phase"] == "released" and cleanup["actual_l1_cleanup_rows"][0]["run_id"] == "run_FSOSGYSAML5QZCYDZOP3TER3JI", "preserved existing released L1 cleanup evidence")

print(json.dumps({
    "archive_verified": True,
    "checks": len(checks),
    "original_manifest_members": 28,
    "byte_identical_preserved_files": 31,
    "original_v1_p2_findings": 2,
    "v2_remaining_p1_p2": 0,
    "before_ordinary": before_ordinary,
    "before_race": before_race,
    "after_ordinary": after_ordinary,
    "after_race": after_race,
    "saved_real_lease_overlay_before": "FAIL preserved",
    "saved_real_lease_overlay_after": "PASS preserved",
    "lease_difference_seconds": 12.959337,
    "historical_lease_sample_at": postgres["observed_at"],
    "actual_recovery_execution": False,
    "limits": [
        "The original reviewer executed offline source/temp-file tests using a saved read-only SQL observation; these logs are not a fresh database or Docker execution.",
        "This archivist rehashed the 28 original members, report and pinned source context, and parsed logs/sample facts; no prior test suite, actual recovery, provider or live resource was invoked.",
        "Upstream author archives, 614 saved input checks and production-diff statements remain attributed to the preserved independent review; this portable verifier does not access those external paths.",
        "Absolute historical command/overlay paths are intentionally unchanged. Source .txt files are evidence; run only this relative-path verifier for portable validation.",
        "Successful recovery would only close retained work. It would not reclassify the original failed ENOSPC attempt or independently complete S12.9/paid evaluation gates."
    ]
}, indent=2, sort_keys=True))
