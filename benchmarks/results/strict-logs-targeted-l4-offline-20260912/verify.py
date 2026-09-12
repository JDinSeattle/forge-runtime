"""Portable evidence verification only; does not run tests or access live resources."""
from pathlib import Path
import collections
import hashlib
import json

ROOT = Path(__file__).resolve().parent
sha = lambda data: hashlib.sha256(data).hexdigest()
read = lambda path: (ROOT / path).read_bytes()
j = lambda path: json.loads(read(path))
checks = []


def check(condition, label):
    if not condition:
        raise AssertionError(label)
    checks.append(label)


index = j("archive-manifest.json")
check(index["final_committed_revision"] == "1b4e0b482fe14aaf834da8e6968b15e4afab0d9a" and len(index["files"]) == 26, "exact archive source inventory")
for row in index["files"]:
    data = read(row["archived_file"])
    check(row["projection"] is None and sha(data) == row["source_sha256"] == row["archived_sha256"] and len(data) == row["bytes"], "byte-identical source " + row["archived_file"])

freeze1, freeze2 = j("freeze-v1.json"), j("freeze-v2.json")
check(set(freeze1) == set(freeze2) and len(freeze1) == 7, "same seven reviewed paths")
prefix = "/home/postedism/Desktop/mini claude code/forge-runtime/"
for version, frozen in [(1, freeze1), (2, freeze2)]:
    for source, expected in frozen.items():
        check(source.startswith(prefix), "exact source root")
        relative = source.removeprefix(prefix)
        check(sha(read("source-v%d/" % version + relative + ".txt")) == expected, "frozen source v%d " % version + relative)
changed = {path.removeprefix(prefix) for path in freeze1 if freeze1[path] != freeze2[path]}
check(changed == {"scripts/faults/application/strict_logs_l4_continuation_test.go", "docs/strict-logs-combined-evidence.md"}, "bounded two-file v2 delta")

initial = read("author/python-first-failed.log").decode()
check("Ran 35 tests" in initial and "FAILED (errors=1)" in initial and "ValueError: explicit L4 preparation or retained evidence already exists" in initial, "original directory-fixture error retained")
check("test_l4_preparation_is_exclusive_and_targeted_case_requires_completed_recovery" in initial, "original failing test identity")
corrected = read("author/python-corrected.log").decode()
check("Ran 35 tests" in corrected and corrected.strip().endswith("OK") and "FAILED" not in corrected, "corrected author Python result")


def test_log(path, expected):
    entries = [json.loads(line) for line in read(path).splitlines() if line.strip()]
    counts = collections.Counter(entry["Action"] for entry in entries if "Test" in entry and entry["Action"] in {"pass", "skip", "fail"})
    check(dict(counts) == expected, "test counts " + path)
    check(entries[-1]["Action"] == "pass" and "Test" not in entries[-1] and not any(entry["Action"] == "fail" for entry in entries), "package success " + path)
    return dict(counts)


author_full = test_log("author/race-initial.jsonl", {"pass": 104, "skip": 4})
author_delta = test_log("author/race-delta.jsonl", {"pass": 1})
independent = test_log("independent/delta-race-v2.jsonl", {"pass": 8})
check(read("author/vet.log") == b"", "empty original vet output preserved, not a standalone exit-code proof")
python = read("independent/delta-python-v2.log").decode()
check("Ran 2 tests" in python and python.strip().endswith("OK") and "FAILED" not in python, "two independently executed Python cases")

review1, review2 = j("independent/review-v1.json"), j("independent/review-v2.json")
check(sum(finding["priority"] == "P2" for finding in review1["findings"]) == 2 and review1["verdict"] == "P2 revisions required before acceptance", "original two independent P2 findings retained")
check(review2["supersedes_review"]["sha256"] == sha(read("independent/review-v1.json")) and review2["supersedes_review"]["original_report_preserved"] is True, "v2 explicitly supersedes original review")
check(review1["source_freeze_sha256"] == sha(read("freeze-v1.json")) and review2["source_freeze_sha256"] == sha(read("freeze-v2.json")) and review2["source_sha256"] == freeze2, "review/source version binding")
check(review2["source_hashes_before_after_match"] is True and review2["verdict"].startswith("PASS: no remaining P1/P2"), "independent v2 verdict with unchanged source")
check(review2["independent_validation"]["log_sha256"]["delta-race-v2.jsonl"] == sha(read("independent/delta-race-v2.jsonl")) and review2["independent_validation"]["log_sha256"]["delta-python-v2.log"] == sha(read("independent/delta-python-v2.log")), "independent logs bound to report")
v2 = read("source-v2/scripts/faults/application/strict_logs_l4_continuation_test.go.txt").decode()
check(v2.index("SELECT oid::bigint FROM pg_catalog.pg_namespace") < v2.index('schema := pgx.Identifier'), "namespace identity checked before business-table queries")
check("report.SchemaOID != 852843" in v2 and "liveOID != report.SchemaOID" in v2 and ".effects WHERE status NOT IN ('succeeded','failed','cancelled')" in v2, "final OID and complete effect-state literals")
check("status<>'settled' OR NOT request_slot_released" in v2 and ".workspace_cleanup WHERE phase<>'released'" in v2, "slot release and workspace cleanup guards")

print(json.dumps({
    "archive_verified": True,
    "checks": len(checks),
    "source_files_per_version": 7,
    "byte_identical_preserved_files": 26,
    "final_committed_revision": index["final_committed_revision"],
    "original_python_failure_preserved": True,
    "author_full_race": author_full,
    "author_delta_race": author_delta,
    "author_corrected_python_passes": 35,
    "independent_delta_race": independent,
    "independent_python_passes": 2,
    "independent_review_p2_before": 2,
    "independent_review_p2_after": 0,
    "actual_recovery_or_L4_execution": False,
    "limitations": [
        "The initial Python error predates freeze v1. Its raw stack/result is preserved; an exact pre-fix source snapshot was not available and was not reconstructed.",
        "Author logs are retained results associated with the author's reported revisions, not independent clean-build attestations. Author delta tested the guard before the final effect SQL literal broadened to all nonterminal statuses. Independent v2 compilation includes that final literal; no live SQL was exercised.",
        "The empty vet log alone cannot establish its exit status; exit0 was reported by the author.",
        "Recovery producer was then WIP in another worktree and is outside this seven-file freeze. Final combined build must also bind its eventual frozen source.",
        "This verifier reads archive files only. It does not rerun suites, touch volumes/services/databases/credentials or call paid models, and does not close actual L4 acceptance."
    ]
}, indent=2, sort_keys=True))
