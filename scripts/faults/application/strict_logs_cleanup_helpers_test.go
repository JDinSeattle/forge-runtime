//go:build linux

package applicationfaults

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/artifact"
	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/runner"
)

func slCleanupTestJournal() slJournal {
	j := slJournal{Identity: slCleanupUUID, Version: 5, Tables: map[string][]map[string]any{}}
	for _, table := range []string{"workspaces", "operations", "volume_slots", "volume_leases", "operation_logs", "log_runs", "runner_artifacts"} {
		j.Tables[table] = []map[string]any{}
	}
	for i := 0; i < 4; i++ {
		id := fmt.Sprintf("old-%d", i)
		released := int64(1)
		if i == 0 {
			id = slCleanupID
			released = 0
		}
		j.Tables["workspaces"] = append(j.Tables["workspaces"], map[string]any{"id": id, "run_id": id, "tenant_id": "strict-log-fixture", "source_id": "lifecycle", "profile_id": "lifecycle-python", "baseline_hash": slCleanupTree, "epoch": int64(1), "revision": int64(1), "active_operation": "", "released": released, "stopped": int64(0), "adopting": int64(0)})
		j.Tables["volume_slots"] = append(j.Tables["volume_slots"], map[string]any{"id": fmt.Sprintf("slot-%03d", i+1), "spec_json": fmt.Sprintf("immutable-spec-%d", i)})
		j.Tables["volume_leases"] = append(j.Tables["volume_leases"], map[string]any{"workspace_id": id, "slot_id": fmt.Sprintf("slot-%03d", i+1), "tenant_id": "strict-log-fixture", "run_id": id, "epoch": int64(1), "released": released})
	}
	for i := 0; i < 44; i++ {
		wid := domain.ID("old-1")
		id := fmt.Sprintf("old-op-%02d", i)
		if i < 4 {
			wid = slCleanupID
			id = fmt.Sprintf("%s-op-%02d", slCleanupID, i)
		}
		req := runner.OperationRequest{WorkspaceRequest: runner.WorkspaceRequest{TenantID: "strict-log-fixture", RunID: wid, WorkspaceID: wid, Epoch: 1}, OperationID: domain.ID(id), ExpectedRevision: 1, Kind: "run_command", Args: json.RawMessage(`{"command":["true"]}`)}
		j.Tables["operations"] = append(j.Tables["operations"], map[string]any{"id": id, "status": "failed", "request_json": string(slJSON(req)), "result_json": "immutable-result", "receipt_json": "immutable-receipt", "before_hash": slCleanupTree, "after_hash": slCleanupTree, "after_revision": int64(1)})
		if i < 4 {
			j.Tables["operation_logs"] = append(j.Tables["operation_logs"], map[string]any{"operation_id": id, "container_id": fmt.Sprintf("exact-%d", i), "policy_json": "immutable-policy", "cleanup_state": "retained"})
		}
	}
	j.Tables["log_runs"] = append(j.Tables["log_runs"], map[string]any{"tenant_id": "strict-log-fixture", "run_id": slCleanupID, "operations": int64(4), "reserved_bytes": int64(2097152)})
	return j
}
func slCleanupTestClone(j slJournal) slJournal {
	var out slJournal
	_ = json.Unmarshal(slJSON(j), &out)
	return out
}
func slCleanupTestStopped(j *slJournal) {
	j.Tables["workspaces"][0]["stopped"], j.Tables["workspaces"][0]["adopting"] = int64(1), int64(1)
}
func slCleanupTestReleased(j *slJournal) {
	slCleanupTestStopped(j)
	j.Tables["workspaces"][0]["released"] = int64(1)
	j.Tables["volume_leases"][0]["released"] = int64(1)
	for _, r := range j.Tables["operation_logs"] {
		r["cleanup_state"] = "removed"
	}
}

func TestStrictLogsCleanupJournalBoundaries(t *testing.T) {
	before := slCleanupTestJournal()
	if e := slCleanupInitial(before); e != nil {
		t.Fatal(e)
	}
	for _, stage := range []string{"original", "stop", "partial-container-remove", "workspace-released-storage-pending", "released"} {
		t.Run(stage, func(t *testing.T) {
			j := slCleanupTestClone(before)
			switch stage {
			case "stop":
				slCleanupTestStopped(&j)
			case "partial-container-remove":
				slCleanupTestStopped(&j)
				j.Tables["operation_logs"][0]["cleanup_state"] = "removing"
			case "workspace-released-storage-pending":
				slCleanupTestStopped(&j)
				j.Tables["workspaces"][0]["released"] = int64(1)
			case "released":
				slCleanupTestReleased(&j)
			}
			if e := slCleanupJournal(before, j, stage != "original"); e != nil {
				t.Fatal(e)
			}
			if stage != "original" && slCleanupJournal(before, j, false) == nil {
				t.Fatal("changed state admitted without durable intent")
			}
			needs := stage == "partial-container-remove" || stage == "workspace-released-storage-pending" || stage == "released"
			if slCleanupNeedsReleaseProof(j) != needs {
				t.Fatal("release-stage proof gate differs")
			}
			if stage == "released" {
				if e := slCleanupReleased(j); e != nil {
					t.Fatal(e)
				}
			} else if slCleanupReleased(j) == nil {
				t.Fatal("incomplete release accepted")
			}
		})
	}
	negatives := map[string]func(*slJournal){
		"uuid":     func(j *slJournal) { j.Identity = "other" },
		"version":  func(j *slJournal) { j.Version = 6 },
		"epoch":    func(j *slJournal) { j.Tables["workspaces"][0]["epoch"] = int64(2) },
		"revision": func(j *slJournal) { j.Tables["workspaces"][0]["revision"] = int64(2) },
		"unknown":  func(j *slJournal) { j.Tables["operations"][0]["status"] = "unknown" },
		"receipt":  func(j *slJournal) { j.Tables["operations"][0]["receipt_json"] = "replacement" },
		"request":  func(j *slJournal) { j.Tables["operations"][0]["request_json"] = "replacement" },
		"result":   func(j *slJournal) { j.Tables["operations"][0]["result_json"] = "replacement" },
		"extra_operation": func(j *slJournal) {
			j.Tables["operations"] = append(j.Tables["operations"], map[string]any{"id": "new"})
		},
		"other_workspace":    func(j *slJournal) { j.Tables["workspaces"][1]["released"] = int64(0) },
		"other_volume":       func(j *slJournal) { j.Tables["volume_leases"][1]["epoch"] = int64(2) },
		"volume_identity":    func(j *slJournal) { j.Tables["volume_slots"][0]["spec_json"] = "replacement" },
		"budget_refund":      func(j *slJournal) { j.Tables["log_runs"][0]["reserved_bytes"] = int64(0) },
		"container_identity": func(j *slJournal) { j.Tables["operation_logs"][0]["container_id"] = "replacement" },
		"log_policy":         func(j *slJournal) { j.Tables["operation_logs"][0]["policy_json"] = "replacement" },
		"cleanup_state":      func(j *slJournal) { j.Tables["operation_logs"][0]["cleanup_state"] = "unknown" },
		"artifact_scope": func(j *slJournal) {
			ref := artifact.Ref{TenantID: "other", RunID: slCleanupID, Kind: "workspace_snapshot", ObjectKey: "wrong"}
			j.Tables["runner_artifacts"] = append(j.Tables["runner_artifacts"], map[string]any{"object_key": "wrong", "ref_json": string(slJSON(ref))})
		},
	}
	for name, mutate := range negatives {
		t.Run(name, func(t *testing.T) {
			j := slCleanupTestClone(before)
			mutate(&j)
			if slCleanupJournal(before, j, true) == nil {
				t.Fatal("unsafe journal mutation accepted")
			}
		})
	}
}
func slCleanupTestArtifacts() (runner.StopReceipt, []byte, runner.Snapshot, []byte) {
	w := runner.Workspace{TenantID: "strict-log-fixture", RunID: slCleanupID, ID: slCleanupID, Epoch: 1, Revision: 1, SourceID: "lifecycle", ProfileID: "lifecycle-python", BaselineHash: slCleanupTree, Stopped: true, Adopting: true}
	stop := runner.StopReceipt{Workspace: w, NoActiveOperations: true}
	stopRaw := slJSON(stop)
	ref := func(kind string, b []byte) artifact.Ref {
		return artifact.Ref{TenantID: "strict-log-fixture", RunID: slCleanupID, Kind: kind, ObjectKey: "strict-log-fixture/" + slCleanupID + "/" + slSHA(b), SHA256: slSHA(b), Size: int64(len(b))}
	}
	stop.Ref = ref("workspace_stop", stopRaw)
	snapRaw := slJSON(map[string]any{"workspace": w, "files": map[string]any{"app.py": map[string]any{"sha256": slSHA([]byte(slCleanupSource)), "content": []byte(slCleanupSource), "executable": false}}})
	return stop, stopRaw, runner.Snapshot{Workspace: w, Hash: slCleanupTree, Artifact: ref("workspace_snapshot", snapRaw)}, snapRaw
}
func TestStrictLogsCleanupSnapshotProof(t *testing.T) {
	stop, stopRaw, snap, snapRaw := slCleanupTestArtifacts()
	if e := slCleanupStop(stop, stopRaw); e != nil {
		t.Fatal(e)
	}
	if e := slCleanupSnapshot(snap, snapRaw); e != nil {
		t.Fatal(e)
	}
	tests := map[string]func(*runner.Snapshot, *[]byte){
		"other_run":    func(s *runner.Snapshot, _ *[]byte) { s.Artifact.RunID = "other" },
		"other_kind":   func(s *runner.Snapshot, _ *[]byte) { s.Artifact.Kind = "operation_log" },
		"digest":       func(s *runner.Snapshot, _ *[]byte) { s.Artifact.SHA256 = "wrong" },
		"length":       func(s *runner.Snapshot, _ *[]byte) { s.Artifact.Size++ },
		"epoch":        func(s *runner.Snapshot, _ *[]byte) { s.Workspace.Epoch++ },
		"revision":     func(s *runner.Snapshot, _ *[]byte) { s.Workspace.Revision++ },
		"still_active": func(s *runner.Snapshot, _ *[]byte) { s.Workspace.ActiveOperation = "op" },
		"released":     func(s *runner.Snapshot, _ *[]byte) { s.Workspace.Released = true },
		"changed_code": func(s *runner.Snapshot, b *[]byte) {
			*b = bytes.ReplaceAll(*b, []byte("ZGVmIGNsYW1w"), []byte("eHh4IGNsYW1w"))
			s.Artifact.SHA256 = slSHA(*b)
			s.Artifact.Size = int64(len(*b))
			s.Artifact.ObjectKey = "strict-log-fixture/" + slCleanupID + "/" + s.Artifact.SHA256
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			s, b := snap, bytes.Clone(snapRaw)
			mutate(&s, &b)
			if slCleanupSnapshot(s, b) == nil {
				t.Fatal("unverified snapshot accepted")
			}
		})
	}
	stop.NoActiveOperations = false
	if slCleanupStop(stop, stopRaw) == nil {
		t.Fatal("incomplete stop accepted")
	}
}
func TestStrictLogsCleanupMinimalGrant(t *testing.T) {
	s, e := runner.NewSigner(bytes.Repeat([]byte{7}, 32))
	if e != nil {
		t.Fatal(e)
	}
	now := time.Now()
	r, e := slCleanupGrant(s, now)
	if e != nil {
		t.Fatal(e)
	}
	for _, permission := range []string{"inspect", "cancel", "snapshot", "release"} {
		if e = s.Verify(r.Grant, r, permission, now); e != nil {
			t.Fatal(e)
		}
	}
	for _, permission := range []string{"prepare", "execute", "verify", "adopt"} {
		if s.Verify(r.Grant, r, permission, now) == nil {
			t.Fatalf("unexpected permission %s", permission)
		}
	}
	wrong := r
	wrong.WorkspaceID = "other"
	if s.Verify(r.Grant, wrong, "release", now) == nil {
		t.Fatal("cross-workspace grant")
	}
	wrong = r
	wrong.Epoch++
	if s.Verify(r.Grant, wrong, "release", now) == nil {
		t.Fatal("cross-epoch grant")
	}
	if s.Verify(r.Grant, r, "release", now.Add(30*time.Second)) == nil {
		t.Fatal("expired grant accepted")
	}
}
func TestStrictLogsCleanupImmutableStagesAndManifest(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "stage.json")
	value := map[string]any{"stage": "snapshot"}
	if e := slCleanupExisting(p, value); e == nil {
		t.Fatal("missing stage accepted")
	}
	if e := slCleanupSave(p, value); e != nil {
		t.Fatal(e)
	}
	if e := slCleanupSave(p, value); e != nil {
		t.Fatal(e)
	}
	if e := slCleanupExisting(p, value); e != nil {
		t.Fatal(e)
	}
	if slCleanupSave(p, map[string]any{"stage": "replacement"}) == nil {
		t.Fatal("immutable stage overwritten")
	}
	if e := slCleanupMakeManifest(dir); e != nil {
		t.Fatal(e)
	}
	if e := slCleanupManifest(dir, "", map[string]string{}); e != nil {
		t.Fatal(e)
	}
	if e := slWrite(filepath.Join(dir, "unlisted"), []byte("extra")); e != nil {
		t.Fatal(e)
	}
	if slCleanupManifest(dir, "", map[string]string{}) == nil {
		t.Fatal("unlisted evidence accepted")
	}
	linked := t.TempDir()
	if e := os.Symlink(p, filepath.Join(linked, "stage.json")); e != nil {
		t.Fatal(e)
	}
	if slCleanupWrite(filepath.Join(linked, "stage.json"), []byte("replacement")) == nil {
		t.Fatal("symlink stage accepted")
	}
}
func TestStrictLogsCleanupArchiveRequiresBytesAndPins(t *testing.T) {
	dir := t.TempDir()
	objects := t.TempDir()
	c := slRunnerConfig{ArtifactRoot: objects}
	stop, sb, snap, b := slCleanupTestArtifacts()
	j := slCleanupTestJournal()
	slCleanupTestStopped(&j)
	for _, x := range []struct {
		name  string
		ref   artifact.Ref
		raw   []byte
		value any
	}{{"stop", stop.Ref, sb, stop}, {"snapshot", snap.Artifact, b, snap}} {
		p := filepath.Join(objects, x.ref.ObjectKey)
		if e := os.MkdirAll(filepath.Dir(p), 0700); e != nil {
			t.Fatal(e)
		}
		if e := slWrite(p, x.raw); e != nil {
			t.Fatal(e)
		}
		if e := slCleanupSave(filepath.Join(dir, x.name+".json"), x.value); e != nil {
			t.Fatal(e)
		}
		if e := slCleanupWrite(filepath.Join(dir, x.name+".bytes"), x.raw); e != nil {
			t.Fatal(e)
		}
		j.Tables["runner_artifacts"] = append(j.Tables["runner_artifacts"], map[string]any{"object_key": x.ref.ObjectKey, "ref_json": string(slJSON(x.ref))})
	}
	if _, _, e := slCleanupArchive(dir, c, j); e != nil {
		t.Fatal(e)
	}
	bad := slCleanupTestClone(j)
	bad.Tables["runner_artifacts"] = bad.Tables["runner_artifacts"][:1]
	if _, _, e := slCleanupArchive(dir, c, bad); e == nil {
		t.Fatal("missing durable snapshot pin accepted")
	}
	if e := os.Remove(filepath.Join(dir, "snapshot.bytes")); e != nil {
		t.Fatal(e)
	}
	if _, _, e := slCleanupArchive(dir, c, j); e == nil {
		t.Fatal("release allowed without archived snapshot bytes")
	}
}

// Read-only optional validation against retained originals. It does not read the
// signing key, open SQLite, call a socket or run any production process.
func TestStrictLogsCleanupArchivedInputs(t *testing.T) {
	scope := os.Getenv("FORGE_STRICT_LOGS_READONLY_SCOPE")
	if scope == "" {
		t.Skip("optional retained-file verification")
	}
	var a slAcceptance
	var c slRunnerConfig
	if e := slReadJSON(filepath.Join(scope, "acceptance-02.json"), &a); e != nil {
		t.Fatal(e)
	}
	if e := slReadJSON(a.RunnerConfig, &c); e != nil {
		t.Fatal(e)
	}
	in, e := slCleanupInputs(a, c)
	if e != nil {
		t.Fatal(e)
	}
	if e = slCleanupJournal(in.Before, slCleanupTestClone(in.Before), false); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 4; i++ {
		var op runner.Operation
		p := filepath.Join(scope, "evidence", "logs-01", fmt.Sprintf("L3-bytes-op-%02d-operation.json", i))
		if e = slReadJSON(p, &op); e != nil {
			t.Fatal(e)
		}
		raw, e := slCleanupObject(c, op.Receipt)
		if e != nil {
			t.Fatal(e)
		}
		archive, e := slRead(filepath.Join(scope, "evidence", "logs-01", fmt.Sprintf("L3-bytes-op-%02d-receipt.json", i)), 1<<20)
		if e != nil {
			t.Fatal(e)
		}
		if !bytes.Equal(raw, archive) {
			t.Fatal("retained original receipt bytes differ")
		}
	}
	t.Logf("verified %d immutable source/evidence files; UUID %s; no live operations", len(in.Hashes), in.Before.Identity)
}

func TestStrictLogsCleanupDirectoryDurabilityBeforeExecution(t *testing.T) {
	for _, failure := range []string{"cleanup-parent", "invocation-parent", "none"} {
		t.Run(failure, func(t *testing.T) {
			parent := t.TempDir()
			dir := filepath.Join(parent, "cleanup")
			calls := []string{}
			attempt, e := slCleanupPrepareDirectory(dir, func(p string) error {
				calls = append(calls, p)
				if (failure == "cleanup-parent" && p == parent) || (failure == "invocation-parent" && p == dir) {
					return fmt.Errorf("injected fsync failure")
				}
				return slCleanupSyncDirectory(p)
			})
			// The helper is the gate before key loading/process/RPC setup. Neither
			// failed sync may return an executable invocation path to that setup.
			if failure != "none" {
				if e == nil || attempt != "" {
					t.Fatal("failed sync admitted execution")
				}
			} else if e != nil || attempt == "" {
				t.Fatal("durable directory preparation failed", e)
			}
			if len(calls) == 0 || calls[0] != parent {
				t.Fatal("cleanup parent must be synced first")
			}
			entries, err := os.ReadDir(dir)
			if err != nil {
				t.Fatal(err)
			}
			if failure == "cleanup-parent" {
				if len(calls) != 1 || len(entries) != 0 {
					t.Fatal("invocation created before parent durability")
				}
			} else if len(calls) != 2 || calls[1] != dir || len(entries) != 1 {
				t.Fatal("invocation directory entry was not synced")
			}
		})
	}
}
