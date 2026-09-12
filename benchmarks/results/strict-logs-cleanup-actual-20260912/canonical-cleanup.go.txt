//go:build linux

package applicationfaults

// An explicit, one-workspace fixture cleanup. It opens no PostgreSQL connection,
// starts no worker, and never prepares, executes, verifies or adopts work.
import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/artifact"
	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/runner"
	"github.com/JDinSeattle/forge-runtime/internal/sandbox"
	"golang.org/x/sys/unix"
)

const (
	slCleanupID            = "sl-L3-bytes-46djebzw3ifph2ui7nesemhkz6"
	slCleanupUUID          = "b010dd7440a54c58601967342f397faf686b7e306061af9f49809ee342b0ec8e"
	slCleanupTree          = "8b0fc272ee30b0c4aa6f0b1d286033c5b1519ff2d6f019e3c962c45dc4123357"
	slCleanupFailedSHA     = "3ff0f3548c756214e9bdd41032c749d1b873d5322e680baac3a5850a125cf15d"
	slCleanupHistoricalSHA = "f41acf98de6b4c3a0a36ac1e025d9ec6434c0cb51daa00228ac663566bd3a76b"
	slCleanupSource        = "def clamp(v, lo, hi):\n    return v\n"
)

type slCleanupInput struct {
	Historical slAcceptance
	Before     slJournal
	Hashes     map[string]string
}
type slCleanupReport struct {
	Passed                  bool              `json:"passed"`
	JournalUUID             string            `json:"journal_uuid"`
	WorkspaceID             string            `json:"workspace_id"`
	Epoch                   uint64            `json:"epoch"`
	Revision                uint64            `json:"revision"`
	Released                bool              `json:"released"`
	SnapshotVerified        bool              `json:"snapshot_verified"`
	FailedManifestSHA       string            `json:"failed_manifest_sha256"`
	HistoricalAcceptanceSHA string            `json:"historical_acceptance_sha256"`
	Inputs                  map[string]string `json:"input_sha256"`
	Scope                   string            `json:"scope"`
}

func slCleanupHashFile(path string, hashes map[string]string, expected string) ([]byte, error) {
	b, e := slRead(path, 128<<20)
	if e != nil {
		return nil, e
	}
	hash := slSHA(b)
	if expected != "" && hash != expected {
		return nil, fmt.Errorf("proof hash differs: %s", path)
	}
	hashes[path] = hash
	return b, nil
}

// A manifest covers regular files, including nested evidence, but never itself.
func slCleanupManifest(dir, expected string, hashes map[string]string) error {
	b, e := slCleanupHashFile(filepath.Join(dir, "manifest.json"), hashes, expected)
	if e != nil {
		return e
	}
	var m map[string]string
	if e = json.Unmarshal(b, &m); e != nil {
		return e
	}
	if len(m) == 0 {
		return fmt.Errorf("empty manifest")
	}
	for p, h := range m {
		full := filepath.Join(dir, p)
		if p == "manifest.json" || filepath.IsAbs(p) || filepath.Clean(p) != p || !slWithin(dir, full) || len(h) != 64 {
			return fmt.Errorf("unsafe manifest member")
		}
		if _, e = slCleanupHashFile(full, hashes, h); e != nil {
			return e
		}
	}
	// Reject unlisted additions as well as missing/altered members.
	return filepath.WalkDir(dir, func(path string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if d.IsDir() {
			return nil
		}
		rel, e := filepath.Rel(dir, path)
		if e != nil {
			return e
		}
		if rel == "manifest.json" {
			return nil
		}
		if _, ok := m[rel]; !ok {
			return fmt.Errorf("unlisted manifest member: %s", rel)
		}
		return nil
	})
}
func slCleanupInputs(a slAcceptance, c slRunnerConfig) (slCleanupInput, error) {
	in := slCleanupInput{Hashes: map[string]string{}}
	if e := slValidateContract(a, c); e != nil {
		return in, e
	}
	if a.EvidenceDir != filepath.Join(a.ScopeRoot, "evidence", "sigterm-02") {
		return in, fmt.Errorf("historical sigterm-02 required")
	}
	old := filepath.Join(a.ScopeRoot, "evidence", "logs-01")
	if e := slCleanupManifest(old, slCleanupFailedSHA, in.Hashes); e != nil {
		return in, e
	}
	b, e := slCleanupHashFile(filepath.Join(a.ScopeRoot, "acceptance-02.json"), in.Hashes, slCleanupHistoricalSHA)
	if e != nil {
		return in, e
	}
	if e = json.Unmarshal(b, &in.Historical); e != nil {
		return in, e
	}
	var pre struct {
		Acceptance slAcceptance `json:"acceptance"`
	}
	if e = slReadJSON(filepath.Join(old, "preflight.json"), &pre); e != nil {
		return in, e
	}
	// The original suite has no acceptance-input.json. Its manifest covers this
	// embedded original acceptance; never manufacture a replacement old file.
	if pre.Acceptance != in.Historical {
		return in, fmt.Errorf("historical acceptance differs from frozen preflight")
	}
	comparable := a
	comparable.RunnerBinary = in.Historical.RunnerBinary
	comparable.RunnerSHA = in.Historical.RunnerSHA
	comparable.WorkerBinary = in.Historical.WorkerBinary
	comparable.WorkerSHA = in.Historical.WorkerSHA
	comparable.TestBinary = in.Historical.TestBinary
	comparable.TestSHA = in.Historical.TestSHA
	if comparable != in.Historical {
		return in, fmt.Errorf("new binary changes non-binary authority")
	}
	// Actual JSON field names are fixed, not inferred from summary prose.
	var acceptance struct {
		Passed bool              `json:"passed"`
		Before map[string]string `json:"input_sha256_before"`
		After  map[string]string `json:"input_sha256_after"`
	}
	if e = slReadJSON(filepath.Join(old, "acceptance.json"), &acceptance); e != nil {
		return in, e
	}
	if acceptance.Passed || len(acceptance.Before) != 8 || !reflect.DeepEqual(acceptance.Before, acceptance.After) {
		return in, fmt.Errorf("original failure/input identity differs")
	}
	for p, h := range acceptance.Before {
		if !slWithin(a.ScopeRoot, p) {
			return in, fmt.Errorf("historical input outside scope")
		}
		if _, e = slCleanupHashFile(p, in.Hashes, h); e != nil {
			return in, e
		}
	}
	for _, x := range []struct{ p, h string }{{a.RunnerBinary, a.RunnerSHA}, {a.WorkerBinary, a.WorkerSHA}, {a.TestBinary, a.TestSHA}} {
		if _, e = slCleanupHashFile(x.p, in.Hashes, x.h); e != nil {
			return in, e
		}
	}
	for p, h := range map[string]string{
		"result.json": "d3d1e7379a4441ab5d832fcac986c8918b4afeda89c3f6defbb1c94ac351987a",
		"intent.json": "aa6ab254b718ec8a1edc09a45ab0bd6cbc7bacb62704a3cacf2217278a50ffd3",
	} {
		if _, e = slCleanupHashFile(filepath.Join(a.ScopeRoot, "evidence", "host-logs-02", p), in.Hashes, h); e != nil {
			return in, e
		}
	}
	var host struct {
		ExitCode int               `json:"exit_code"`
		Unit     string            `json:"unit"`
		After    map[string]string `json:"after"`
	}
	if e = slReadJSON(filepath.Join(a.ScopeRoot, "evidence", "host-logs-02", "result.json"), &host); e != nil {
		return in, e
	}
	if host.ExitCode != 1 || host.Unit != "forge-lifecycle-lr20260912_a-logs-9b7ee7d18173afdb.service" || host.After["MainPID"] != "0" || host.After["LoadState"] != "not-found" {
		return in, fmt.Errorf("original host not terminal")
	}
	if e = slReadJSON(filepath.Join(old, "L3-bytes-op-03-journal.json"), &in.Before); e != nil {
		return in, e
	}
	if e = slCleanupInitial(in.Before); e != nil {
		return in, e
	}
	return in, nil
}

func slCleanupInitial(j slJournal) error {
	if j.Identity != slCleanupUUID || j.Version != 5 || len(j.Tables["operations"]) != 44 || len(j.Tables["workspaces"]) != 4 || len(j.Tables["volume_slots"]) != 4 {
		return fmt.Errorf("original journal identity/count mismatch")
	}
	target, e := slRow(j, "workspaces", "id", slCleanupID)
	if e != nil {
		return e
	}
	if target["tenant_id"] != "strict-log-fixture" || target["run_id"] != slCleanupID || target["source_id"] != "lifecycle" || target["profile_id"] != "lifecycle-python" || target["baseline_hash"] != slCleanupTree || slNum(target["epoch"]) != 1 || slNum(target["revision"]) != 1 || slNum(target["stopped"]) != 0 || slNum(target["adopting"]) != 0 || slNum(target["released"]) != 0 || target["active_operation"] != "" {
		return fmt.Errorf("original target tuple differs")
	}
	count := 0
	for _, r := range j.Tables["operations"] {
		if r["status"] != "succeeded" && r["status"] != "failed" && r["status"] != "cancelled" {
			return fmt.Errorf("unsettled original operation")
		}
		var req runner.OperationRequest
		if json.Unmarshal([]byte(fmt.Sprint(r["request_json"])), &req) != nil {
			return fmt.Errorf("invalid original request")
		}
		if req.WorkspaceID == slCleanupID {
			want := fmt.Sprintf("%s-op-%02d", slCleanupID, count)
			count++
			if string(req.OperationID) != want || r["id"] != want || req.TenantID != "strict-log-fixture" || req.RunID != slCleanupID || req.Epoch != 1 || req.ExpectedRevision != 1 || r["status"] != "failed" || slNum(r["after_revision"]) != 1 || r["before_hash"] != slCleanupTree || r["after_hash"] != slCleanupTree {
				return fmt.Errorf("original four-operation binding differs")
			}
		}
	}
	if count != 4 {
		return fmt.Errorf("exact four terminal operations required")
	}
	for _, r := range j.Tables["workspaces"] {
		if r["id"] != slCleanupID && (slNum(r["released"]) != 1 || r["active_operation"] != "") {
			return fmt.Errorf("other workspace not idle")
		}
	}
	for _, r := range j.Tables["volume_leases"] {
		if r["workspace_id"] == slCleanupID {
			if r["slot_id"] != "slot-001" || r["tenant_id"] != "strict-log-fixture" || r["run_id"] != slCleanupID || slNum(r["epoch"]) != 1 || slNum(r["released"]) != 0 {
				return fmt.Errorf("target volume tuple differs")
			}
		} else if slNum(r["released"]) != 1 {
			return fmt.Errorf("other volume leased")
		}
	}
	return nil
}
func slCleanupSameJournal(a, b slJournal) bool {
	if a.Identity != b.Identity || a.Version != b.Version || len(a.Tables) != len(b.Tables) {
		return false
	}
	for table, rows := range a.Tables {
		if !reflect.DeepEqual(slCleanupCanonicalRows(rows), slCleanupCanonicalRows(b.Tables[table])) {
			return false
		}
	}
	return true
}
func slCleanupCanonicalRows(rows []map[string]any) []string {
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		out = append(out, string(slJSON(r)))
	}
	sort.Strings(out)
	return out
}

// Only production Stop/Release flags, four container cleanup states and the two
// expected immutable artifact pins may change. Requests/results/fees are never
// rewritten, and lifetime log reservations remain charged after release.
func slCleanupJournal(before, current slJournal, intent bool) error {
	if e := slCleanupInitial(before); e != nil {
		return e
	}
	if current.Identity != before.Identity || current.Version != before.Version || len(current.Tables) != len(before.Tables) {
		return fmt.Errorf("journal replaced")
	}
	for table, old := range before.Tables {
		rows := current.Tables[table]
		if table == "runner_artifacts" {
			existing := map[string]map[string]any{}
			for _, r := range old {
				existing[fmt.Sprint(r["object_key"])] = r
			}
			additions := 0
			for _, r := range rows {
				key := fmt.Sprint(r["object_key"])
				if prior, ok := existing[key]; ok {
					if !bytes.Equal(slJSON(prior), slJSON(r)) {
						return fmt.Errorf("existing artifact pin changed")
					}
					delete(existing, key)
					continue
				}
				var ref artifact.Ref
				if !intent || json.Unmarshal([]byte(fmt.Sprint(r["ref_json"])), &ref) != nil || ref.TenantID != "strict-log-fixture" || ref.RunID != slCleanupID || (ref.Kind != "workspace_stop" && ref.Kind != "workspace_snapshot") || ref.ObjectKey != key {
					return fmt.Errorf("unexpected new artifact pin")
				}
				additions++
			}
			if len(existing) != 0 || additions > 2 {
				return fmt.Errorf("artifact pins removed/expanded")
			}
			continue
		}
		normalized := make([]map[string]any, 0, len(rows))
		for _, r := range rows {
			n := map[string]any{}
			for k, v := range r {
				n[k] = v
			}
			if intent && table == "workspaces" && r["id"] == slCleanupID {
				stopped, adopting, released := slNum(r["stopped"]), slNum(r["adopting"]), slNum(r["released"])
				if !((stopped == 0 && adopting == 0 && released == 0) || (stopped == 1 && adopting == 1 && (released == 0 || released == 1))) {
					return fmt.Errorf("unexpected cleanup state")
				}
				n["stopped"], n["adopting"], n["released"] = int64(0), int64(0), int64(0)
			}
			if intent && table == "volume_leases" && r["workspace_id"] == slCleanupID {
				if slNum(r["released"]) != 0 && slNum(r["released"]) != 1 {
					return fmt.Errorf("invalid volume state")
				}
				n["released"] = int64(0)
			}
			if intent && table == "operation_logs" && strings.HasPrefix(fmt.Sprint(r["operation_id"]), slCleanupID+"-op-") {
				if r["cleanup_state"] != "retained" && r["cleanup_state"] != "removing" && r["cleanup_state"] != "removed" {
					return fmt.Errorf("invalid log cleanup state")
				}
				n["cleanup_state"] = "retained"
			}
			normalized = append(normalized, n)
		}
		if !reflect.DeepEqual(slCleanupCanonicalRows(old), slCleanupCanonicalRows(normalized)) {
			return fmt.Errorf("unexpected journal changes in %s", table)
		}
	}
	return nil
}
func slCleanupNeedsReleaseProof(j slJournal) bool {
	for _, r := range j.Tables["workspaces"] {
		if r["id"] == slCleanupID && slNum(r["released"]) == 1 {
			return true
		}
	}
	for _, r := range j.Tables["volume_leases"] {
		if r["workspace_id"] == slCleanupID && slNum(r["released"]) == 1 {
			return true
		}
	}
	for _, r := range j.Tables["operation_logs"] {
		if strings.HasPrefix(fmt.Sprint(r["operation_id"]), slCleanupID+"-op-") && r["cleanup_state"] != "retained" {
			return true
		}
	}
	return false
}
func slCleanupReleased(j slJournal) error {
	if e := slIdle(j); e != nil {
		return e
	}
	n := 0
	for _, r := range j.Tables["operation_logs"] {
		if strings.HasPrefix(fmt.Sprint(r["operation_id"]), slCleanupID+"-op-") {
			n++
			if r["cleanup_state"] != "removed" {
				return fmt.Errorf("retained operation container cleanup not confirmed")
			}
		}
	}
	if n != 4 {
		return fmt.Errorf("four removed container receipts required")
	}
	return nil
}
func slCleanupIntent(hashes map[string]string) map[string]any {
	return map[string]any{"journal_uuid": slCleanupUUID, "workspace_id": slCleanupID, "epoch": 1, "revision": 1, "input_sha256": hashes, "scope": "fresh short-lived operator fixture grant only; no SQL run/lease, no execution authority"}
}
func slCleanupReleaseIntent(stop runner.StopReceipt, snap runner.Snapshot) map[string]any {
	return map[string]any{"workspace_id": slCleanupID, "epoch": 1, "snapshot_verified": true, "stop_sha256": stop.Ref.SHA256, "snapshot_sha256": snap.Artifact.SHA256}
}
func slCleanupWorkspace(w runner.Workspace) error {
	if w.TenantID != "strict-log-fixture" || w.RunID != slCleanupID || w.ID != slCleanupID || w.Epoch != 1 || w.Revision != 1 || w.BaselineHash != slCleanupTree || w.SourceID != "lifecycle" || w.ProfileID != "lifecycle-python" || w.ActiveOperation != "" || !w.Stopped || !w.Adopting || w.Released {
		return fmt.Errorf("stopped workspace binding differs")
	}
	return nil
}
func slCleanupRef(ref artifact.Ref, kind string, raw []byte) error {
	if ref.TenantID != "strict-log-fixture" || ref.RunID != slCleanupID || ref.Kind != kind || ref.SHA256 != slSHA(raw) || ref.Size != int64(len(raw)) || ref.ObjectKey != "strict-log-fixture/"+slCleanupID+"/"+ref.SHA256 {
		return fmt.Errorf("artifact tuple/content differs")
	}
	return nil
}
func slCleanupStop(stop runner.StopReceipt, raw []byte) error {
	if e := slCleanupWorkspace(stop.Workspace); e != nil {
		return e
	}
	if !stop.NoActiveOperations {
		return fmt.Errorf("stop incomplete")
	}
	if e := slCleanupRef(stop.Ref, "workspace_stop", raw); e != nil {
		return e
	}
	var body runner.StopReceipt
	if e := json.Unmarshal(raw, &body); e != nil {
		return e
	}
	want := stop
	want.Ref = artifact.Ref{}
	if !reflect.DeepEqual(body, want) {
		return fmt.Errorf("stop object body differs")
	}
	return nil
}
func slCleanupSnapshot(s runner.Snapshot, raw []byte) error {
	if e := slCleanupWorkspace(s.Workspace); e != nil {
		return e
	}
	if s.Hash != slCleanupTree {
		return fmt.Errorf("snapshot source hash differs")
	}
	if e := slCleanupRef(s.Artifact, "workspace_snapshot", raw); e != nil {
		return e
	}
	var body struct {
		Workspace runner.Workspace `json:"workspace"`
		Files     map[string]struct {
			SHA        string `json:"sha256"`
			Content    []byte `json:"content"`
			Executable bool   `json:"executable"`
		} `json:"files"`
	}
	if e := json.Unmarshal(raw, &body); e != nil {
		return e
	}
	file, ok := body.Files["app.py"]
	if body.Workspace != s.Workspace || len(body.Files) != 1 || !ok || file.Executable || string(file.Content) != slCleanupSource || file.SHA != slSHA(file.Content) {
		return fmt.Errorf("snapshot is not the original complete source tree")
	}
	// Independently reconstruct the single-file tree hash, not just metadata.
	if slSHA([]byte(fmt.Sprintf("6:app.py:%s:false\n", file.SHA))) != s.Hash {
		return fmt.Errorf("snapshot tree digest differs")
	}
	return nil
}
func slCleanupObject(c slRunnerConfig, ref artifact.Ref) ([]byte, error) {
	if ref.ObjectKey != "strict-log-fixture/"+slCleanupID+"/"+ref.SHA256 || len(ref.SHA256) != 64 || ref.Size <= 0 || ref.Size > 64<<20 {
		return nil, fmt.Errorf("unsafe cleanup object ref")
	}
	b, e := slRead(filepath.Join(c.ArtifactRoot, ref.ObjectKey), 64<<20)
	if e != nil {
		return nil, e
	}
	if slSHA(b) != ref.SHA256 || int64(len(b)) != ref.Size {
		return nil, fmt.Errorf("object bytes changed")
	}
	return b, nil
}

// Existing successful stage files are immutable. A partial file is a hard
// failure requiring inspection; it is never truncated to make a retry pass.
func slCleanupSave(path string, v any) error {
	b, e := json.MarshalIndent(v, "", "  ")
	if e != nil {
		return e
	}
	return slCleanupWrite(path, append(b, '\n'))
}
func slCleanupWrite(path string, b []byte) error {
	old, e := slRead(path, 128<<20)
	if e == nil {
		if !bytes.Equal(old, b) {
			return fmt.Errorf("existing stage differs: %s", path)
		}
		return nil
	}
	if !os.IsNotExist(e) {
		return e
	}
	return slWrite(path, b)
}
func slCleanupExisting(path string, v any) error {
	b, e := slRead(path, 128<<20)
	if e != nil {
		return e
	}
	// JSON object member order is not authority. A saved struct keeps declaration
	// order, while a decoded map marshals sorted keys. Canonicalize both sides and
	// retain exact JSON numbers instead of comparing those incidental encodings.
	existing, e := domain.CanonicalJSON(b)
	if e != nil {
		return e
	}
	wanted, e := domain.CanonicalJSON(slJSON(v))
	if e != nil {
		return e
	}
	if !bytes.Equal(existing, wanted) {
		return fmt.Errorf("required existing stage differs: %s", path)
	}
	return nil
}
func slCleanupGrant(s *runner.Signer, now time.Time) (runner.WorkspaceRequest, error) {
	r := runner.WorkspaceRequest{TenantID: "strict-log-fixture", RunID: slCleanupID, WorkspaceID: slCleanupID, Epoch: 1}
	var e error
	// This direct-RPC fixture has no SQL run. This is fresh operator fixture
	// authority, NOT an old execution lease or a database-backed cleanup grant.
	r.Grant, e = s.Sign(runner.Claims{TenantID: r.TenantID, RunID: r.RunID, WorkspaceID: r.WorkspaceID, Epoch: 1, IssuedAt: now, ExpiresAt: now.Add(20 * time.Second), Permissions: []string{"inspect", "cancel", "snapshot", "release"}}, now.Add(25*time.Second))
	return r, e
}
func slCleanupArchive(dir string, c slRunnerConfig, j slJournal) (runner.StopReceipt, runner.Snapshot, error) {
	var stop runner.StopReceipt
	var snap runner.Snapshot
	if e := slReadJSON(filepath.Join(dir, "stop.json"), &stop); e != nil {
		return stop, snap, e
	}
	raw, e := slRead(filepath.Join(dir, "stop.bytes"), 64<<20)
	if e != nil {
		return stop, snap, e
	}
	if e = slCleanupStop(stop, raw); e != nil {
		return stop, snap, e
	}
	actual, e := slCleanupObject(c, stop.Ref)
	if e != nil || !bytes.Equal(raw, actual) {
		return stop, snap, fmt.Errorf("stop archive/object mismatch: %v", e)
	}
	if e = slReadJSON(filepath.Join(dir, "snapshot.json"), &snap); e != nil {
		return stop, snap, e
	}
	raw, e = slRead(filepath.Join(dir, "snapshot.bytes"), 64<<20)
	if e != nil {
		return stop, snap, e
	}
	if e = slCleanupSnapshot(snap, raw); e != nil {
		return stop, snap, e
	}
	actual, e = slCleanupObject(c, snap.Artifact)
	if e != nil || !bytes.Equal(raw, actual) {
		return stop, snap, fmt.Errorf("snapshot archive/object mismatch: %v", e)
	}
	return stop, snap, slVerifyPins(j, stop.Ref, snap.Artifact)
}
func slCleanupMakeManifest(dir string) error {
	m := map[string]string{}
	e := filepath.WalkDir(dir, func(p string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if d.IsDir() {
			return nil
		}
		rel, e := filepath.Rel(dir, p)
		if e != nil {
			return e
		}
		if rel == "manifest.json" {
			return nil
		}
		b, e := slRead(p, 128<<20)
		if e != nil {
			return e
		}
		m[rel] = slSHA(b)
		return nil
	})
	if e != nil {
		return e
	}
	return slSave(filepath.Join(dir, "manifest.json"), m)
}

// Called by the next full logs execution before creating a schema or runner.
// Recheck every cleanup proof and immutable input, not only the passed summary.
func slCleanupPrerequisite(a slAcceptance, c slRunnerConfig, j slJournal) (map[string]string, error) {
	in, e := slCleanupInputs(a, c)
	if e != nil {
		return nil, e
	}
	dir := filepath.Join(a.ScopeRoot, "evidence", "logs-01-cleanup")
	if e = slCleanupManifest(dir, "", in.Hashes); e != nil {
		return nil, e
	}
	var report slCleanupReport
	if e = slReadJSON(filepath.Join(dir, "report.json"), &report); e != nil {
		return nil, e
	}
	if !report.Passed || report.JournalUUID != slCleanupUUID || report.WorkspaceID != slCleanupID || report.Epoch != 1 || report.Revision != 1 || !report.Released || !report.SnapshotVerified || report.FailedManifestSHA != slCleanupFailedSHA || report.HistoricalAcceptanceSHA != slCleanupHistoricalSHA {
		return nil, fmt.Errorf("cleanup acceptance incomplete")
	}
	var cleanupA slAcceptance
	cleanupPath := filepath.Join(a.ScopeRoot, "acceptance-logs-cleanup-01.json")
	if e = slReadJSON(cleanupPath, &cleanupA); e != nil {
		return nil, e
	}
	required, e := slCleanupInputs(cleanupA, c)
	if e != nil {
		return nil, e
	}
	if _, e = slCleanupHashFile(cleanupPath, required.Hashes, ""); e != nil {
		return nil, e
	}
	if !reflect.DeepEqual(required.Hashes, report.Inputs) {
		return nil, fmt.Errorf("cleanup report does not bind every original/execution input")
	}
	if e = slCleanupExisting(filepath.Join(dir, "acceptance-input.json"), cleanupA); e != nil {
		return nil, e
	}
	if e = slCleanupExisting(filepath.Join(dir, "intent.json"), slCleanupIntent(required.Hashes)); e != nil {
		return nil, e
	}
	for p, h := range report.Inputs {
		if !slWithin(a.ScopeRoot, p) {
			return nil, fmt.Errorf("cleanup input outside scope")
		}
		if _, e = slCleanupHashFile(p, in.Hashes, h); e != nil {
			return nil, e
		}
	}
	if e = slCleanupJournal(in.Before, j, true); e != nil {
		return nil, e
	}
	if e = slCleanupReleased(j); e != nil {
		return nil, e
	}
	if e = slCleanupStorageGone(c); e != nil {
		return nil, e
	}
	stop, snap, err := slCleanupArchive(dir, c, j)
	if err != nil {
		return nil, err
	}
	if e = slCleanupExisting(filepath.Join(dir, "release-intent.json"), slCleanupReleaseIntent(stop, snap)); e != nil {
		return nil, e
	}
	for _, ref := range []artifact.Ref{stop.Ref, snap.Artifact} {
		if _, e = slCleanupHashFile(filepath.Join(c.ArtifactRoot, ref.ObjectKey), in.Hashes, ref.SHA256); e != nil {
			return nil, e
		}
	}
	if e = slCleanupExisting(filepath.Join(dir, "release.json"), runner.ReleaseResult{WorkspaceID: slCleanupID, Released: true}); e != nil {
		return nil, e
	}
	var final slJournal
	if e = slReadJSON(filepath.Join(dir, "released-journal.json"), &final); e != nil {
		return nil, e
	}
	if !slCleanupSameJournal(final, j) {
		return nil, fmt.Errorf("live journal changed after cleanup")
	}
	for _, name := range []string{"intent.json", "release-intent.json", "release.json", "stop.bytes", "stop.json", "snapshot.bytes", "snapshot.json", "released-journal.json", "report.json", "acceptance-input.json"} {
		if _, ok := in.Hashes[filepath.Join(dir, name)]; !ok {
			return nil, fmt.Errorf("cleanup proof omitted from manifest: %s", name)
		}
	}
	return in.Hashes, nil
}

func TestStrictLogsRetainedCleanup(t *testing.T) {
	if os.Getenv("FORGE_RUN_STRICT_LOGS_CLEANUP") != "1" {
		t.Skip("explicit retained-fixture cleanup; no default host actions")
	}
	path := os.Getenv("FORGE_STRICT_LOGS_ACCEPTANCE")
	var a slAcceptance
	var c slRunnerConfig
	check := func(e error) {
		t.Helper()
		if e != nil {
			t.Fatal(e)
		}
	}
	check(slReadJSON(path, &a))
	check(slReadJSON(a.RunnerConfig, &c))
	if path != filepath.Join(a.ScopeRoot, "acceptance-logs-cleanup-01.json") {
		t.Fatal("exact cleanup manifest required")
	}
	in, e := slCleanupInputs(a, c)
	check(e)
	_, e = slCleanupHashFile(path, in.Hashes, "")
	check(e)
	uid, e := os.ReadFile("/proc/self/uid_map")
	check(e)
	gid, e := os.ReadFile("/proc/self/gid_map")
	check(e)
	self, e := os.Executable()
	check(e)
	if os.Geteuid() != 0 || !slMapping(uid) || !slMapping(gid) || self != a.TestBinary {
		t.Fatal("declared binary in full subordinate mapping required")
	}
	unlock, e := lifecycleControlLock(a.ScopeRoot)
	check(e)
	defer unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	dir := filepath.Join(a.ScopeRoot, "evidence", "logs-01-cleanup")
	// Persist both newly-created directory entries before intent, key loading,
	// process launch, or any RPC. A sync error preserves files but aborts here.
	attempt, e := slCleanupPrepareDirectory(dir, slCleanupSyncDirectory)
	check(e)
	passed := false
	invocationSaved := false
	defer func() {
		if invocationSaved {
			return
		}
		if e := slSave(filepath.Join(attempt, "report.json"), map[string]any{"passed": passed && !t.Failed(), "finished_at": time.Now().UTC(), "scope": "one known terminal fixture workspace; failures preserve all state"}); e != nil {
			t.Error(e)
		}
	}()
	check(slCleanupSave(filepath.Join(dir, "acceptance-input.json"), a))
	var prior map[string]any
	intentPath := filepath.Join(dir, "intent.json")
	intentErr := slReadJSON(intentPath, &prior)
	hasIntent := intentErr == nil
	if intentErr != nil && !os.IsNotExist(intentErr) {
		check(intentErr)
	}
	j, e := slJournalSnapshot(ctx, c.JournalPath)
	check(e)
	check(slCleanupJournal(in.Before, j, hasIntent))
	check(slSave(filepath.Join(attempt, "journal-before.json"), j))
	wantIntent := slCleanupIntent(in.Hashes)
	check(slCleanupSave(intentPath, wantIntent))
	check(slCleanupVolumes(ctx, c, j))
	if conn, e := net.DialTimeout("unix", c.Server.UnixSocket, 100*time.Millisecond); e == nil {
		conn.Close()
		t.Fatal("live runner endpoint: refuse takeover")
	}
	// Confirm no existing engine owns any of these four exact volume locks. The
	// locks are released just before the new runner takes its own lifetime locks.
	var held []int
	defer func() {
		for _, fd := range held {
			unix.Close(fd)
		}
	}()
	for _, v := range c.VolumeSlots {
		fd, e := unix.Open(filepath.Join(v.MountPath, ".forge-pool.lock"), unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		check(e)
		var st unix.Stat_t
		check(unix.Fstat(fd, &st))
		if st.Mode&unix.S_IFMT != unix.S_IFREG || st.Nlink != 1 {
			unix.Close(fd)
			t.Fatal("pool lock identity differs")
		}
		if e = unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); e != nil {
			unix.Close(fd)
			check(e)
		}
		held = append(held, fd)
	}
	target, e := slRow(j, "workspaces", "id", slCleanupID)
	check(e)
	lease, e := slRow(j, "volume_leases", "workspace_id", slCleanupID)
	check(e)
	alreadyReleased := slNum(target["released"]) == 1
	if slCleanupNeedsReleaseProof(j) {
		stop, snap, err := slCleanupArchive(dir, c, j)
		check(err)
		check(slCleanupExisting(filepath.Join(dir, "release-intent.json"), slCleanupReleaseIntent(stop, snap)))
	}
	needRunner := !alreadyReleased || slNum(lease["released"]) != 1
	if needRunner {
		key, e := slRead(c.SigningKeyFile, 4096)
		check(e)
		signer, e := runner.NewSigner(key)
		clear(key)
		check(e)
		f := &slFixture{t: t, ctx: ctx, a: a, c: c, dir: attempt, journalID: slCleanupUUID, signer: signer, configs: map[string]string{"bytes": filepath.Join(a.ScopeRoot, "runtime", "runner-byteguard.json")}}
		// Registered before start so partial startup failure still attempts to drain
		// only this child. The outer invocation report survives any Fatal in cleanup.
		defer func() {
			if f.ownRunner != nil {
				f.stopRunner()
			}
		}()
		for _, fd := range held {
			check(unix.Close(fd))
		}
		held = nil
		f.startRunner("bytes", "")
		j, e = slJournalSnapshot(ctx, c.JournalPath)
		check(e)
		check(slCleanupJournal(in.Before, j, true))
		grant := func() runner.WorkspaceRequest { r, e := slCleanupGrant(signer, time.Now()); check(e); return r }
		for i := 0; i < 4; i++ {
			id := domain.ID(fmt.Sprintf("%s-op-%02d", slCleanupID, i))
			op, e := f.client.InspectOperation(ctx, runner.InspectRequest{WorkspaceRequest: grant(), OperationID: id})
			check(e)
			op.Request.Grant = ""
			var expected runner.Operation
			check(slReadJSON(filepath.Join(a.ScopeRoot, "evidence", "logs-01", fmt.Sprintf("L3-bytes-op-%02d-operation.json", i)), &expected))
			if !bytes.Equal(slJSON(op), slJSON(expected)) {
				t.Fatal("same-ID terminal operation changed")
			}
			check(slSave(filepath.Join(attempt, fmt.Sprintf("operation-%02d.json", i)), op))
			receipt, e := slCleanupObject(c, op.Receipt)
			check(e)
			archived, e := slRead(filepath.Join(a.ScopeRoot, "evidence", "logs-01", fmt.Sprintf("L3-bytes-op-%02d-receipt.json", i)), 1<<20)
			check(e)
			if !bytes.Equal(receipt, archived) {
				t.Fatal("original operation receipt bytes differ")
			}
			var job sandbox.Job
			check(json.Unmarshal(op.Result, &job))
			if job.Log == nil {
				t.Fatal("missing strict log ref")
			}
			logBytes, e := slCleanupObject(c, job.Log.Artifact)
			check(e)
			archived, e = slRead(filepath.Join(a.ScopeRoot, "evidence", "logs-01", fmt.Sprintf("L3-bytes-op-%02d-log.flg", i)), 1<<20)
			check(e)
			if !bytes.Equal(logBytes, archived) {
				t.Fatal("original archived log bytes differ")
			}
			_, e = slValidateLog(op, op.Request, logBytes, nil)
			check(e)
			check(slVerifyPins(j, op.Receipt, job.Log.Artifact))
		}
		if !alreadyReleased {
			stop, e := f.client.StopWorkspace(ctx, grant())
			check(e)
			raw, e := slCleanupObject(c, stop.Ref)
			check(e)
			check(slCleanupStop(stop, raw))
			check(slCleanupWrite(filepath.Join(dir, "stop.bytes"), raw))
			check(slCleanupSave(filepath.Join(dir, "stop.json"), stop))
			snap, e := f.client.SealSnapshot(ctx, grant())
			check(e)
			raw, e = slCleanupObject(c, snap.Artifact)
			check(e)
			check(slCleanupSnapshot(snap, raw))
			// Bytes and metadata are fsynced, including their parent, before any release.
			check(slCleanupWrite(filepath.Join(dir, "snapshot.bytes"), raw))
			check(slCleanupSave(filepath.Join(dir, "snapshot.json"), snap))
		}
		j, e = slJournalSnapshot(ctx, c.JournalPath)
		check(e)
		check(slCleanupJournal(in.Before, j, true))
		stop, snap, err := slCleanupArchive(dir, c, j)
		check(err)
		check(slCleanupSave(filepath.Join(dir, "release-intent.json"), slCleanupReleaseIntent(stop, snap)))
		rel, e := f.client.ReleaseWorkspace(ctx, grant())
		check(e)
		if !rel.Released || rel.WorkspaceID != slCleanupID {
			t.Fatal("release not confirmed")
		}
		check(slCleanupSave(filepath.Join(dir, "release.json"), rel))
		f.stopRunner()
	}
	j, e = slJournalSnapshot(ctx, c.JournalPath)
	check(e)
	check(slCleanupJournal(in.Before, j, true))
	check(slCleanupReleased(j))
	check(slCleanupStorageGone(c))
	check(slCleanupVolumes(ctx, c, j))
	_, _, e = slCleanupArchive(dir, c, j)
	check(e)
	// In an after-Release crash, both persisted released flags and the previously
	// archived proofs can confirm completion without re-sealing missing storage.
	check(slCleanupSave(filepath.Join(dir, "release.json"), runner.ReleaseResult{WorkspaceID: slCleanupID, Released: true}))
	check(slCleanupSave(filepath.Join(dir, "released-journal.json"), j))
	for p, h := range in.Hashes {
		_, e = slCleanupHashFile(p, map[string]string{}, h)
		check(e)
	}
	report := slCleanupReport{Passed: true, JournalUUID: slCleanupUUID, WorkspaceID: slCleanupID, Epoch: 1, Revision: 1, Released: true, SnapshotVerified: true, FailedManifestSHA: slCleanupFailedSHA, HistoricalAcceptanceSHA: slCleanupHistoricalSHA, Inputs: in.Hashes, Scope: "operator fixture cleanup of one direct-RPC workspace; archived code snapshot; no PG run, scheduler lease, worker, provider call or recovery-quality claim"}
	check(slCleanupSave(filepath.Join(dir, "report.json"), report))
	passed = true
	// Write the successful invocation before manifest collection; the deferred
	// failure writer is disabled only after its durable write succeeds.
	check(slSave(filepath.Join(attempt, "report.json"), map[string]any{"passed": true, "finished_at": time.Now().UTC(), "scope": "one known terminal fixture workspace; failures preserve all state"}))
	invocationSaved = true
	check(slCleanupMakeManifest(dir))
	_, e = slCleanupPrerequisite(a, c, j)
	check(e)
}

// The enclosing directory's entry is a separate durability boundary from
// fsync of files and of the newly-created directory itself.
func slCleanupSyncDirectory(path string) error {
	fd, e := unix.Open(path, unix.O_DIRECTORY|unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if e != nil {
		return e
	}
	defer unix.Close(fd)
	return unix.Fsync(fd)
}
func slCleanupPrepareDirectory(dir string, syncDirectory func(string) error) (string, error) {
	if e := lifecyclePath(dir); e != nil {
		return "", e
	}
	if e := os.Mkdir(dir, 0700); e != nil && !os.IsExist(e) {
		return "", e
	}
	// Also sync an existing parent entry after a crash between mkdir and fsync.
	if e := syncDirectory(filepath.Dir(dir)); e != nil {
		return "", fmt.Errorf("cleanup parent sync before execution: %w", e)
	}
	info, e := os.Stat(dir)
	if e != nil {
		return "", e
	}
	if !info.IsDir() || info.Mode().Perm() != 0700 {
		return "", fmt.Errorf("private cleanup directory required")
	}
	if _, e = os.Lstat(filepath.Join(dir, "manifest.json")); e == nil {
		return "", fmt.Errorf("cleanup already finalized; use read-only prerequisite verification")
	} else if !os.IsNotExist(e) {
		return "", e
	}
	attempt, e := os.MkdirTemp(dir, "invocation-")
	if e != nil {
		return "", e
	}
	if e = syncDirectory(dir); e != nil {
		return "", fmt.Errorf("invocation parent sync before execution: %w", e)
	}
	return attempt, nil
}
func slCleanupStorageGone(c slRunnerConfig) error {
	if len(c.VolumeSlots) != 4 || c.VolumeSlots[0].ID != "slot-001" {
		return fmt.Errorf("fixed cleanup slot missing")
	}
	p := filepath.Join(c.VolumeSlots[0].MountPath, "workspace-"+slCleanupID)
	if _, e := os.Lstat(p); !os.IsNotExist(e) {
		return fmt.Errorf("released workspace storage still present or inaccessible: %v", e)
	}
	return nil
}
func slCleanupVolumes(ctx context.Context, c slRunnerConfig, j slJournal) error {
	for _, v := range c.VolumeSlots {
		row, e := slRow(j, "volume_slots", "id", v.ID)
		if e != nil {
			return e
		}
		var stored sandbox.VolumeSpec
		if json.Unmarshal([]byte(fmt.Sprint(row["spec_json"])), &stored) != nil || stored != v {
			return fmt.Errorf("persisted volume identity differs")
		}
		if e = sandbox.VerifyVolume(ctx, v); e != nil {
			return e
		}
		owner, e := slRead(filepath.Join(v.MountPath, ".forge-pool.owner"), 4096)
		if e != nil {
			return e
		}
		if string(owner) != c.JournalPath+"\n" {
			return fmt.Errorf("pool owner changed; no rebinding")
		}
	}
	return nil
}
