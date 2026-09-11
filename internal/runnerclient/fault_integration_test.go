package runnerclient

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/runner"
	"github.com/JDinSeattle/forge-runtime/internal/sandbox"
)

type crashConfig struct {
	RootDir          string                     `json:"root_dir"`
	JournalPath      string                     `json:"journal_path"`
	SigningKeyFile   string                     `json:"signing_key_file"`
	DockerHost       string                     `json:"docker_host"`
	AllowTestBackend bool                       `json:"allow_test_backend"`
	VolumeSlots      []sandbox.VolumeSpec       `json:"volume_slots"`
	Profiles         map[string]sandbox.Profile `json:"profiles"`
	Server           ServerConfig               `json:"server"`
}
type crashProcess struct {
	cmd    *exec.Cmd
	done   chan error
	exited bool
	log    *os.File
}
type crashHarness struct {
	t                                    *testing.T
	ctx                                  context.Context
	config                               crashConfig
	configPath, binary, evidence, socket string
	signer                               *runner.Signer
	child                                *crashProcess
	client                               *Client
	db                                   *sql.DB
	sequence                             int
}
type crashEvidence struct {
	Case                        string            `json:"case"`
	MatrixComponents            []string          `json:"matrix_components"`
	WorkspaceID                 domain.ID         `json:"workspace_id"`
	OperationID                 domain.ID         `json:"operation_id,omitempty"`
	FaultMarker                 json.RawMessage   `json:"fault_marker,omitempty"`
	InitialStatus               runner.Status     `json:"initial_status,omitempty"`
	FinalOperation              *runner.Operation `json:"final_operation,omitempty"`
	SourceHash                  string            `json:"source_hash,omitempty"`
	LeaseEpoch                  uint64            `json:"lease_epoch"`
	WorkspaceExistsBeforeResume bool              `json:"workspace_exists_before_resume"`
	OperationsBeforeResume      int               `json:"operations_before_resume"`
	DispatchStarted             bool              `json:"dispatch_started"`
	DockerStartIntent           bool              `json:"docker_start_intent"`
	SameIDReceipt               bool              `json:"same_id_original_receipt"`
	Counter                     string            `json:"counter,omitempty"`
	DockerEvents                []json.RawMessage `json:"docker_events,omitempty"`
	DockerCreates               int               `json:"docker_creates"`
	DockerStarts                int               `json:"docker_starts"`
	Pins                        int               `json:"journal_artifact_pins"`
	NoActive                    bool              `json:"no_active"`
	Released                    bool              `json:"released"`
	Snapshot                    string            `json:"snapshot_sha256,omitempty"`
	OldEpochRejected            bool              `json:"old_epoch_rejected"`
	WriterExclusive             bool              `json:"writer_exclusive"`
	OldWriterTimestamps         []int64           `json:"old_writer_timestamps_ns,omitempty"`
	NewWriterFirst              int64             `json:"new_writer_first_ns,omitempty"`
	Passed                      bool              `json:"passed"`
	// The harness is runner-only. It deliberately does not claim that these
	// direct RPCs created control-plane runs, effects, budgets or SSE events.
	ControlPlaneEvidence string `json:"control_plane_evidence"`
}

// TestRealRunnerCrashMatrix starts actual forge-runner child processes and kills
// them at operator-only boundary hooks while real Docker jobs survive. It must
// run inside the deployed UID map, with production workers/runner quiesced. All
// mutations are restricted to unique test workspaces and one-shot private plans.
func TestRealRunnerCrashMatrix(t *testing.T) {
	configPath := os.Getenv("FORGE_RUNNER_FAULT_CONFIG")
	if configPath == "" {
		t.Skip("requires quiesced real runner config, binary and evidence directory; see scripts/faults/README.md")
	}
	binary := os.Getenv("FORGE_RUNNER_FAULT_BINARY")
	evidence := os.Getenv("FORGE_RUNNER_FAULT_EVIDENCE")
	if !filepath.IsAbs(binary) || !filepath.IsAbs(evidence) {
		t.Fatal("absolute FORGE_RUNNER_FAULT_BINARY and FORGE_RUNNER_FAULT_EVIDENCE required")
	}
	raw, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	var c crashConfig
	if err = json.Unmarshal(raw, &c); err != nil {
		t.Fatal(err)
	}
	if c.AllowTestBackend || c.DockerHost == "" || len(c.VolumeSlots) == 0 || !strings.Contains(c.Profiles["python-clamp"].Image, "@sha256:") {
		t.Fatal("real fixed-volume Docker backend and pinned python-clamp profile required")
	}
	key, err := os.ReadFile(c.SigningKeyFile)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := runner.NewSigner(key)
	if err != nil {
		t.Fatal(err)
	}
	// Keep private UDS short enough for sockaddr_un regardless of repository path.
	private, err := os.MkdirTemp("/tmp", "forge-fault-")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.MkdirAll(evidence, 0700); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(evidence); err != nil || info.Mode().Perm()&0077 != 0 {
		t.Fatal("evidence directory must be owner-only")
	}
	var copyConfig map[string]json.RawMessage
	if err = json.Unmarshal(raw, &copyConfig); err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(private, "runner.sock")
	serverRaw, _ := json.Marshal(ServerConfig{UnixSocket: socket})
	copyConfig["server"] = serverRaw
	copied, _ := json.MarshalIndent(copyConfig, "", "  ")
	localConfig := filepath.Join(private, "runner.json")
	if err = os.WriteFile(localConfig, copied, 0600); err != nil {
		t.Fatal(err)
	}
	q := url.Values{"mode": {"ro"}, "_pragma": {"busy_timeout(5000)", "query_only(1)"}}
	u := url.URL{Scheme: "file", Path: c.JournalPath, RawQuery: q.Encode()}
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Minute)
	defer cancel()
	h := &crashHarness{t: t, ctx: ctx, config: c, configPath: localConfig, binary: binary, evidence: evidence, socket: socket, signer: signer, db: db}
	defer h.stop()
	if retained := os.Getenv("FORGE_RUNNER_FAULT_RECOVER"); retained != "" {
		h.recoverCreated(retained)
		return
	}
	// Do not remove evidence or private config on failure: it contains paths, not
	// raw key bytes, and permits operator diagnosis. No existing workspace erased.
	for _, point := range []string{"after_volume_lease", "after_import_file", "after_import_before_workspace"} {
		if !t.Run(point, func(t *testing.T) { h.t = t; h.initialization(point) }) {
			return
		}
	}
	for _, point := range []string{"after_prepared", "after_docker_create", "after_docker_start", "after_job_start", "after_job_exit", "after_receipt_before_commit", "after_start_acceptance", "takeover"} {
		if !t.Run(point, func(t *testing.T) { h.t = t; h.operation(point) }) {
			return
		}
	}
}

// recoverCreated only settles the original retained operation. It has no Start
// call and cannot allocate a new workspace; a successful run seals then releases
// precisely the immutable scope named by an earlier failed acceptance report.
func (h *crashHarness) recoverCreated(filename string) {
	if !filepath.IsAbs(filename) {
		h.t.Fatal("absolute retained report path required")
	}
	raw, err := os.ReadFile(filename)
	if err != nil {
		h.t.Fatal(err)
	}
	var prior crashEvidence
	if json.Unmarshal(raw, &prior) != nil || prior.Case != "after_docker_create" || prior.Passed || prior.WorkspaceID.Validate() != nil || prior.OperationID.Validate() != nil {
		h.t.Fatal("requires the failed after_docker_create report")
	}
	e := &crashEvidence{Case: "recover_after_docker_create", WorkspaceID: prior.WorkspaceID, OperationID: prior.OperationID, FaultMarker: prior.FaultMarker, MatrixComponents: []string{"F07-runner"}}
	defer h.report(e)
	h.start("")
	var tenant, run, workspace string
	var epoch uint64
	err = h.db.QueryRow(`SELECT o.tenant_id,o.run_id,o.workspace_id,w.epoch,o.dispatch_started,o.docker_start_intent FROM operations o JOIN workspaces w ON w.id=o.workspace_id WHERE o.id=?`, prior.OperationID).Scan(&tenant, &run, &workspace, &epoch, &e.DispatchStarted, &e.DockerStartIntent)
	if err != nil || tenant != "runner-fault-acceptance" || run != string(prior.WorkspaceID) || workspace != string(prior.WorkspaceID) || epoch != 1 || !e.DispatchStarted || e.DockerStartIntent {
		h.t.Fatalf("retained operation does not have required exact no-start proof: %v", err)
	}
	e.LeaseEpoch = epoch
	inspect := runner.InspectRequest{WorkspaceRequest: h.binding(prior.WorkspaceID, epoch, time.Minute), OperationID: prior.OperationID}
	op, err := h.client.CancelOperation(h.ctx, inspect)
	if err != nil || op.Status != runner.Cancelled || op.Receipt.ObjectKey == "" {
		h.t.Fatalf("retained original operation unresolved: %+v %v", op, err)
	}
	e.FinalOperation = &op
	if _, err = os.Stat(filepath.Join(h.checkout(prior.WorkspaceID), "fault-count.txt")); !os.IsNotExist(err) {
		h.t.Fatal("retained never-started job unexpectedly wrote files")
	}
	var marker struct {
		Observed time.Time `json:"observed_at"`
	}
	if json.Unmarshal(prior.FaultMarker, &marker) != nil || marker.Observed.IsZero() {
		h.t.Fatal("invalid original fault evidence")
	}
	h.events(marker.Observed.Add(-5*time.Second), op.JobID, e)
	if e.DockerCreates != 1 || e.DockerStarts != 0 {
		h.t.Fatalf("retained job execution history differs: create=%d start=%d", e.DockerCreates, e.DockerStarts)
	}
	var count int
	if err = h.db.QueryRow(`SELECT count(*) FROM operations WHERE workspace_id=?`, prior.WorkspaceID).Scan(&count); err != nil || count != 1 {
		h.t.Fatal("recovery created another operation")
	}
	h.cleanup(prior.WorkspaceID, epoch, e)
}
func (h *crashHarness) unique(prefix string) domain.ID {
	var nonce [6]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		h.t.Fatal(err)
	}
	return domain.ID(prefix + "-" + hex.EncodeToString(nonce[:]))
}
func (h *crashHarness) binding(id domain.ID, epoch uint64, lifetime time.Duration) runner.WorkspaceRequest {
	now := time.Now()
	p := runner.Claims{TenantID: "runner-fault-acceptance", RunID: id, WorkspaceID: id, Epoch: epoch, IssuedAt: now, ExpiresAt: now.Add(lifetime), Permissions: []string{"prepare", "execute", "inspect", "cancel", "snapshot", "release", "adopt"}}
	grant, err := h.signer.Sign(p, now.Add(lifetime+time.Minute))
	if err != nil {
		h.t.Fatal(err)
	}
	return runner.WorkspaceRequest{TenantID: p.TenantID, RunID: id, WorkspaceID: id, Epoch: epoch, Grant: grant}
}
func (h *crashHarness) start(plan string) {
	h.t.Helper()
	h.stop()
	h.sequence++
	// This exact private socket belongs to the already-waited child, never live RPC.
	if err := os.Remove(h.socket); err != nil && !os.IsNotExist(err) {
		h.t.Fatal(err)
	}
	log, err := os.OpenFile(filepath.Join(h.evidence, fmt.Sprintf("runner-%02d.log", h.sequence)), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		h.t.Fatal(err)
	}
	args := []string{"-config", h.configPath}
	if plan != "" {
		args = append(args, "-allow-fault-injection", "-fault-file", plan)
	}
	cmd := exec.Command(h.binary, args...)
	cmd.Stdout = log
	cmd.Stderr = log
	p := &crashProcess{cmd: cmd, done: make(chan error, 1), log: log}
	if err = cmd.Start(); err != nil {
		log.Close()
		h.t.Fatal(err)
	}
	go func() { p.done <- cmd.Wait() }()
	h.child = p
	ready, cancel := context.WithTimeout(h.ctx, 12*time.Second)
	defer cancel()
	client, err := Dial(ready, ClientConfig{UnixSocket: h.socket, RPCTimeout: 10 * time.Second, ReconcileTimeout: 300 * time.Millisecond})
	if err != nil {
		h.t.Fatalf("child startup failed (runner-%02d.log): %v", h.sequence, err)
	}
	h.client = client
}
func (h *crashHarness) stop() {
	if h.client != nil {
		h.client.Close()
		h.client = nil
	}
	if h.child == nil {
		return
	}
	p := h.child
	h.child = nil
	if !p.exited {
		p.cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-p.done:
		case <-time.After(12 * time.Second):
			p.cmd.Process.Kill()
			<-p.done
		}
	}
	p.log.Close()
}
func (h *crashHarness) crashed(plan string) json.RawMessage {
	h.t.Helper()
	select {
	case err := <-h.child.done:
		h.child.exited = true
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ExitCode() != 86 {
			h.t.Fatalf("expected real process exit86, got %v", err)
		}
	case <-time.After(15 * time.Second):
		h.t.Fatal("fault did not kill runner process")
	}
	marker, err := os.ReadFile(plan + ".used")
	if err != nil {
		h.t.Fatal(err)
	}
	var event struct {
		PID int `json:"pid"`
	}
	if json.Unmarshal(marker, &event) != nil || event.PID != h.child.cmd.Process.Pid {
		h.t.Fatal("marker does not identify killed runner process")
	}
	return marker
}
func (h *crashHarness) plan(point, action string, id, op domain.ID) string {
	dir := filepath.Join(h.config.RootDir, "operator-faults")
	if err := os.MkdirAll(dir, 0700); err != nil {
		h.t.Fatal(err)
	}
	p := map[string]any{"point": point, "action": action, "tenant_id": "runner-fault-acceptance", "run_id": id, "workspace_id": id, "operation_id": op, "epoch": 1, "expires_at": time.Now().Add(5 * time.Minute)}
	if action == "delay" {
		p["delay_millis"] = 2000
	}
	raw, _ := json.Marshal(p)
	file := filepath.Join(dir, string(id)+".json")
	f, err := os.OpenFile(file, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		h.t.Fatal(err)
	}
	_, err = f.Write(raw)
	if err == nil {
		err = f.Sync()
	}
	f.Close()
	if err != nil {
		h.t.Fatal(err)
	}
	return file
}
func (h *crashHarness) report(e *crashEvidence) {
	e.Passed = !h.t.Failed()
	e.ControlPlaneEvidence = "N/A: isolated runner RPC acceptance; no PostgreSQL run/effect/budget/SSE claim"
	raw, err := json.MarshalIndent(e, "", "  ")
	if err == nil {
		err = os.WriteFile(filepath.Join(h.evidence, e.Case+".json"), append(raw, '\n'), 0600)
	}
	if err != nil {
		h.t.Error(err)
	}
}
func (h *crashHarness) prepare(id domain.ID, epoch uint64) runner.Workspace {
	h.t.Helper()
	w, err := h.client.PrepareWorkspace(h.ctx, runner.PrepareRequest{WorkspaceRequest: h.binding(id, epoch, time.Minute), SourceID: "clamp", ProfileID: "python-clamp"})
	if err != nil {
		h.t.Fatal(err)
	}
	return w
}
func (h *crashHarness) initialization(point string) {
	id := h.unique("init")
	e := &crashEvidence{Case: point, WorkspaceID: id, MatrixComponents: []string{"initialization-volume-lease"}}
	defer h.report(e)
	plan := h.plan(point, "exit", id, "")
	h.start(plan)
	_, err := h.client.PrepareWorkspace(h.ctx, runner.PrepareRequest{WorkspaceRequest: h.binding(id, 1, time.Minute), SourceID: "clamp", ProfileID: "python-clamp"})
	if err == nil {
		h.t.Fatal("interrupted initialization returned success")
	}
	e.FaultMarker = h.crashed(plan)
	err = h.db.QueryRow(`SELECT source_hash,epoch,EXISTS(SELECT 1 FROM workspaces WHERE id=?),(SELECT count(*) FROM operations WHERE workspace_id=?) FROM volume_leases WHERE workspace_id=? AND released=0`, id, id, id).Scan(&e.SourceHash, &e.LeaseEpoch, &e.WorkspaceExistsBeforeResume, &e.OperationsBeforeResume)
	if err != nil || e.SourceHash == "" || e.LeaseEpoch != 1 || e.WorkspaceExistsBeforeResume || e.OperationsBeforeResume != 0 {
		h.t.Fatalf("initialization reservation evidence: %+v %v", e, err)
	}
	h.start("")
	w := h.prepare(id, 2)
	if w.BaselineHash != e.SourceHash || w.Epoch != 2 {
		h.t.Fatal("resume source/epoch mismatch")
	}
	h.cleanup(id, 2, e)
}
func (h *crashHarness) request(id, op domain.ID, epoch, revision uint64, code string) runner.OperationRequest {
	raw, _ := json.Marshal(runner.CommandArgs{Command: []string{"python", "-I", "-B", "-c", code}})
	raw, _ = domain.CanonicalJSON(raw)
	sum := sha256.Sum256(raw)
	return runner.OperationRequest{WorkspaceRequest: h.binding(id, epoch, time.Minute), OperationID: op, ExpectedRevision: revision, Kind: "run_command", Args: raw, ArgsHash: hex.EncodeToString(sum[:]), PolicyVersion: "runner-crash-v1", Deadline: time.Now().Add(45 * time.Second).UTC()}
}
func (h *crashHarness) wait(id, op domain.ID, epoch uint64) runner.Operation {
	h.t.Helper()
	until := time.Now().Add(25 * time.Second)
	for {
		value, err := h.client.InspectOperation(h.ctx, runner.InspectRequest{WorkspaceRequest: h.binding(id, epoch, time.Minute), OperationID: op})
		if err != nil {
			h.t.Fatal(err)
		}
		if value.Status.Terminal() {
			return value
		}
		if time.Now().After(until) {
			h.t.Fatalf("operation unresolved (workspace retained): %s %s", value.Status, value.Error)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
func (h *crashHarness) checkout(id domain.ID) string {
	var slot string
	if err := h.db.QueryRow(`SELECT slot_id FROM volume_leases WHERE workspace_id=? AND released=0`, id).Scan(&slot); err != nil {
		h.t.Fatal(err)
	}
	for _, spec := range h.config.VolumeSlots {
		if spec.ID == slot {
			return filepath.Join(spec.MountPath, "workspace-"+string(id), "checkout")
		}
	}
	h.t.Fatal("unknown slot")
	return ""
}
func (h *crashHarness) operation(point string) {
	id := h.unique("fault")
	op := domain.ID(string(id) + "-op")
	e := &crashEvidence{Case: point, WorkspaceID: id, OperationID: op, LeaseEpoch: 1, MatrixComponents: []string{"F05-runner", "F07-runner"}}
	defer h.report(e)
	actualPoint := point
	action := "exit"
	if point == "takeover" {
		actualPoint = "after_docker_start"
		e.MatrixComponents = []string{"F08-runner"}
	}
	if point == "after_start_acceptance" {
		action = "delay"
	}
	plan := h.plan(actualPoint, action, id, op)
	h.start(plan)
	h.prepare(id, 1)
	code := `import os
with open('fault-count.txt','a') as f:
 f.write('1\n');f.flush();os.fsync(f.fileno())`
	if point == "takeover" {
		code = `import os,time
with open('old-writer.txt','a') as f:
 for i in range(600):
  f.write(str(time.time_ns())+'\n');f.flush();os.fsync(f.fileno());time.sleep(.05)`
	}
	req := h.request(id, op, 1, 1, code)
	if point == "takeover" {
		req.WorkspaceRequest = h.binding(id, 1, 1500*time.Millisecond)
	}
	started := time.Now().Add(-time.Second)
	if point == "after_start_acceptance" {
		h.client.timeout = 100 * time.Millisecond
	}
	accepted, err := h.client.StartOperation(h.ctx, req)
	if point == "after_start_acceptance" {
		if err != nil || accepted.Request.OperationID != op {
			h.t.Fatalf("timeout failed same-ID inspection: %+v %v", accepted, err)
		}
		h.client.timeout = 10 * time.Second
		marker, err := os.ReadFile(plan + ".used")
		if err != nil {
			h.t.Fatal(err)
		}
		e.FaultMarker = marker
	} else {
		e.FaultMarker = h.crashed(plan)
		h.start("")
	}
	observed, err := h.client.InspectOperation(h.ctx, runner.InspectRequest{WorkspaceRequest: h.binding(id, 1, time.Minute), OperationID: op})
	if err != nil {
		h.t.Fatal(err)
	}
	e.InitialStatus = observed.Status
	if err = h.db.QueryRow(`SELECT dispatch_started,docker_start_intent FROM operations WHERE id=?`, op).Scan(&e.DispatchStarted, &e.DockerStartIntent); err != nil {
		h.t.Fatal(err)
	}
	if (point == "after_prepared" || point == "after_docker_create") && e.DockerStartIntent {
		h.t.Fatal("unexpected durable start intent before boundary")
	}
	epoch := uint64(1)
	if point == "after_prepared" || point == "after_docker_create" {
		if observed.Status != runner.Unknown {
			h.t.Fatalf("uncertain gap was guessed: %s", observed.Status)
		}
		if _, err = h.client.ReleaseWorkspace(h.ctx, h.binding(id, 1, time.Minute)); !errors.Is(err, domain.ErrReconciliation) {
			h.t.Fatalf("unknown effect released capacity: %v", err)
		}
		observed, err = h.client.CancelOperation(h.ctx, runner.InspectRequest{WorkspaceRequest: h.binding(id, 1, time.Minute), OperationID: op})
		if err != nil || observed.Status != runner.Cancelled {
			h.t.Fatalf("never-started proof failed: %+v %v", observed, err)
		}
	} else if point == "takeover" {
		// Let the original capability expire while the actual orphan writer lives.
		time.Sleep(1600 * time.Millisecond)
		if _, err = h.client.StartOperation(h.ctx, req); !errors.Is(err, domain.ErrFenced) {
			h.t.Fatalf("expired old grant accepted: %v", err)
		}
		adopted, err := h.client.AdoptWorkspace(h.ctx, h.binding(id, 2, time.Minute))
		if err != nil || !adopted.NoActiveOperations {
			h.t.Fatalf("takeover lacks stop proof: %+v %v", adopted, err)
		}
		epoch = 2
		e.LeaseEpoch = 2
		old := h.binding(id, 1, time.Minute)
		stale := req
		stale.WorkspaceRequest = old
		if _, err = h.client.StartOperation(h.ctx, stale); !errors.Is(err, domain.ErrFenced) {
			h.t.Fatalf("old epoch writer reopened: %v", err)
		}
		e.OldEpochRejected = true
		oldBytes, err := os.ReadFile(filepath.Join(h.checkout(id), "old-writer.txt"))
		if err != nil || len(oldBytes) == 0 {
			h.t.Fatalf("old writer never ran: %v", err)
		}
		for _, line := range strings.Fields(string(oldBytes)) {
			n, parseErr := strconv.ParseInt(line, 10, 64)
			if parseErr != nil || n <= 0 {
				h.t.Fatal("invalid old writer timestamp")
			}
			if len(e.OldWriterTimestamps) > 0 && n <= e.OldWriterTimestamps[len(e.OldWriterTimestamps)-1] {
				h.t.Fatal("old writer timestamps not increasing")
			}
			e.OldWriterTimestamps = append(e.OldWriterTimestamps, n)
		}
		next := h.request(id, domain.ID(string(op)+"-new"), 2, adopted.Workspace.Revision, `import os,time
with open('new-writer.txt','w') as f:
 f.write(str(time.time_ns())+'\n');f.flush();os.fsync(f.fileno())`)
		if _, err = h.client.StartOperation(h.ctx, next); err != nil {
			h.t.Fatal(err)
		}
		newOp := h.wait(id, next.OperationID, 2)
		if newOp.Status != runner.Succeeded {
			h.t.Fatal("replacement writer failed")
		}
		after, err := os.ReadFile(filepath.Join(h.checkout(id), "old-writer.txt"))
		if err != nil || string(after) != string(oldBytes) {
			h.t.Fatal("old writer continued after no-active adoption proof")
		}
		newBytes, err := os.ReadFile(filepath.Join(h.checkout(id), "new-writer.txt"))
		if err != nil {
			h.t.Fatal(err)
		}
		e.NewWriterFirst, err = strconv.ParseInt(strings.TrimSpace(string(newBytes)), 10, 64)
		if err != nil || e.NewWriterFirst <= e.OldWriterTimestamps[len(e.OldWriterTimestamps)-1] {
			h.t.Fatal("new writer interval overlaps old writer")
		}
		e.WriterExclusive = true
	}
	settled := h.wait(id, op, epoch)
	e.FinalOperation = &settled
	if point == "after_prepared" || point == "after_docker_create" || point == "takeover" {
		if settled.Status != runner.Cancelled {
			h.t.Fatalf("cancel outcome %s", settled.Status)
		}
	} else {
		if settled.Status != runner.Succeeded {
			h.t.Fatalf("observed success lost: %s", settled.Status)
		}
	}
	if point != "takeover" {
		replay, err := h.client.StartOperation(h.ctx, req)
		if err != nil || replay.Receipt != settled.Receipt || replay.AfterHash != settled.AfterHash || replay.AfterRevision != settled.AfterRevision {
			h.t.Fatalf("same ID changed immutable receipt: %v", err)
		}
		e.SameIDReceipt = true
		data, readErr := os.ReadFile(filepath.Join(h.checkout(id), "fault-count.txt"))
		if settled.Status == runner.Succeeded {
			if readErr != nil || string(data) != "1\n" {
				h.t.Fatalf("effect replayed/missing: %q %v", data, readErr)
			}
			e.Counter = string(data)
		} else if !os.IsNotExist(readErr) {
			h.t.Fatal("never-started operation wrote files")
		}
	}
	h.events(started, settled.JobID, e)
	expectedStarts := 1
	if point == "after_prepared" || point == "after_docker_create" {
		expectedStarts = 0
	}
	expectedCreates := 1
	if point == "after_prepared" {
		expectedCreates = 0
	}
	if e.DockerStarts != expectedStarts || e.DockerCreates != expectedCreates {
		h.t.Fatalf("unexpected real daemon execution counts: creates=%d starts=%d", e.DockerCreates, e.DockerStarts)
	}
	h.cleanup(id, epoch, e)
}
func (h *crashHarness) events(since time.Time, job string, e *crashEvidence) {
	observe, cancel := context.WithTimeout(h.ctx, 8*time.Second)
	defer cancel()
	args := []string{"--host", h.config.DockerHost, "events", "--since", since.Format(time.RFC3339Nano), "--until", time.Now().Format(time.RFC3339Nano), "--filter", "container=" + job, "--format", "{{json .}}"}
	raw, err := exec.CommandContext(observe, "docker", args...).CombinedOutput()
	if err != nil {
		h.t.Fatalf("read-only Docker events failed: %v %s", err, raw)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		if line == "" {
			continue
		}
		var value struct {
			Action string `json:"Action"`
		}
		if json.Unmarshal([]byte(line), &value) != nil {
			h.t.Fatal("invalid Docker event")
		}
		switch value.Action {
		case "create":
			e.DockerCreates++
		case "start":
			e.DockerStarts++
		}
		e.DockerEvents = append(e.DockerEvents, json.RawMessage(line))
	}
}
func (h *crashHarness) cleanup(id domain.ID, epoch uint64, e *crashEvidence) {
	h.t.Helper()
	stop, err := h.client.StopWorkspace(h.ctx, h.binding(id, epoch, time.Minute))
	if err != nil || !stop.NoActiveOperations {
		h.t.Fatalf("test workspace retained without stop proof: %s %v", id, err)
	}
	e.NoActive = true
	snap, err := h.client.SealSnapshot(h.ctx, h.binding(id, epoch, time.Minute))
	if err != nil || snap.Artifact.ObjectKey == "" {
		h.t.Fatalf("test workspace retained without snapshot: %s %v", id, err)
	}
	e.Snapshot = snap.Artifact.SHA256
	for _, key := range []string{stop.Ref.ObjectKey, snap.Artifact.ObjectKey} {
		var count int
		if err = h.db.QueryRow(`SELECT count(*) FROM runner_artifacts WHERE object_key=?`, key).Scan(&count); err != nil || count != 1 {
			h.t.Fatal("returned artifact missing durable pin")
		}
		e.Pins += count
	}
	if e.FinalOperation != nil {
		var count int
		if err = h.db.QueryRow(`SELECT count(*) FROM runner_artifacts WHERE object_key=?`, e.FinalOperation.Receipt.ObjectKey).Scan(&count); err != nil || count != 1 {
			h.t.Fatal("operation receipt missing durable pin")
		}
		e.Pins += count
	}
	released, err := h.client.ReleaseWorkspace(h.ctx, h.binding(id, epoch, time.Minute))
	if err != nil || !released.Released {
		h.t.Fatalf("test workspace release failed: %v", err)
	}
	var active int
	if err = h.db.QueryRow(`SELECT count(*) FROM volume_leases WHERE workspace_id=? AND released=0`, id).Scan(&active); err != nil || active != 0 {
		h.t.Fatal("released workspace retained allocation")
	}
	e.Released = true
}
