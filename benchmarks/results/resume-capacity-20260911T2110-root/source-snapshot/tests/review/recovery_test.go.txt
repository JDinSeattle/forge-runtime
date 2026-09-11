package review_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/db"
	"github.com/JDinSeattle/forge-runtime/internal/artifact"
	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	"github.com/JDinSeattle/forge-runtime/internal/runner"
	flow "github.com/JDinSeattle/forge-runtime/internal/runtime"
	"github.com/JDinSeattle/forge-runtime/internal/sandbox"
	"github.com/jackc/pgx/v5"
)

type recoveryConfig struct {
	ID                   string               `json:"id"`
	Root                 string               `json:"root"`
	SourceDatabase       string               `json:"source_database"`
	TargetDatabase       string               `json:"target_database"`
	DockerHost           string               `json:"docker_host"`
	Image                string               `json:"image"`
	VolumeSlots          []sandbox.VolumeSpec `json:"volume_slots"`
	PairedManifestSHA256 string               `json:"paired_manifest_sha256"`
}
type recoveryEvidence struct {
	PID                int                    `json:"pid"`
	At                 time.Time              `json:"at"`
	Runs               []persistence.Run      `json:"runs"`
	CompletedOperation runner.Operation       `json:"completed_operation"`
	UnknownOperation   runner.Operation       `json:"unknown_operation"`
	Artifacts          []persistence.Artifact `json:"artifacts"`
	SourceLaunches     int64                  `json:"source_launches"`
}

func loadRecovery(t *testing.T) (context.Context, recoveryConfig) {
	t.Helper()
	path := os.Getenv("FORGE_RECOVERY_CONFIG")
	if path == "" {
		t.Skip("operator-only paired recovery rehearsal: FORGE_RECOVERY_CONFIG is required")
	}
	var c recoveryConfig
	raw, err := os.ReadFile(path)
	if err != nil || json.Unmarshal(raw, &c) != nil {
		t.Fatal("cannot read recovery configuration")
	}
	if !filepath.IsAbs(c.Root) || len(c.VolumeSlots) != 4 || c.SourceDatabase != "forge_recovery_s_"+c.ID || c.TargetDatabase != "forge_recovery_t_"+c.ID || !strings.HasPrefix(c.DockerHost, "unix:///") || !strings.Contains(c.Image, "@sha256:") {
		t.Fatal("recovery fixture scope is incomplete")
	}
	if c.Root != filepath.Join(filepath.Dir(filepath.Dir(c.Root)), "recovery-rehearsals", c.ID) {
		t.Fatal("recovery root is outside the dedicated rehearsal directory")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	return ctx, c
}

func recoveryStore(t *testing.T, ctx context.Context, c recoveryConfig, source bool) *persistence.Store {
	t.Helper()
	u, err := url.Parse(os.Getenv("FORGE_RECOVERY_ADMIN_DSN"))
	if err != nil || u.Hostname() != "127.0.0.1" || u.Path != "/forge" {
		t.Fatal("explicit loopback /forge administrative DSN required")
	}
	name := c.TargetDatabase
	if source {
		name = c.SourceDatabase
		admin, err := pgx.Connect(ctx, u.String())
		if err != nil {
			t.Fatal(err)
		}
		_, err = admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()+" TEMPLATE template0")
		_ = admin.Close(ctx)
		if err != nil {
			t.Fatal(err)
		}
	}
	u.Path = "/" + name
	if source {
		if err := db.Migrate(ctx, u.String()); err != nil {
			t.Fatal(err)
		}
	}
	s, err := persistence.Open(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s
}

type recoveryBackend struct {
	*sandbox.Docker
	starts      atomic.Int64
	restoreOnly bool
}

func (b *recoveryBackend) Start(ctx context.Context, spec sandbox.JobSpec) (sandbox.Job, error) {
	b.starts.Add(1)
	if b.restoreOnly {
		return sandbox.Job{}, fmt.Errorf("paired recovery inspection never authorizes a new external launch")
	}
	return b.Docker.Start(ctx, spec)
}

func recoveryEngine(t *testing.T, ctx context.Context, c recoveryConfig, source bool, fault func(string) error) (*runner.Engine, *runner.Signer, *artifact.LocalStore, *recoveryBackend) {
	t.Helper()
	label := "target"
	if source {
		label = "source"
	}
	root := filepath.Join(c.Root, label)
	key, err := os.ReadFile(filepath.Join(root, "runner.key"))
	if err != nil {
		t.Fatal(err)
	}
	signer, err := runner.NewSigner(key)
	if err != nil {
		t.Fatal(err)
	}
	objects, err := artifact.NewLocalStore(filepath.Join(root, "runtime/artifacts"), 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = objects.Close() })
	backend := &recoveryBackend{Docker: &sandbox.Docker{Host: c.DockerHost, WorkspaceQuota: sandbox.FixedVolumeQuota{Slots: c.VolumeSlots}}, restoreOnly: !source}
	if err := backend.Check(ctx); err != nil {
		t.Fatal(err)
	}
	engine, err := runner.Open(runner.Config{RootDir: filepath.Join(root, "runtime/engine"), JournalPath: filepath.Join(root, "runtime/runner.db"), Artifacts: objects, Backend: backend, Signer: signer, VolumeSlots: c.VolumeSlots, Sources: map[string]string{"recovery": filepath.Join(root, "input")}, Profiles: map[string]sandbox.Profile{"recovery": {ID: "recovery", Image: c.Image, User: "1000:1000", MemoryBytes: 128 << 20, WorkspaceQuotaBytes: 256 << 20, CPUs: 1, PIDs: 32}}, Fault: fault})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	return engine, signer, objects, backend
}

func recoveryGrant(t *testing.T, signer *runner.Signer, r persistence.Run) runner.WorkspaceRequest {
	t.Helper()
	now := time.Now()
	token, err := signer.Sign(runner.Claims{TenantID: r.TenantID, RunID: r.ID, WorkspaceID: r.ID, Epoch: r.State.Lease.Epoch, Permissions: []string{"prepare", "execute", "inspect", "snapshot"}, IssuedAt: now, ExpiresAt: now.Add(time.Minute)}, r.State.Lease.Until)
	if err != nil {
		t.Fatal(err)
	}
	return runner.WorkspaceRequest{TenantID: r.TenantID, RunID: r.ID, WorkspaceID: r.ID, Epoch: r.State.Lease.Epoch, Grant: token}
}

// The isolated target has no scheduler. This explicit operator fixture authority
// permits inspecting the clone and running a file read, while its backend rejects
// every process launch. It neither renews nor claims a copied PostgreSQL lease.
func recoveryOperatorGrant(t *testing.T, signer *runner.Signer, r persistence.Run) runner.WorkspaceRequest {
	t.Helper()
	now := time.Now()
	token, err := signer.Sign(runner.Claims{TenantID: r.TenantID, RunID: r.ID, WorkspaceID: r.ID, Epoch: r.State.Lease.Epoch, Permissions: []string{"execute", "inspect", "snapshot"}, IssuedAt: now, ExpiresAt: now.Add(time.Minute)}, now.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	return runner.WorkspaceRequest{TenantID: r.TenantID, RunID: r.ID, WorkspaceID: r.ID, Epoch: r.State.Lease.Epoch, Grant: token}
}

func recoveryJSON(t *testing.T, path string, v any) {
	t.Helper()
	raw, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.Write(append(raw, '\n')); err == nil {
		err = f.Sync()
	}
	_ = f.Close()
	if err != nil {
		t.Fatal(err)
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	err = dir.Sync()
	_ = dir.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func recoveryPublish(t *testing.T, ctx context.Context, s *persistence.Store, ref artifact.Ref) string {
	t.Helper()
	id := domain.ID("artifact_" + ref.SHA256)
	if err := s.PublishArtifact(ctx, persistence.Artifact{TenantID: ref.TenantID, RunID: ref.RunID, ID: id, Kind: ref.Kind, ObjectKey: ref.ObjectKey, SHA256: ref.SHA256, ByteSize: ref.Size}); err != nil {
		t.Fatal(err)
	}
	return string(id)
}

func recoveryWait(t *testing.T, ctx context.Context, engine *runner.Engine, request runner.OperationRequest) runner.Operation {
	t.Helper()
	for {
		o, err := engine.InspectOperation(ctx, runner.InspectRequest{WorkspaceRequest: request.WorkspaceRequest, OperationID: request.OperationID})
		if err != nil {
			t.Fatal(err)
		}
		if o.Status.Terminal() {
			if o.Status != runner.Succeeded {
				t.Fatalf("fixture operation did not succeed: %+v", o)
			}
			return o
		}
		if ctx.Err() != nil {
			t.Fatal(ctx.Err())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Run this compiled test binary directly. Exit 86 deliberately leaves the real
// SQLite WAL after all fixture execution stopped; backup must use the WAL-aware
// SQLite backup API. No application API or production worker is started.
func TestRecoverySourceProcess(t *testing.T) {
	ctx, c := loadRecovery(t)
	s := recoveryStore(t, ctx, c, true)
	uncertain := false
	engine, signer, objects, backend := recoveryEngine(t, ctx, c, true, func(point string) error {
		if point == "after_prepared" && uncertain {
			return errors.New("recovery fixture: dispatch never reached Docker")
		}
		return nil
	})
	if err := s.BootstrapTenant(ctx, "recovery_tenant", "recovery_operator", "admin"); err != nil {
		t.Fatal(err)
	}
	if err := s.RegisterRunner(ctx, "recovery_runner", "in-process-isolated-recovery-fixture", 4); err != nil {
		t.Fatal(err)
	}
	p, err := s.CreateProject(ctx, "recovery_tenant", "paired recovery fixture", "recovery", "recovery")
	if err != nil {
		t.Fatal(err)
	}
	evidence := recoveryEvidence{PID: os.Getpid(), At: time.Now()}
	for index := range 2 {
		_, _, err := s.Submit(ctx, persistence.SubmitRequest{TenantID: "recovery_tenant", PrincipalID: "recovery_operator", ProjectID: p.ID, Task: "paired recovery protocol fixture", BaseCommit: "fixture-source", Config: persistence.Config{Provider: "fake", Model: "not-called", MaxModelRounds: 4, MaxToolCalls: 4, MaxRuntimeSeconds: 1800}}, fmt.Sprint(index))
		if err != nil {
			t.Fatal(err)
		}
		r, err := s.ClaimOnRunner(ctx, "recovery_source_driver", 5*time.Minute, "recovery_runner")
		if err != nil {
			t.Fatal(err)
		}
		grant := recoveryGrant(t, signer, r)
		w, err := engine.PrepareWorkspace(ctx, runner.PrepareRequest{WorkspaceRequest: grant, SourceID: "recovery", ProfileID: "recovery"})
		if err != nil {
			t.Fatal(err)
		}
		body, _ := json.Marshal(w)
		ref, err := objects.Put(ctx, r.TenantID, r.ID, "recovery_control_evidence", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		controlRef := recoveryPublish(t, ctx, s, ref)
		advance := func(event flow.Event) {
			event.ExpectedVersion, event.Owner, event.Epoch = r.State.Version, r.State.Lease.Owner, r.State.Lease.Epoch
			r, err = s.Advance(ctx, r.TenantID, r.ID, event)
			if err != nil {
				t.Fatal(err)
			}
		}
		advance(flow.Event{Kind: flow.EventWorkspaceReady, WorkspaceRevision: w.Revision, OutputRef: controlRef})
		advance(flow.Event{Kind: flow.EventContextBuilt, OutputRef: controlRef})
		advance(flow.Event{Kind: flow.EventModelCompleted, Complete: true, OutputRef: controlRef})
		command := []string{"python", "-I", "-c", "from pathlib import Path; import os; p=Path('execution-count.txt'); assert not p.exists(); p.write_text('1\\n'); d=Path('private'); d.mkdir(mode=0o700); q=d/'evidence.txt'; q.write_text('paired recovery bytes\\n'); q.chmod(0o600); print('completed once')"}
		args, _ := json.Marshal(runner.CommandArgs{Command: command})
		args, err = domain.CanonicalJSON(args)
		if err != nil {
			t.Fatal(err)
		}
		hash := sha256.Sum256(args)
		effectID := domain.ID(fmt.Sprintf("recovery_operation_%d", index))
		advance(flow.Event{Kind: flow.EventToolsValidated, Complete: true, OutputRef: controlRef, Effects: []flow.Effect{{ID: effectID, Kind: "run_command", Args: args, ArgsHash: hex.EncodeToString(hash[:]), ExpectedRevision: w.Revision, PolicyVersion: "explicit-operator-recovery-v1", Status: flow.EffectPlanned}}})
		request := runner.OperationRequest{WorkspaceRequest: grant, OperationID: effectID, ExpectedRevision: r.State.PendingEffect.ExpectedRevision, Kind: "run_command", Args: args, ArgsHash: hex.EncodeToString(hash[:]), PolicyVersion: "explicit-operator-recovery-v1", Deadline: time.Now().Add(20 * time.Minute)}
		uncertain = index == 1
		o, startErr := engine.StartOperation(ctx, request)
		if uncertain {
			if startErr == nil || o.Status != runner.Prepared {
				t.Fatalf("uncertain fixture did not stop at durable prepare: %+v %v", o, startErr)
			}
			advance(flow.Event{Kind: flow.EventEffectUncertain, Reason: "source process stopped after durable prepare; no replay authorization from SQL"})
			o.Request.Grant = ""
			evidence.UnknownOperation = o
		} else {
			if startErr != nil {
				t.Fatal(startErr)
			}
			o = recoveryWait(t, ctx, engine, request)
			receipt := recoveryPublish(t, ctx, s, o.Receipt)
			advance(flow.Event{Kind: flow.EventEffectCompleted, Receipt: &flow.EffectReceipt{EffectID: effectID, ArgsHash: request.ArgsHash, Epoch: request.Epoch, Status: flow.EffectSucceeded, BeforeRevision: request.ExpectedRevision, AfterRevision: o.AfterRevision, Ref: receipt, Settled: true}})
			o.Request.Grant = ""
			evidence.CompletedOperation = o
		}
		fresh, err := s.GetRun(ctx, r.TenantID, r.ID)
		if err != nil {
			t.Fatal(err)
		}
		evidence.Runs = append(evidence.Runs, fresh)
		listed, err := s.ListArtifacts(ctx, r.TenantID, r.ID, "", 1000)
		if err != nil {
			t.Fatal(err)
		}
		evidence.Artifacts = append(evidence.Artifacts, listed...)
	}
	evidence.SourceLaunches = backend.starts.Load()
	if evidence.SourceLaunches != 1 {
		t.Fatalf("expected one actual source Docker launch, got %d", evidence.SourceLaunches)
	}
	recoveryJSON(t, filepath.Join(c.Root, "source-evidence.json"), evidence)
	fmt.Fprintln(os.Stdout, "RECOVERY_SOURCE_QUIESCED; intentional exit 86 preserves committed SQLite WAL")
	os.Exit(86)
}

func TestRecoveryRestoredProcess(t *testing.T) {
	ctx, c := loadRecovery(t)
	manifest, err := os.ReadFile(filepath.Join(c.Root, "backup/manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(manifest)
	if c.PairedManifestSHA256 == "" || c.PairedManifestSHA256 != hex.EncodeToString(sum[:]) {
		t.Fatal("restored runner requires the verified paired-backup manifest")
	}
	var evidence recoveryEvidence
	raw, err := os.ReadFile(filepath.Join(c.Root, "backup/source-evidence.json"))
	if err != nil || json.Unmarshal(raw, &evidence) != nil || len(evidence.Runs) != 2 {
		t.Fatal("invalid paired source evidence")
	}
	s := recoveryStore(t, ctx, c, false)
	engine, signer, objects, backend := recoveryEngine(t, ctx, c, false, nil)
	for _, before := range evidence.Runs {
		after, err := s.GetRun(ctx, before.TenantID, before.ID)
		if err != nil {
			t.Fatal(err)
		}
		left, _ := json.Marshal(before.State)
		right, _ := json.Marshal(after.State)
		if !bytes.Equal(left, right) {
			t.Fatal("restoring the paired data changed the control-plane state")
		}
		assertReviewTransitionReplay(t, ctx, s, after)
	}
	for _, a := range evidence.Artifacts {
		got, err := s.GetArtifact(ctx, a.TenantID, a.ID)
		if err != nil || got.SHA256 != a.SHA256 || got.ObjectKey != a.ObjectKey || got.ByteSize != a.ByteSize {
			t.Fatalf("restored READY publication mismatch: %v", err)
		}
		stream, err := objects.Open(ctx, a.TenantID, a.RunID, artifact.Ref{TenantID: a.TenantID, RunID: a.RunID, ObjectKey: a.ObjectKey, SHA256: a.SHA256, Size: a.ByteSize, Kind: a.Kind})
		if err != nil {
			t.Fatal(err)
		}
		_, err = io.Copy(io.Discard, stream)
		_ = stream.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	known, unknown := evidence.Runs[0], evidence.Runs[1]
	old := evidence.CompletedOperation
	old.Request.WorkspaceRequest = recoveryOperatorGrant(t, signer, known)
	replayed, err := engine.StartOperation(ctx, old.Request)
	if !old.Request.Deadline.After(time.Now()) && errors.Is(err, domain.ErrInvalid) {
		replayed, err = engine.InspectOperation(ctx, runner.InspectRequest{WorkspaceRequest: old.Request.WorkspaceRequest, OperationID: old.Request.OperationID})
	}
	if err != nil || replayed.Status != runner.Succeeded || replayed.Receipt.SHA256 != old.Receipt.SHA256 || replayed.JobID != old.JobID {
		t.Fatalf("original operation facts were not reused: %+v %v", replayed, err)
	}
	// A new file-tool operation reads task-owned private bytes from the restored
	// mounted image through the actual mapped runner, not a SQL row or dump file.
	readArgs, _ := domain.CanonicalJSON([]byte(`{"path":"private/evidence.txt"}`))
	readHash := sha256.Sum256(readArgs)
	read := runner.OperationRequest{WorkspaceRequest: recoveryOperatorGrant(t, signer, known), OperationID: "recovery_read_restored_bytes", ExpectedRevision: old.AfterRevision, Kind: "read_file", Args: readArgs, ArgsHash: hex.EncodeToString(readHash[:]), PolicyVersion: "explicit-operator-recovery-v1", Deadline: time.Now().Add(time.Minute)}
	if _, err := engine.StartOperation(ctx, read); err != nil {
		t.Fatal(err)
	}
	readResult := recoveryWait(t, ctx, engine, read)
	var contents struct {
		Content string `json:"content"`
	}
	if json.Unmarshal(readResult.Result, &contents) != nil || contents.Content != "paired recovery bytes\n" {
		t.Fatalf("restored private file bytes mismatch: %s", readResult.Result)
	}
	snapshot, err := engine.SealSnapshot(ctx, recoveryOperatorGrant(t, signer, known))
	if err != nil || snapshot.Workspace.Revision != known.State.WorkspaceRevision {
		t.Fatalf("actual restored snapshot failed: %+v %v", snapshot, err)
	}
	u := evidence.UnknownOperation
	u.Request.WorkspaceRequest = recoveryOperatorGrant(t, signer, unknown)
	observed, err := engine.InspectOperation(ctx, runner.InspectRequest{WorkspaceRequest: u.Request.WorkspaceRequest, OperationID: u.Request.OperationID})
	if err != nil || observed.Status != runner.Unknown {
		t.Fatalf("missing external evidence became replay permission: %+v %v", observed, err)
	}
	duplicate, err := engine.StartOperation(ctx, u.Request)
	if !u.Request.Deadline.After(time.Now()) && errors.Is(err, domain.ErrInvalid) {
		duplicate, err = engine.InspectOperation(ctx, runner.InspectRequest{WorkspaceRequest: u.Request.WorkspaceRequest, OperationID: u.Request.OperationID})
	}
	if err != nil || duplicate.Status != runner.Unknown || duplicate.JobID != u.JobID {
		t.Fatalf("uncertain original operation changed identity: %+v %v", duplicate, err)
	}
	if backend.starts.Load() != 0 {
		t.Fatal("restored inspection attempted an external command launch")
	}
	afterUnknown, err := s.GetRun(ctx, unknown.TenantID, unknown.ID)
	if err != nil || afterUnknown.State.Status != domain.StatusNeedsReconciliation || afterUnknown.State.PendingEffect.Status != flow.EffectUnknown {
		t.Fatal("restored SQL unknown obligation was silently settled")
	}
	recoveryJSON(t, filepath.Join(c.Root, "recovery-report.json"), map[string]any{"passed": true, "paired_manifest_sha256": c.PairedManifestSHA256, "source_database": c.SourceDatabase, "target_database": c.TargetDatabase, "restored_ready_artifacts_verified": len(evidence.Artifacts), "private_file_read_via_restored_runner": true, "restored_snapshot_sha256": snapshot.Artifact.SHA256, "original_operation_receipt_sha256": replayed.Receipt.SHA256, "unknown_operation": string(observed.Status), "unknown_run": afterUnknown.State.Status, "source_actual_launches": evidence.SourceLaunches, "restore_actual_launches": backend.starts.Load(), "production_worker_attached": false, "scope": "Quiescent paired PostgreSQL + WAL-aware SQLite + four fixed ext4 image + artifact restore into separate databases/directories/loop devices; actual source command, restored mapped file reads/snapshot; uncertainty remains blocked"})
}
