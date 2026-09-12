//go:build linux

package applicationfaults

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	migrations "github.com/JDinSeattle/forge-runtime/db"
	"github.com/JDinSeattle/forge-runtime/internal/application"
	"github.com/JDinSeattle/forge-runtime/internal/configuration"
	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	"github.com/JDinSeattle/forge-runtime/internal/provider"
	"github.com/JDinSeattle/forge-runtime/internal/quota"
	"github.com/JDinSeattle/forge-runtime/internal/runner"
	"github.com/JDinSeattle/forge-runtime/internal/runnerclient"
	"github.com/JDinSeattle/forge-runtime/internal/sandbox"
	"github.com/jackc/pgx/v5"
)

// This opt-in test does not prepare/mount a pool, stop an existing service,
// rebind an owner, or open Engine in-process. Every execution and recovery RPC
// reaches an actual forge-runner process with the production Docker backend.
func TestRealWorkerRunnerSIGTERM(t *testing.T) {
	if os.Getenv("FORGE_RUN_WORKER_RUNNER_SIGTERM") != "1" {
		t.Skip("operator opt-in: dedicated new lifecycle pool, actual runner/worker executables and private PostgreSQL")
	}
	path := os.Getenv("FORGE_WORKER_RUNNER_SIGTERM_ACCEPTANCE")
	if !filepath.IsAbs(path) {
		t.Fatal("absolute acceptance manifest required")
	}
	var a lifecycleAcceptance
	var c lifecycleRunnerConfig
	if err := readJSON(path, &a); err != nil {
		t.Fatal(err)
	}
	if err := readJSON(a.RunnerConfig, &c); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	preflight, err := lifecyclePreflight(ctx, a, c)
	if err != nil {
		t.Fatal("no process or schema started; preflight:", err)
	}
	u, err := url.Parse(os.Getenv("FORGE_TEST_DATABASE_URL"))
	if err != nil || u.Hostname() != "127.0.0.1" || u.Port() != "32773" || u.Path != "/forge" || u.Query().Get("search_path") != "" {
		t.Fatal("dedicated loopback32773 /forge admin connection without search_path required")
	}
	if err = os.MkdirAll(a.EvidenceDir, 0700); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(a.EvidenceDir); err != nil || info.Mode().Perm()&0077 != 0 {
		t.Fatal("private evidence root required")
	}
	dir := filepath.Join(a.EvidenceDir, "worker-runner-sigterm")
	if err = os.Mkdir(dir, 0700); err != nil {
		t.Fatal("fresh case directory required; previous evidence retained:", err)
	}
	private := filepath.Join(a.ScopeRoot, "runtime", "sigterm-private")
	if err = os.Mkdir(private, 0700); err != nil {
		t.Fatal("fresh private fixture settings required:", err)
	}
	if err = os.MkdirAll(filepath.Dir(c.Server.UnixSocket), 0700); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(filepath.Dir(c.Server.UnixSocket)); err != nil || info.Mode().Perm()&0077 != 0 {
		t.Fatal("private UDS parent required")
	}
	h := &harness{t: t, ctx: ctx, c: c.runnerSettings, dir: dir, private: private, binary: a.RunnerBinary, localRunner: a.RunnerConfig}
	h.fatal(save(filepath.Join(dir, "preflight.json"), preflight))
	h.fatal(save(filepath.Join(dir, "acceptance-input.json"), a))
	runnerConfigHash, err := sigtermDigest(a.RunnerConfig)
	h.fatal(err)
	h.schema = "appfault_lifecycle_" + strings.ToLower(rand.Text())
	recovery := map[string]any{"purpose": lifecyclePurpose, "schema": h.schema, "runner_config": a.RunnerConfig, "runner_config_sha256": runnerConfigHash, "journal": c.JournalPath, "artifact_root": c.ArtifactRoot, "private_settings": private, "phase": "before_schema", "scope": "same dedicated journal/owner only; no lease/deadline edits, no unknown workspace deletion"}
	defer func() {
		// Parent process exit is not business cancellation. On any failed assert,
		// stop only the children this test spawned and retain Docker/job/spool/PG.
		h.stopAll()
		recovery["test_failed"] = t.Failed()
		recovery["harness_finished_at"] = time.Now().UTC()
		_ = save(filepath.Join(dir, "recovery.json"), recovery)
	}()
	h.fatal(save(filepath.Join(dir, "recovery.json"), recovery))
	admin, err := pgx.Connect(ctx, u.String())
	h.fatal(err)
	_, err = admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{h.schema}.Sanitize())
	h.fatal(err)
	admin.Close(ctx)
	q := u.Query()
	q.Set("search_path", h.schema)
	u.RawQuery = q.Encode()
	h.dsn = u.String()
	h.fatal(migrations.Migrate(ctx, h.dsn))
	h.db, err = persistence.Open(ctx, h.dsn)
	h.fatal(err)
	defer h.db.Close()
	h.workerDSN, h.workerRole, err = restrictedWorker(ctx, h.db, *u, h.schema)
	h.fatal(err)
	role, err := preflightWorkerRole(ctx, h.workerDSN, h.workerRole, h.schema)
	h.fatal(err)
	h.fatal(save(filepath.Join(dir, "worker-role-preflight.json"), role))
	// This private file is needed to recover a failed fixture. It is not copied
	// into public evidence and is never printed or included in child identity.
	h.fatal(save(filepath.Join(private, "database.json"), map[string]string{"worker_dsn": h.workerDSN, "schema": h.schema}))
	var sourceID, source, profileID string
	for k, v := range c.Sources {
		sourceID, source = k, v
	}
	for k := range c.Profiles {
		profileID = k
	}
	hash, err := runner.ComputeSourceHash(ctx, source, 0, 0)
	h.fatal(err)
	key, err := os.ReadFile(c.SigningKeyFile)
	h.fatal(err)
	signer, err := runner.NewSigner(key)
	h.fatal(err)
	tenant := domain.ID("lifecycle-" + strings.ToLower(rand.Text()))
	h.fatal(h.db.BootstrapTenant(ctx, tenant, "fixture-operator", "admin"))
	project, err := h.db.CreateProject(ctx, tenant, "actual worker and runner SIGTERM", sourceID, profileID)
	h.fatal(err)
	h.fatal(h.db.RegisterRunner(ctx, "application-fault-runner", "private-uds", 1))
	h.fatal(quota.New(h.db.Pool).Configure(ctx, quota.Config{CredentialGroup: "application-faults-fake", MaxConcurrent: 2, MaxTokens: 1_000_000, MaxCost: 100_000_000, WindowDuration: time.Hour, FailureThreshold: 3, BreakerCooldown: time.Second}))
	cfg := persistence.Config{Provider: "fake", Model: "fake", MaxModelRounds: 4, MaxToolCalls: 4, MaxCost: 1_000_000, MaxRuntimeSeconds: 170}
	run, _, err := h.db.Submit(ctx, persistence.SubmitRequest{TenantID: tenant, PrincipalID: "fixture-operator", ProjectID: project.ID, Task: "Explicit finite SIGTERM lifecycle fixture; no model-quality or successful-repair claim", BaseCommit: hash, Config: cfg}, "worker-runner-sigterm")
	h.fatal(err)
	scripts := []provider.Script{tool("lifecycle-original", "run_command", runner.CommandArgs{Command: []string{"python", "-I", "-B", "-c", lifecycleCommand}}), tool("unapproved-next", "run_command", runner.CommandArgs{Command: []string{"python", "-I", "-B", "-c", "raise RuntimeError('must remain unapproved')"}})}
	h.s = settings{RunnerConfig: a.RunnerConfig, Socket: c.Server.UnixSocket, Evidence: dir, SourceHash: hash, Mode: lifecyclePurpose, RunID: run.ID, Tenant: tenant, TargetOp: domain.ID(string(run.ID) + "_step_1_op_0"), Scripts: scripts}
	workerConfig := configuration.Config{ArtifactRoot: c.ArtifactRoot, SigningKeyFile: c.SigningKeyFile, WorkerID: "lifecycle-worker", WorkerSlots: 1, RunnerID: "application-fault-runner", Runner: runnerclient.ClientConfig{UnixSocket: c.Server.UnixSocket}, Sources: map[string]configuration.Source{sourceID: {Path: source, Hash: hash, ProfileID: profileID, HasTarget: true}}, Configs: map[string]persistence.Config{"fixture": cfg}, Models: map[string]application.ModelSpec{"fake/fake": modelSpec()}, Providers: map[string]provider.Registry{"fake": {"fake": {ToolCalling: true, ContextWindow: 32768, MaxOutputTokens: 1024}}}, FakeScripts: map[string][]provider.Script{sourceID: scripts}}
	configFile := filepath.Join(private, "worker.json")
	h.fatal(save(configFile, workerConfig))
	workerConfigHash, err := sigtermDigest(configFile)
	h.fatal(err)
	recovery["worker_config_sha256"] = workerConfigHash
	for k, v := range map[string]any{"phase": "before_runner", "tenant_id": tenant, "run_id": run.ID, "operation_id": h.s.TargetOp, "worker_role": h.workerRole, "worker_config": configFile} {
		recovery[k] = v
	}
	h.fatal(save(filepath.Join(dir, "recovery.json"), recovery))
	h.fatal(save(filepath.Join(dir, "fixture.json"), map[string]any{"schema": h.schema, "tenant": tenant, "run_id": run.ID, "source_hash": hash, "source_id": sourceID, "profile_id": profileID, "scripts": scripts, "provider": "deterministic ScriptedProvider; no network or paid usage", "synthetic_price_version": modelSpec().PriceVersion, "default_worker_lease_seconds": 30, "maximum_child_seconds": 70}))
	originalRunner := lifecycleStartRunner(h, a, c, "runner-original")
	lifecycleOpenJournal(h)
	defer h.journal.Close()
	var uuid string
	h.fatal(h.journal.QueryRow(`SELECT id FROM journal_identity WHERE singleton=1`).Scan(&uuid))
	recovery["journal_uuid"] = uuid
	h.fatal(save(filepath.Join(dir, "recovery.json"), recovery))
	originalWorker := sigtermLaunch(h, a.WorkerBinary, configFile, "lifecycle-original")
	h.wait(func() bool { return h.getRun().State.Status == domain.StatusWaitingApproval }, 15*time.Second, "first literal command approval")
	baselineOp := lifecycleReadOperation(h, domain.ID(string(run.ID)+"_0_initial_target"))
	var baselineJob sandbox.Job
	h.fatal(json.Unmarshal(baselineOp.Result, &baselineJob))
	if baselineOp.Status != runner.Failed || !baselineJob.Started || baselineJob.Running || baselineJob.ExitCode != 1 || baselineJob.Log == nil || !sandbox.VerificationLogValid(baselineJob) || !bytes.Contains(baselineJob.Output, []byte("AssertionError")) {
		t.Fatal("baseline target was not a complete real assertion failure")
	}
	h.fatal(save(filepath.Join(dir, "baseline-target.json"), baselineOp))
	h.approve(h.getRun())
	var original runner.Operation
	h.wait(func() bool {
		r := h.getRun()
		if r.State.PendingEffect == nil || r.State.PendingEffect.ID != h.s.TargetOp || r.State.PendingEffect.Status != "in_flight" {
			return false
		}
		op, e := sigtermInspect(h, h.client, signer, r, h.s.TargetOp)
		if e != nil || op.Status != runner.Running {
			return false
		}
		base := lifecycleBase(h)
		data, e := os.ReadFile(filepath.Join(base, "logs", string(h.s.TargetOp)+".spool"))
		if e != nil {
			return false
		}
		stdout, stderr, e := lifecycleFrames(data)
		if e != nil {
			return false
		}
		if !bytes.Contains(stdout, []byte("LIFECYCLE_PARENT_READY")) || !bytes.Contains(stdout, []byte("LIFECYCLE_CHILD_READY")) || !bytes.Contains(stderr, []byte{'p', 'a', 'r', 'e', 'n', 't', '-', 's', 't', 'd', 'e', 'r', 'r', 0, 255}) || !bytes.Contains(stderr, []byte{'c', 'h', 'i', 'l', 'd', '-', 's', 't', 'd', 'e', 'r', 'r', 0, 255}) {
			return false
		}
		original = op
		return true
	}, 15*time.Second, "actual Docker parent/child and both binary streams")
	container := lifecycleObserve(h, c, original, "before-signals")
	if !container.State.Running {
		t.Fatal("command exited before signal boundary")
	}
	cgroupPath := lifecycleCgroup(h, c, container.ID, "", "before-signals", true)
	for k, v := range map[string]any{"phase": "original_running", "container_id": container.ID, "container_name": original.JobID, "epoch": original.Request.Epoch, "original_request": original.Request, "journal_uuid": uuid} {
		recovery[k] = v
	}
	h.fatal(save(filepath.Join(dir, "recovery.json"), recovery))
	h.capture("before-signals")
	lifecycleJournal(h, "before-signals")
	workerSignal := sigtermExit(h, originalWorker, "worker-original", a.WorkerSHA256)
	afterWorker := h.getRun()
	if afterWorker.State.Status != domain.StatusRunning || afterWorker.State.Lease.Owner != "lifecycle-original-0" || afterWorker.State.PendingEffect == nil || afterWorker.State.PendingEffect.ID != original.Request.OperationID {
		t.Fatal("worker SIGTERM mutated business state")
	}
	workerView := lifecycleObserve(h, c, original, "after-worker-sigterm")
	if !workerView.State.Running || workerView.ID != container.ID {
		t.Fatal("worker SIGTERM stopped or replaced original container")
	}
	lifecycleCgroup(h, c, container.ID, cgroupPath, "after-worker-sigterm", true)
	lifecycleAssertReserved(h)
	beforeRunner := lifecycleReadOperation(h, h.s.TargetOp)
	if beforeRunner.Status != runner.Running || beforeRunner.CancelRequested {
		t.Fatal("worker SIGTERM cancelled durable operation")
	}
	runnerSignal := sigtermExit(h, originalRunner, "runner-original", a.RunnerSHA256)
	shutdownView := lifecycleObserve(h, c, original, "after-runner-sigterm")
	if !shutdownView.State.Running || shutdownView.ID != container.ID {
		t.Fatal("runner SIGTERM stopped or replaced original container")
	}
	lifecycleCgroup(h, c, container.ID, cgroupPath, "after-runner-sigterm", true)
	unknown := lifecycleReadOperation(h, h.s.TargetOp)
	if unknown.Status != runner.Unknown || unknown.CancelRequested {
		t.Fatal("runner shutdown did not retain non-cancelled unknown")
	}
	gap := lifecycleSpool(h, "runner-shutdown")
	if gap.Complete || gap.DroppedKnown || !gap.Truncated || gap.TerminationRequested || gap.TerminationObserved || gap.Reason != "runner_shutdown" {
		t.Fatal("shutdown gap was misrepresented as complete or policy cancellation", gap)
	}
	recovery["phase"] = "runner_shutdown_gap"
	h.fatal(save(filepath.Join(dir, "recovery.json"), recovery))
	h.capture("after-both-signals")
	lifecycleJournal(h, "after-both-signals")
	lifecycleAssertReserved(h)
	// The original process has exited and released its SQLite/flock authority.
	// No Start is sent during inspection; same old epoch proof remains valid.
	successorRunner := lifecycleStartRunner(h, a, c, "runner-successor")
	var sameUUID string
	h.fatal(h.journal.QueryRow(`SELECT id FROM journal_identity WHERE singleton=1`).Scan(&sameUUID))
	if sameUUID != uuid {
		t.Fatal("journal identity changed")
	}
	inspected, err := sigtermInspect(h, h.client, signer, afterWorker, h.s.TargetOp)
	h.fatal(err)
	if inspected.Status != runner.Unknown || inspected.Request.Epoch != original.Request.Epoch || inspected.JobID != original.JobID || inspected.CancelRequested {
		t.Fatal("same epoch successor inspection restarted/settled unknown operation")
	}
	h.fatal(save(filepath.Join(dir, "successor-same-epoch-inspection.json"), inspected))
	successorView := lifecycleObserve(h, c, original, "after-runner-successor-inspect")
	if !successorView.State.Running || successorView.ID != container.ID {
		t.Fatal("inspection stopped/replaced original writer")
	}
	lifecycleCgroup(h, c, container.ID, cgroupPath, "after-runner-successor-inspect", true)
	successorWorker := sigtermLaunch(h, a.WorkerBinary, configFile, "lifecycle-successor")
	var claimTime time.Time
	h.wait(func() bool {
		var raw string
		err := h.db.Pool.QueryRow(ctx, `SELECT input_event->>'at' FROM run_snapshots WHERE tenant_id=$1 AND run_id=$2 AND input_event->>'kind'='claimed' AND input_event->'lease'->>'owner'='lifecycle-successor-0' ORDER BY version LIMIT 1`, tenant, run.ID).Scan(&raw)
		if errors.Is(err, pgx.ErrNoRows) {
			return false
		}
		h.fatal(err)
		claimTime, err = time.Parse(time.RFC3339Nano, raw)
		h.fatal(err)
		if claimTime.Before(afterWorker.State.Lease.Until) {
			t.Fatal("successor bypassed natural original lease expiry")
		}
		return true
	}, 45*time.Second, "natural 30 second lease expiry and successor claim")
	// Adoption is allowed to stop the old writer. The second literal command
	// remains deliberately unapproved, preventing any successful repair claim.
	h.wait(func() bool { return h.getRun().State.Status == domain.StatusWaitingApproval }, 20*time.Second, "adoption/old receipt settlement then next tool approval")
	adopted := h.getRun()
	if adopted.State.Approval == nil || adopted.State.Approval.EffectID == h.s.TargetOp || adopted.State.Lease.Epoch <= original.Request.Epoch {
		t.Fatal("successor did not consume original receipt under new fencing")
	}
	settled := lifecycleReadOperation(h, h.s.TargetOp)
	boundBefore, err := json.Marshal(original.Request)
	h.fatal(err)
	boundAfter, err := json.Marshal(settled.Request)
	h.fatal(err)
	if !settled.Status.Terminal() || settled.Status == runner.Succeeded || settled.JobID != original.JobID || !bytes.Equal(boundBefore, boundAfter) {
		t.Fatal("original immutable operation/gap outcome changed")
	}
	var job sandbox.Job
	h.fatal(json.Unmarshal(settled.Result, &job))
	if job.Log == nil || sandbox.VerificationLogValid(job) || job.Log.Complete || job.Log.Reason != "runner_shutdown" || job.Log.Artifact.ObjectKey == "" {
		t.Fatal("captured gap became trusted or lost its artifact")
	}
	stopped := lifecycleObserve(h, c, original, "after-successor-adoption")
	if stopped.State.Running || stopped.ID != container.ID {
		t.Fatal("adoption did not confirm original writer stopped")
	}
	lifecycleCgroup(h, c, container.ID, cgroupPath, "after-successor-adoption", false)
	var publishedLogCount int
	h.fatal(h.db.Pool.QueryRow(ctx, `SELECT count(*) FROM artifacts WHERE tenant_id=$1 AND run_id=$2 AND state='ready' AND kind='operation_log' AND object_key=$3 AND sha256=$4 AND byte_size=$5`, tenant, run.ID, job.Log.Artifact.ObjectKey, job.Log.Artifact.SHA256, job.Log.Artifact.Size).Scan(&publishedLogCount))
	if publishedLogCount != 1 {
		t.Fatal("retained gap log not published exactly once to PG READY")
	}
	logPath := filepath.Join(c.ArtifactRoot, job.Log.Artifact.ObjectKey)
	logDigest, err := sigtermDigest(logPath)
	h.fatal(err)
	logStat, err := os.Stat(logPath)
	h.fatal(err)
	if logDigest != job.Log.Artifact.SHA256 || logStat.Size() != job.Log.Artifact.Size || logStat.Size() != job.Log.RetainedBytes {
		t.Fatal("published log hash/length mismatch")
	}
	containerStarted, err := time.Parse(time.RFC3339Nano, container.State.StartedAt)
	h.fatal(err)
	containerFinished, err := time.Parse(time.RFC3339Nano, stopped.State.FinishedAt)
	h.fatal(err)
	if containerFinished.Before(claimTime) || containerFinished.Sub(containerStarted) >= 65*time.Second {
		t.Fatal("writer stop cannot be attributed to adoption before70 second natural bound")
	}
	base := lifecycleBase(h)
	counter, err := os.ReadFile(filepath.Join(base, "checkout", "lifecycle-counter.txt"))
	h.fatal(err)
	if string(counter) != "1\n" {
		t.Fatal("original side effect executed more than once")
	}
	writes, err := os.ReadFile(filepath.Join(base, "checkout", "lifecycle-writer.txt"))
	h.fatal(err)
	h.fatal(os.WriteFile(filepath.Join(dir, "writer-timestamps.txt"), writes, 0600))
	h.fatal(os.WriteFile(filepath.Join(dir, "start-counter.txt"), counter, 0600))
	lifecycleSpool(h, "after-adoption")
	h.fatal(save(filepath.Join(dir, "original-operation-settled.json"), settled))
	h.capture("after-adoption")
	lifecycleJournal(h, "after-adoption")
	recovery["phase"] = "adoption_settled_waiting_approval"
	h.fatal(save(filepath.Join(dir, "recovery.json"), recovery))
	var preCancel int
	h.fatal(h.db.Pool.QueryRow(ctx, `SELECT count(*) FROM run_snapshots WHERE tenant_id=$1 AND run_id=$2 AND input_event->>'kind'='cancel_requested'`, tenant, run.ID).Scan(&preCancel))
	if preCancel != 0 {
		t.Fatal("process signals issued business cancellation")
	}
	_, err = h.db.Cancel(ctx, persistence.Identity{TenantID: tenant, PrincipalID: "fixture-operator", Role: "admin"}, run.ID)
	h.fatal(err)
	h.wait(func() bool { return h.getRun().State.Status.Terminal() }, 20*time.Second, "explicit business cancel and confirmed stop")
	final := h.getRun()
	if final.State.Status != domain.StatusCancelled || final.State.Verification == domain.VerificationVerified || final.State.Verification == domain.VerificationRegressionOnly {
		t.Fatal("fixture falsely claimed successful trusted verification", final.State)
	}
	sigtermExit(h, successorWorker, "worker-successor", a.WorkerSHA256)
	proof := lifecycleLedgers(h, original, afterWorker, claimTime, workerSignal)
	proof["worker_signal"], proof["runner_signal"] = workerSignal, runnerSignal
	proof["original_container_id"], proof["original_job_id"] = container.ID, original.JobID
	proof["journal_uuid"] = uuid
	proof["original_epoch"], proof["successor_epoch"] = original.Request.Epoch, adopted.State.Lease.Epoch
	proof["operation_outcome"], proof["log_gap"] = settled.Status, job.Log
	proof["writer_interval"] = map[string]any{"docker_started_at": containerStarted, "successor_claim_db_time": claimTime, "docker_finished_at": containerFinished, "natural_program_seconds": 70, "finished_before_natural_bound": true}
	proof["docker_events"] = lifecycleEvents(h, c, container.ID, original.JobID, time.Now().Add(-3*time.Minute))
	h.capture("before-cleanup")
	lifecycleJournal(h, "before-cleanup")
	// Production cleanup publishes a sealed code snapshot before release and
	// removes only owned strict-log containers. It never rebinds the pool owner.
	d, closeDriver, err := driver(ctx, h.s, h.c, h.workerDSN, h.client)
	h.fatal(err)
	cleanup, err := d.CleanupWorkspace(ctx, tenant, run.ID, "lifecycle-explicit-cleanup", 0)
	closeDriver()
	h.fatal(err)
	if cleanup.Phase != "released" || cleanup.SnapshotRef == "" {
		t.Fatal("cleanup did not commit snapshot then release")
	}
	proof["cleanup"] = cleanup
	var retained, unsettled int
	h.fatal(h.journal.QueryRow(`SELECT count(*) FROM volume_leases WHERE released=0`).Scan(&retained))
	h.fatal(h.journal.QueryRow(`SELECT count(*) FROM operations WHERE status IN('prepared','running','unknown')`).Scan(&unsettled))
	if retained != 0 || unsettled != 0 {
		t.Fatal("fixture left unsettled capacity", retained, unsettled)
	}
	var logOps, reservedLogBytes, removedLogs int
	h.fatal(h.journal.QueryRow(`SELECT operations,reserved_bytes FROM log_runs WHERE tenant_id=? AND run_id=?`, tenant, run.ID).Scan(&logOps, &reservedLogBytes))
	h.fatal(h.journal.QueryRow(`SELECT count(*) FROM operation_logs WHERE cleanup_state='removed'`).Scan(&removedLogs))
	if logOps != 2 || reservedLogBytes != 2*(512<<10) || removedLogs != 2 {
		t.Fatal("shutdown/recovery duplicated log budget or cleanup was not durable", logOps, reservedLogBytes, removedLogs)
	}
	remaining, err := lifecycleDocker(ctx, c, "ps", "--all", "--no-trunc", "--filter", "label=forge.operation_id="+string(h.s.TargetOp), "--format", "{{.ID}}")
	h.fatal(err)
	if len(strings.TrimSpace(string(remaining))) != 0 {
		t.Fatal("own operation container remains after production release")
	}
	h.fatal(os.WriteFile(filepath.Join(dir, "container-after-release.txt"), remaining, 0600))
	proof["log_reservations"] = map[string]int{"operations": logOps, "reserved_bytes": reservedLogBytes, "removed_containers": removedLogs}
	h.capture("cleanup-released")
	lifecycleJournal(h, "cleanup-released")
	h.archiveArtifacts()
	sigtermExit(h, successorRunner, "runner-successor", a.RunnerSHA256)
	for _, s := range c.VolumeSlots {
		owner, err := os.ReadFile(filepath.Join(s.MountPath, ".forge-pool.owner"))
		h.fatal(err)
		if string(owner) != c.JournalPath+"\n" {
			t.Fatal("volume owner changed")
		}
	}
	lastHash, err := sigtermDigest(a.RunnerConfig)
	h.fatal(err)
	if lastHash != runnerConfigHash {
		t.Fatal("operator config was changed")
	}
	recovery["phase"] = "released"
	proof["scope"] = "actual production worker and runner OS SIGTERM; private PostgreSQL; production Docker.Start and fixed ext4 volume, SQLite v5 and physical strict spool; synthetic model/fees"
	proof["capture"] = "PostgreSQL/SQLite/Docker observations are sequential, not an atomic cross-system snapshot"
	proof["passed"] = true
	h.fatal(save(filepath.Join(dir, "acceptance.json"), proof))
	t.Logf("full worker+runner SIGTERM evidence: %s", dir)
}

func lifecycleAssertReserved(h *harness) {
	var active, slots, allocations int
	h.fatal(h.db.Pool.QueryRow(h.ctx, `SELECT active_count FROM tenant_runtime WHERE tenant_id=$1`, h.s.Tenant).Scan(&active))
	h.fatal(h.db.Pool.QueryRow(h.ctx, `SELECT reserved_slots FROM runners WHERE id='application-fault-runner'`).Scan(&slots))
	h.fatal(h.db.Pool.QueryRow(h.ctx, `SELECT count(*) FROM runner_allocations WHERE tenant_id=$1 AND run_id=$2 AND state='reserved'`, h.s.Tenant, h.s.RunID).Scan(&allocations))
	if active != 1 || slots != 1 || allocations != 1 {
		h.t.Fatal("unsettled signal boundary released capacity", active, slots, allocations)
	}
}
func lifecycleLedgers(h *harness, original runner.Operation, atExit persistence.Run, claimed time.Time, signal sigtermBoundary) map[string]any {
	var cancelEvents, active, slots, requests, attempts, reservations, settled, targetRows, confirmations, wrongClaims int
	var actual, committed, reservedCost, reservedTokens int64
	h.fatal(h.db.Pool.QueryRow(h.ctx, `SELECT count(*) FROM run_snapshots WHERE tenant_id=$1 AND run_id=$2 AND input_event->>'kind'='cancel_requested'`, h.s.Tenant, h.s.RunID).Scan(&cancelEvents))
	h.fatal(h.db.Pool.QueryRow(h.ctx, `SELECT active_count FROM tenant_runtime WHERE tenant_id=$1`, h.s.Tenant).Scan(&active))
	h.fatal(h.db.Pool.QueryRow(h.ctx, `SELECT reserved_slots FROM runners WHERE id='application-fault-runner'`).Scan(&slots))
	h.fatal(h.db.Pool.QueryRow(h.ctx, `SELECT active_requests,reserved_microusd,reserved_tokens,committed_microusd FROM provider_quotas WHERE credential_group='application-faults-fake'`).Scan(&requests, &reservedCost, &reservedTokens, &committed))
	h.fatal(h.db.Pool.QueryRow(h.ctx, `SELECT count(*) FROM model_attempts WHERE tenant_id=$1 AND run_id=$2 AND status='completed'`, h.s.Tenant, h.s.RunID).Scan(&attempts))
	h.fatal(h.db.Pool.QueryRow(h.ctx, `SELECT count(*),count(*) FILTER(WHERE status='settled' AND request_slot_released),coalesce(sum(actual_microusd),0)::bigint FROM quota_reservations WHERE tenant_id=$1 AND run_id=$2`, h.s.Tenant, h.s.RunID).Scan(&reservations, &settled, &actual))
	h.fatal(h.db.Pool.QueryRow(h.ctx, `SELECT count(*) FROM effects WHERE tenant_id=$1 AND run_id=$2 AND operation_id=$3 AND epoch=$4 AND status IN('failed','cancelled') AND receipt_ref IS NOT NULL`, h.s.Tenant, h.s.RunID, h.s.TargetOp, original.Request.Epoch).Scan(&targetRows))
	h.fatal(h.db.Pool.QueryRow(h.ctx, `SELECT count(*) FROM run_snapshots WHERE tenant_id=$1 AND run_id=$2 AND input_event->>'kind' IN ('effect_completed','reconciled') AND input_event->'receipt'->>'effect_id'=$3`, h.s.Tenant, h.s.RunID, h.s.TargetOp).Scan(&confirmations))
	h.fatal(h.db.Pool.QueryRow(h.ctx, `SELECT count(*) FROM run_snapshots WHERE tenant_id=$1 AND run_id=$2 AND input_event->>'kind'='claimed' AND (extract(epoch FROM ((input_event->'lease'->>'until')::timestamptz-(input_event->>'at')::timestamptz))<>30 OR (input_event->'lease'->>'owner'='lifecycle-original-0' AND (input_event->>'at')::timestamptz>$3))`, h.s.Tenant, h.s.RunID, signal.DBAfter).Scan(&wrongClaims))
	if cancelEvents != 1 || active != 0 || slots != 0 || requests != 0 || reservedCost != 0 || reservedTokens != 0 || attempts != 2 || reservations != 2 || settled != 2 || actual != 300 || committed != 300 || targetRows != 1 || confirmations != 1 || wrongClaims != 0 {
		h.t.Fatalf("terminal ledger mismatch cancel=%d capacity=%d/%d request=%d reserved=%d/%d models=%d reservations=%d/%d cost=%d/%d target=%d confirmations=%d wrongclaims=%d", cancelEvents, active, slots, requests, reservedCost, reservedTokens, attempts, reservations, settled, actual, committed, targetRows, confirmations, wrongClaims)
	}
	_, err := h.db.Heartbeat(h.ctx, h.s.Tenant, h.s.RunID, atExit.State.Lease.Owner, original.Request.Epoch, 30*time.Second)
	if !errors.Is(err, domain.ErrFenced) {
		h.t.Fatal("old epoch heartbeat was not fenced", err)
	}
	return map[string]any{"cancel_events": cancelEvents, "tenant_active": active, "runner_reserved": slots, "provider_active_requests": requests, "synthetic_microusd": actual, "model_attempts": attempts, "settled_reservations": settled, "original_effect_confirmations": confirmations, "old_lease_until": atExit.State.Lease.Until, "successor_claim_db_time": claimed, "old_epoch_heartbeat_fenced": true, "final_state": h.getRun().State}
}
func lifecycleEvents(h *harness, c lifecycleRunnerConfig, id, name string, since time.Time) map[string]any {
	raw, err := lifecycleDocker(h.ctx, c, "events", "--since", since.Format(time.RFC3339Nano), "--until", time.Now().Format(time.RFC3339Nano), "--filter", "container="+id, "--format", "{{json .}}")
	h.fatal(err)
	h.fatal(os.WriteFile(filepath.Join(h.dir, "original-container-events.jsonl"), raw, 0600))
	creates, starts, dies := 0, 0, 0
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		if line == "" {
			continue
		}
		var e struct {
			Action string
			Actor  struct {
				ID         string
				Attributes map[string]string
			}
		}
		h.fatal(json.Unmarshal([]byte(line), &e))
		if e.Actor.ID != id || e.Actor.Attributes["name"] != name {
			h.t.Fatal("foreign Docker event")
		}
		switch e.Action {
		case "create":
			creates++
		case "start":
			starts++
		case "die":
			dies++
		}
	}
	if creates != 1 || starts != 1 || dies != 1 {
		h.t.Fatal("original Docker execution repeated or not stopped", creates, starts, dies)
	}
	return map[string]any{"container_id": id, "creates": creates, "starts": starts, "dies": dies, "bound": fmt.Sprintf("read-only daemon events since %s", since.Format(time.RFC3339Nano))}
}

// Recovery is a separate explicit operator action. It never reruns the first
// case's fresh-state setup or submits a replacement run/operation. The private
// restricted credential created by that case is sufficient; no admin needed.
func TestRecoverWorkerRunnerSIGTERM(t *testing.T) {
	if os.Getenv("FORGE_RECOVER_WORKER_RUNNER_SIGTERM") != "1" {
		t.Skip("operator-only recovery of retained lifecycle fixture")
	}
	var a lifecycleAcceptance
	var c lifecycleRunnerConfig
	if err := readJSON(os.Getenv("FORGE_WORKER_RUNNER_SIGTERM_ACCEPTANCE"), &a); err != nil {
		t.Fatal(err)
	}
	if err := readJSON(a.RunnerConfig, &c); err != nil {
		t.Fatal(err)
	}
	if err := lifecycleShape(a, c); err != nil {
		t.Fatal(err)
	}
	var retained struct {
		Schema           string    `json:"schema"`
		Tenant           domain.ID `json:"tenant_id"`
		Run              domain.ID `json:"run_id"`
		Operation        domain.ID `json:"operation_id"`
		UUID             string    `json:"journal_uuid"`
		ConfigHash       string    `json:"runner_config_sha256"`
		WorkerConfigHash string    `json:"worker_config_sha256"`
		Phase            string    `json:"phase"`
	}
	caseDir := filepath.Join(a.EvidenceDir, "worker-runner-sigterm")
	if err := readJSON(filepath.Join(caseDir, "recovery.json"), &retained); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(retained.Schema, "appfault_lifecycle_") || retained.Tenant.Validate() != nil || retained.Run.Validate() != nil || retained.Operation != domain.ID(string(retained.Run)+"_step_1_op_0") || len(retained.UUID) != 64 {
		t.Fatal("incomplete/foreign recovery identity; no mutation attempted")
	}
	if retained.Phase == "released" {
		t.Fatal("already released fixture must not be reexecuted")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	for _, path := range []string{a.RunnerConfig, a.WorkerBinary, a.RunnerBinary, a.TestBinary, c.RootDir, c.JournalPath, c.ArtifactRoot, c.SigningKeyFile, c.Server.UnixSocket} {
		if err := lifecyclePath(path); err != nil {
			t.Fatal(err)
		}
	}
	for path, want := range map[string]string{a.RunnerConfig: retained.ConfigHash, a.WorkerBinary: a.WorkerSHA256, a.RunnerBinary: a.RunnerSHA256, a.TestBinary: a.TestSHA256} {
		actual, err := sigtermDigest(path)
		if err != nil || actual != want {
			t.Fatal("recovery source/executable changed", path)
		}
	}
	self, err := os.Executable()
	if err != nil || self != a.TestBinary {
		t.Fatal("wrong recovery test executable")
	}
	uid, err := os.ReadFile("/proc/self/uid_map")
	gid, gidErr := os.ReadFile("/proc/self/gid_map")
	if err != nil || gidErr != nil || os.Geteuid() != 0 || !lifecycleMapping(uid) || !lifecycleMapping(gid) {
		t.Fatal("mapped root lifecycle launch required")
	}
	if _, err := os.Lstat(c.Server.UnixSocket); !os.IsNotExist(err) {
		t.Fatal("existing runner socket requires identity review; never stop a possibly live owner")
	}
	for _, s := range c.VolumeSlots {
		if err := sandbox.VerifyVolume(ctx, s); err != nil {
			t.Fatal(err)
		}
		owner, err := os.ReadFile(filepath.Join(s.MountPath, ".forge-pool.owner"))
		if err != nil || string(owner) != c.JournalPath+"\n" {
			t.Fatal("pool owner differs; recovery cannot rebind")
		}
	}
	private := filepath.Join(a.ScopeRoot, "runtime", "sigterm-private")
	for _, path := range []string{private, filepath.Join(private, "database.json"), filepath.Join(private, "worker.json")} {
		if err := lifecyclePath(path); err != nil {
			t.Fatal(err)
		}
	}
	workerConfigHash, err := sigtermDigest(filepath.Join(private, "worker.json"))
	if err != nil || workerConfigHash != retained.WorkerConfigHash {
		t.Fatal("private worker configuration changed")
	}
	var dbPrivate struct {
		DSN    string `json:"worker_dsn"`
		Schema string `json:"schema"`
	}
	if err := readJSON(filepath.Join(private, "database.json"), &dbPrivate); err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(dbPrivate.DSN)
	if err != nil || u.Hostname() != "127.0.0.1" || u.Port() != "32773" || u.Path != "/forge" || u.Query().Get("search_path") != retained.Schema || dbPrivate.Schema != retained.Schema {
		t.Fatal("foreign private database binding")
	}
	dir := filepath.Join(a.EvidenceDir, "recovery-"+time.Now().UTC().Format("20060102T150405.000000000Z"))
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	h := &harness{t: t, ctx: ctx, c: c.runnerSettings, dir: dir, private: private, s: settings{Tenant: retained.Tenant, RunID: retained.Run, TargetOp: retained.Operation, Socket: c.Server.UnixSocket}, schema: retained.Schema, workerDSN: dbPrivate.DSN}
	h.db, err = persistence.Open(ctx, dbPrivate.DSN)
	h.fatal(err)
	defer h.db.Close()
	h.fatal(h.db.CheckWorkerRole(ctx))
	defer h.stopAll()
	lifecycleOpenJournal(h)
	defer h.journal.Close()
	var uuid string
	h.fatal(h.journal.QueryRow(`SELECT id FROM journal_identity WHERE singleton=1`).Scan(&uuid))
	if uuid != retained.UUID {
		t.Fatal("replacement journal cannot recover old fixture")
	}
	var foreign int
	h.fatal(h.journal.QueryRow(`SELECT count(*) FROM workspaces WHERE tenant_id<>? OR run_id<>?`, retained.Tenant, retained.Run).Scan(&foreign))
	if foreign != 0 {
		t.Fatal("another fixture has since used this journal; review rather than auto-recover")
	}
	r := h.getRun()
	if r.State.Status.Terminal() && r.State.Status != domain.StatusCancelled && r.State.Status != domain.StatusFailed {
		t.Fatal("unexpected terminal state requires review")
	}
	h.capture("before-recovery")
	lifecycleJournal(h, "before-recovery")
	runnerProcess := lifecycleStartRunner(h, a, c, "runner-recovery")
	if !r.State.Status.Terminal() {
		_, err = h.db.Cancel(ctx, persistence.Identity{TenantID: retained.Tenant, PrincipalID: "fixture-operator", Role: "admin"}, retained.Run)
		h.fatal(err)
		worker := sigtermLaunch(h, a.WorkerBinary, filepath.Join(private, "worker.json"), "lifecycle-recovery")
		h.wait(func() bool { return h.getRun().State.Status.Terminal() }, 75*time.Second, "original lease expiry and explicit business cancellation")
		sigtermExit(h, worker, "worker-recovery", a.WorkerSHA256)
	}
	d, closeDriver, err := driver(ctx, h.s, h.c, h.workerDSN, h.client)
	h.fatal(err)
	result, err := d.CleanupWorkspace(ctx, retained.Tenant, retained.Run, "lifecycle-retained-recovery", 0)
	closeDriver()
	h.fatal(err)
	if result.Phase != "released" || result.SnapshotRef == "" {
		t.Fatal("original workspace remains unresolved; retained")
	}
	h.capture("recovery-released")
	lifecycleJournal(h, "recovery-released")
	h.archiveArtifacts()
	sigtermExit(h, runnerProcess, "runner-recovery", a.RunnerSHA256)
	h.fatal(save(filepath.Join(dir, "recovery-result.json"), map[string]any{"run_id": retained.Run, "original_operation_id": retained.Operation, "journal_uuid": uuid, "cleanup": result, "passed": true, "scope": "explicit recovery/cleanup; not an acceptance rerun; original failure remains"}))
}
