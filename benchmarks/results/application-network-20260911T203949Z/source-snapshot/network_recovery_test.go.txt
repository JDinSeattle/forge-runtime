package applicationfaults

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
)

// Kept separate from the frozen E25 recovery implementation. This is explicit
// same-run cancellation/reconciliation, never acceptance replay or new submission.
func TestRecoverApplicationNetwork(t *testing.T) {
	path := os.Getenv("FORGE_APP_NETWORK_RECOVER")
	if path == "" {
		t.Skip("explicit original network case recovery-descriptor.json required")
	}
	config, binary, output, u, c := continuationInputs(t)
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 1<<20 {
		t.Fatal("private regular recovery descriptor <=1MiB required")
	}
	var descriptor continuationDescriptor
	if err = readJSON(path, &descriptor); err != nil {
		t.Fatal(err)
	}
	if descriptor.Version != 1 || !networkMode(descriptor.Settings.Mode) || !strings.HasPrefix(descriptor.Schema, "appfault_") || descriptor.Settings.Tenant.Validate() != nil || descriptor.Settings.RunID.Validate() != nil || descriptor.Settings.RunnerConfig != config {
		t.Fatal("exact network-matrix descriptor required")
	}
	raw, err := os.ReadFile(config)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	if hex.EncodeToString(sum[:]) != descriptor.ConfigSHA256 {
		t.Fatal("runner config changed; keep original fixture retained for review")
	}
	fixed, err := scripts("F05", c.Sources["clamp"])
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(fixed, descriptor.Settings.Scripts) {
		t.Fatal("retained scripts differ from fixed trusted fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	defer cancel()
	h := &networkHarness{harness: &harness{t: t, ctx: ctx, c: c, binary: binary, dir: filepath.Join(output, "recovery"), schema: descriptor.Schema, s: descriptor.Settings}}
	h.fatal(os.Mkdir(h.dir, 0700))
	defer h.closeNetwork()
	scoped := *u
	q := scoped.Query()
	q.Set("search_path", h.schema)
	scoped.RawQuery = q.Encode()
	h.dsn = scoped.String()
	h.db, err = persistence.Open(ctx, h.dsn)
	h.fatal(err)
	defer h.db.Close()
	var runCount int
	h.fatal(h.db.Pool.QueryRow(ctx, `SELECT count(*) FROM runs`).Scan(&runCount))
	r := h.getRun()
	validPlacement := r.RunnerID == "application-fault-runner" || (r.RunnerID == "" && r.State.Lease.Epoch == 0 && r.State.WorkspaceRevision == 0)
	if runCount != 1 || r.BaseCommit != h.s.SourceHash || !validPlacement {
		t.Fatal("recovery did not select the exact one-run fixture schema")
	}
	h.openContinuationJournal()
	defer h.journal.Close()
	var identity string
	h.fatal(h.journal.QueryRow(`SELECT id FROM journal_identity WHERE singleton=1`).Scan(&identity))
	if identity != descriptor.JournalIdentity {
		t.Fatal("authoritative journal replaced; retained fixture cannot be guessed")
	}
	h.workerDSN, h.workerRole, err = restrictedWorker(ctx, h.db, scoped, h.schema)
	h.fatal(err)
	role, err := preflightWorkerRole(ctx, h.workerDSN, h.workerRole, h.schema)
	h.fatal(err)
	h.fatal(save(filepath.Join(h.dir, "worker-role-preflight.json"), role))
	h.s.Mode = "network_recovery"
	h.privateContinuationConfig(config)
	h.capture("retained-before-recovery")
	h.runnerStart("")
	if !r.State.Status.Terminal() {
		_, err = h.db.Cancel(ctx, persistence.Identity{TenantID: h.s.Tenant, PrincipalID: "fixture-operator", Role: "admin"}, h.s.RunID)
		h.fatal(err)
		worker := h.workerNetwork("worker2")
		h.wait(func() bool { return h.getRun().State.Status.Terminal() }, 70*time.Second, "original run cancellation and actual effect settlement")
		h.stop(worker)
	}
	final := h.getRun()
	var unsettled, allocations, tenantActive, runnerSlots int
	h.fatal(h.db.Pool.QueryRow(ctx, `SELECT count(*) FROM effects WHERE tenant_id=$1 AND run_id=$2 AND status IN ('in_flight','unknown')`, h.s.Tenant, h.s.RunID).Scan(&unsettled))
	h.fatal(h.db.Pool.QueryRow(ctx, `SELECT count(*) FROM runner_allocations WHERE tenant_id=$1 AND run_id=$2 AND state!='released'`, h.s.Tenant, h.s.RunID).Scan(&allocations))
	h.fatal(h.db.Pool.QueryRow(ctx, `SELECT active_count FROM tenant_runtime WHERE tenant_id=$1`, h.s.Tenant).Scan(&tenantActive))
	h.fatal(h.db.Pool.QueryRow(ctx, `SELECT reserved_slots FROM runners WHERE id='application-fault-runner'`).Scan(&runnerSlots))
	if unsettled != 0 || allocations != 0 || tenantActive != 0 || runnerSlots != 0 {
		h.capture("recovery-retained-uncertainty")
		t.Fatal("unsettled effects/capacity remain: journal, artifacts and volume retained for operator reconciliation")
	}
	proof := map[string]any{"recovery_only": true, "original_acceptance_passed": false, "source_descriptor": path, "schema": h.schema, "run_id": h.s.RunID, "terminal_status": final.State.Status, "replacement_run_submitted": false, "unknown_effects": unsettled, "unreleased_allocations": allocations}
	if final.State.WorkspaceRevision == 0 {
		for _, table := range []string{"workspaces", "volume_leases"} {
			column := "workspace_id"
			if table == "workspaces" {
				column = "id"
			}
			var count int
			h.fatal(h.journal.QueryRow("SELECT count(*) FROM "+table+" WHERE "+column+"=?", h.s.RunID).Scan(&count))
			if count != 0 {
				t.Fatal("local workspace exists with no settled revision; retaining it")
			}
		}
		proof["workspace_cleanup"] = "not applicable: no workspace or volume lease exists"
	} else {
		d, closeDriver, err := driver(ctx, h.s, h.c, h.workerDSN, h.client)
		h.fatal(err)
		defer closeDriver()
		var cleanup persistence.WorkspaceCleanup
		until := time.Now().Add(35 * time.Second)
		for {
			cleanup, err = d.CleanupWorkspace(ctx, h.s.Tenant, h.s.RunID, "network-recovery", 0)
			if !errors.Is(err, domain.ErrCapacity) || time.Now().After(until) {
				break
			}
			time.Sleep(250 * time.Millisecond)
		}
		h.fatal(err)
		if cleanup.Phase != "released" || cleanup.SnapshotRef == "" {
			t.Fatal("snapshot publication/release not proven")
		}
		proof["cleanup"] = cleanup
	}
	h.capture("recovery-completed")
	h.archiveArtifacts()
	h.fatal(save(filepath.Join(h.dir, "recovery.json"), proof))
}
