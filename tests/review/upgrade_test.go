package review_test

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	"github.com/JDinSeattle/forge-runtime/db"
	"github.com/JDinSeattle/forge-runtime/internal/artifact"
	"github.com/JDinSeattle/forge-runtime/internal/runner"
	"github.com/JDinSeattle/forge-runtime/internal/sandbox"
	"github.com/jackc/pgx/v5"
	"github.com/pressly/goose/v3"
)

// The old database is synthesized from the actual immutable migration files,
// with committed data. No downgrade is ever run against a deployed database.
func TestRecoveryPostgresAdditiveUpgrade(t *testing.T) {
	ctx, admin := reviewDB(t)
	schema := "upgrade_" + strings.ToLower(rand.Text())
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+quoted); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := admin.Exec(cleanup, "DROP SCHEMA "+quoted+" CASCADE"); err != nil {
			t.Error(err)
		}
	})
	u, err := url.Parse(os.Getenv("FORGE_REVIEW_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	oldFiles := fstest.MapFS{}
	entries, err := fs.ReadDir(db.Migrations, "migrations")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Name() < "00007" {
			raw, err := db.Migrations.ReadFile("migrations/" + entry.Name())
			if err != nil {
				t.Fatal(err)
			}
			oldFiles[entry.Name()] = &fstest.MapFile{Data: raw}
		}
	}
	connection, err := sql.Open("pgx", u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	provider, err := goose.NewProvider(goose.DialectPostgres, connection, oldFiles)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = provider.Up(ctx); err != nil {
		t.Fatal(err)
	}
	legacy, err := pgx.Connect(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer legacy.Close(context.Background())
	tx, err := legacy.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	f := seed(t, ctx, tx)
	exec(t, ctx, tx, `UPDATE runs SET state='needs_reconciliation',snapshot=$1 WHERE tenant_id=$2 AND id='run_a'`, `{"schema_version":1,"unresolved":"do not replay","number":9007199254740993,"html":"<>&"}`, f.tenantA)
	exec(t, ctx, tx, `UPDATE effects SET status='unknown' WHERE tenant_id=$1 AND operation_id='operation_a'`, f.tenantA)
	exec(t, ctx, tx, `INSERT INTO run_snapshots(tenant_id,run_id,version,step_seq,schema_version,body) SELECT tenant_id,id,1,1,1,snapshot FROM runs WHERE tenant_id=$1 AND id='run_a'`, f.tenantA)
	exec(t, ctx, tx, `INSERT INTO artifacts(tenant_id,id,run_id,kind,object_key,sha256,byte_size,state) VALUES($1,'legacy_artifact','run_a','operation_receipt','legacy-key','legacy-sha',3,'ready')`, f.tenantA)
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	capture := func() map[string]string {
		result := map[string]string{}
		for name, query := range map[string]string{
			// Compare all pre-upgrade columns; migration 9's additive priority
			// is checked independently below rather than silently ignored.
			"runs":           `SELECT json_agg(to_jsonb(x)-ARRAY['priority','snapshot_hold_at','snapshot_hold_reason','snapshot_hold_schema','snapshot_hold_sha256','runnable_at','last_claim_traceparent','lease_yielded'] ORDER BY x.id)::text FROM runs x`,
			"effects":        `SELECT json_agg(row_to_json(x) ORDER BY x.operation_id)::text FROM effects x`,
			"artifacts":      `SELECT json_agg(row_to_json(x) ORDER BY x.id)::text FROM artifacts x`,
			"snapshot_bytes": `SELECT body::text FROM run_snapshots WHERE tenant_id=$1 AND run_id='run_a'`,
		} {
			var err error
			if name == "snapshot_bytes" {
				err = legacy.QueryRow(ctx, query, f.tenantA).Scan(&resultValue{values: result, key: name})
			} else {
				err = legacy.QueryRow(ctx, query).Scan(&resultValue{values: result, key: name})
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		return result
	}
	before := capture()
	for range 2 {
		if err = db.Migrate(ctx, u.String()); err != nil {
			t.Fatal(err)
		}
	}
	after := capture()
	for key, value := range before {
		if value != after[key] {
			t.Fatalf("additive migration changed %s", key)
		}
	}
	var nullInputs, cleanupRLS bool
	if err = legacy.QueryRow(ctx, `SELECT input_state IS NULL AND input_event IS NULL FROM run_snapshots WHERE tenant_id=$1 AND run_id='run_a'`, f.tenantA).Scan(&nullInputs); err != nil || !nullInputs {
		t.Fatalf("legacy audit falsely marked replayable: %v", err)
	}
	if err = legacy.QueryRow(ctx, `SELECT relrowsecurity FROM pg_class WHERE oid='workspace_cleanup'::regclass`).Scan(&cleanupRLS); err != nil || !cleanupRLS {
		t.Fatalf("cleanup table missing RLS: %v", err)
	}
	var defaultPriorities bool
	if err = legacy.QueryRow(ctx, `SELECT count(*)>0 AND bool_and(priority=0) FROM runs`).Scan(&defaultPriorities); err != nil || !defaultPriorities {
		t.Fatalf("legacy runs did not receive default priority: %v", err)
	}
	var emptyHolds bool
	if err = legacy.QueryRow(ctx, `SELECT bool_and(snapshot_hold_at IS NULL AND snapshot_hold_reason IS NULL AND snapshot_hold_schema IS NULL AND snapshot_hold_sha256 IS NULL) FROM runs`).Scan(&emptyHolds); err != nil || !emptyHolds {
		t.Fatalf("migration marked existing rows held: %v", err)
	}
	var missingHistory bool
	if err = legacy.QueryRow(ctx, `SELECT bool_and(runnable_at IS NULL AND last_claim_traceparent IS NULL AND lease_yielded=false) FROM runs`).Scan(&missingHistory); err != nil || !missingHistory {
		t.Fatalf("telemetry migration fabricated history: %v", err)
	}
	var version int
	if err = legacy.QueryRow(ctx, `SELECT max(version_id) FROM goose_db_version WHERE is_applied`).Scan(&version); err != nil || version != 12 {
		t.Fatalf("unexpected migration version %d %v", version, err)
	}
	upgradeReport(t, "postgres", map[string]any{"passed": true, "from": 6, "to": version, "upgrade_twice": true, "committed_run_effect_artifact_and_snapshot_bytes_preserved": true, "unknown_effect_preserved": true, "legacy_audit_inputs_remain_null": nullInputs, "new_cleanup_rls": cleanupRLS, "new_priority_defaults_zero": defaultPriorities, "new_holds_default_null": emptyHolds, "telemetry_history_missing": missingHistory, "run_comparison": "all original columns; new priority, compatibility hold and telemetry defaults independently checked", "private_schema": schema, "scope": "additive migration rehearsal, not an old-binary rolling compatibility claim"})
	t.Log("private PostgreSQL schema upgraded 6 -> 12 twice; original run columns, unknown effect, artifact metadata and snapshot bytes unchanged; new priority defaults to zero and holds are NULL")
}

type resultValue struct {
	values map[string]string
	key    string
}

func (r *resultValue) Scan(src any) error {
	switch value := src.(type) {
	case string:
		r.values[r.key] = value
	case []byte:
		r.values[r.key] = string(value)
	default:
		return errors.New("unexpected capture value")
	}
	return nil
}

func upgradeReport(t *testing.T, kind string, value any) {
	t.Helper()
	if output := os.Getenv("FORGE_RECOVERY_UPGRADE_OUTPUT"); output != "" {
		if !filepath.IsAbs(output) {
			t.Fatal("upgrade output must be absolute")
		}
		if err := os.MkdirAll(output, 0700); err != nil {
			t.Fatal(err)
		}
		recoveryJSON(t, filepath.Join(output, kind+"-upgrade.json"), value)
	}
}

func TestRecoverySQLiteV3AdditiveUpgrade(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var launches atomic.Int64
	backend := &sandbox.TestBackend{StartFunc: func(_ context.Context, job sandbox.JobSpec) (sandbox.Job, error) {
		launches.Add(1)
		return sandbox.Job{ID: job.ID, Started: true, ExitCode: 0, Output: json.RawMessage(`{"fixture":true}`)}, nil
	}}
	cfg, workspace := runnerFixture(t, backend, time.Now)
	artifactRoot := t.TempDir()
	objects, err := artifact.NewLocalStore(artifactRoot, 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer objects.Close()
	cfg.Artifacts = objects
	stopBeforeDispatch := false
	cfg.Fault = func(point string) error {
		if point == "after_prepared" && stopBeforeDispatch {
			return errors.New("legacy prepared fixture")
		}
		return nil
	}
	engine, err := runner.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = engine.PrepareWorkspace(ctx, runner.PrepareRequest{WorkspaceRequest: workspace, SourceID: "fixture", ProfileID: "fixture"}); err != nil {
		t.Fatal(err)
	}
	knownRequest := operation(workspace)
	if _, err = engine.StartOperation(ctx, knownRequest); err != nil {
		t.Fatal(err)
	}
	known := recoveryWait(t, ctx, engine, knownRequest)
	stopBeforeDispatch = true
	unknownRequest := operation(workspace)
	unknownRequest.OperationID = "legacy_unknown"
	if _, err = engine.StartOperation(ctx, unknownRequest); err == nil {
		t.Fatal("prepared fault absent")
	}
	if err = engine.Close(); err != nil {
		t.Fatal(err)
	}
	journal, err := sql.Open("sqlite", cfg.JournalPath)
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	// Build a historical v3 fixture offline from freshly created local state.
	// Drop only columns known to be introduced by the not-yet-deployed v4.
	for _, query := range []string{`DROP TABLE operation_logs`, `DROP TABLE log_runs`, `DROP TABLE runner_artifacts`, `DROP TABLE IF EXISTS journal_identity`, `ALTER TABLE operations DROP COLUMN dispatch_started`, `ALTER TABLE volume_leases DROP COLUMN epoch`, `ALTER TABLE volume_leases DROP COLUMN source_hash`} {
		if _, err = journal.ExecContext(ctx, query); err != nil {
			t.Fatal(err)
		}
	}
	var startIntent int
	if err = journal.QueryRowContext(ctx, `SELECT count(*) FROM pragma_table_info('operations') WHERE name='docker_start_intent'`).Scan(&startIntent); err != nil {
		t.Fatal(err)
	}
	if startIntent != 0 {
		if _, err = journal.ExecContext(ctx, `ALTER TABLE operations DROP COLUMN docker_start_intent`); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = journal.ExecContext(ctx, `PRAGMA user_version=3`); err != nil {
		t.Fatal(err)
	}
	var knownBefore, unknownBefore string
	if err = journal.QueryRowContext(ctx, `SELECT request_json||receipt_json FROM operations WHERE id=?`, knownRequest.OperationID).Scan(&knownBefore); err != nil {
		t.Fatal(err)
	}
	if err = journal.QueryRowContext(ctx, `SELECT request_json FROM operations WHERE id=?`, unknownRequest.OperationID).Scan(&unknownBefore); err != nil {
		t.Fatal(err)
	}
	if err = journal.Close(); err != nil {
		t.Fatal(err)
	}
	// v3 had no journal registration or persistent identity. Remove only this
	// synthetic fixture's new-v4 marker together with its new-v4 schema columns.
	markerHash := sha256.Sum256([]byte(cfg.JournalPath))
	if err = os.Remove(filepath.Join(artifactRoot, ".runner-journal-"+hex.EncodeToString(markerHash[:]))); err != nil {
		t.Fatal(err)
	}
	cfg.Fault = nil
	upgraded, err := runner.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer upgraded.Close()
	journal, err = sql.Open("sqlite", cfg.JournalPath)
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	var version, conservative, pins int
	if err = journal.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil || version != 5 {
		t.Fatalf("version=%d %v", version, err)
	}
	if err = journal.QueryRowContext(ctx, `SELECT count(*) FROM operations WHERE dispatch_started=1`).Scan(&conservative); err != nil || conservative != 2 {
		t.Fatalf("legacy dispatch did not remain conservative: %d %v", conservative, err)
	}
	if err = journal.QueryRowContext(ctx, `SELECT count(*) FROM runner_artifacts`).Scan(&pins); err != nil || pins != 0 {
		t.Fatalf("legacy migration fabricated snapshot pins: %d %v", pins, err)
	}
	var knownAfter, unknownAfter string
	if err = journal.QueryRowContext(ctx, `SELECT request_json||receipt_json FROM operations WHERE id=?`, knownRequest.OperationID).Scan(&knownAfter); err != nil {
		t.Fatal(err)
	}
	if err = journal.QueryRowContext(ctx, `SELECT request_json FROM operations WHERE id=?`, unknownRequest.OperationID).Scan(&unknownAfter); err != nil {
		t.Fatal(err)
	}
	if knownBefore != knownAfter || unknownBefore != unknownAfter {
		t.Fatal("upgrade modified immutable request/receipt bytes")
	}
	restoredKnown, err := upgraded.InspectOperation(ctx, runner.InspectRequest{WorkspaceRequest: workspace, OperationID: knownRequest.OperationID})
	if err != nil || restoredKnown.Receipt != known.Receipt || restoredKnown.Status != runner.Succeeded {
		t.Fatalf("legacy receipt lost: %+v %v", restoredKnown, err)
	}
	unknown, err := upgraded.InspectOperation(ctx, runner.InspectRequest{WorkspaceRequest: workspace, OperationID: unknownRequest.OperationID})
	if err != nil || unknown.Status != runner.Unknown {
		t.Fatalf("legacy prepared without evidence authorized replay: %+v %v", unknown, err)
	}
	duplicate, err := upgraded.StartOperation(ctx, unknownRequest)
	if err != nil || duplicate.Status != runner.Unknown || launches.Load() != 1 {
		t.Fatalf("legacy unknown dispatched: %+v count=%d %v", duplicate, launches.Load(), err)
	}
	upgradeReport(t, "sqlite", map[string]any{"passed": true, "from": 3, "to": version, "legacy_operations": 2, "legacy_dispatch_conservative": conservative, "immutable_request_receipt_bytes_preserved": true, "unknown_remains_unknown": true, "new_external_processes": 0, "scope": "synthetic historical schema, real SQLite migration and no-process TestBackend; existing operation receipts retained, missing pre-v4 snapshot pins cannot be reconstructed"})
	t.Log("real SQLite 3 -> 4 migration preserved two immutable operations/receipt; missing legacy dispatch evidence stays unknown; zero new backend launches")
}
