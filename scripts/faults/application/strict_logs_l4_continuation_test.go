//go:build linux

package applicationfaults

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

const slL4PriorRun = "run_DKT2OOLEVCKYXHW5NGNS7BKBHJ"
const slL4FailedManifest = "8781bba443e5a83da96cb077eb18223facfeee3469de0b23fce6c37529eef97c"
const slL4ObservationManifest = "7ace4cab71b3f4fbaba9f8ed58ae73dbfe202ab94d83bb4ecbe85bac6d8ab736"

type slL4RecoveryReport struct {
	Passed           bool              `json:"passed"`
	RunID            string            `json:"run_id"`
	Tenant           string            `json:"tenant_id"`
	Schema           string            `json:"schema"`
	SchemaOID        int64             `json:"schema_oid"`
	UUID             string            `json:"journal_uuid"`
	Released         bool              `json:"released"`
	SnapshotVerified bool              `json:"snapshot_verified"`
	Before           map[string]string `json:"input_sha256_before"`
	After            map[string]string `json:"input_sha256_after"`
}
type slL4Seal struct {
	Purpose    string `json:"purpose"`
	RunID      string `json:"recovered_run_id"`
	Failed     string `json:"failed_manifest_sha256"`
	Observed   string `json:"observation_manifest_sha256"`
	Recovery   string `json:"recovery_manifest_sha256"`
	Acceptance string `json:"acceptance_sha256"`
}

func slL4RecoveryGuard(report slL4RecoveryReport, seal slL4Seal, uuid string) error {
	if seal.Purpose != "strict-logs-targeted-l4-v1" || seal.RunID != slL4PriorRun || seal.Failed != slL4FailedManifest || seal.Observed != slL4ObservationManifest || len(seal.Recovery) != 64 || len(seal.Acceptance) != 64 {
		return fmt.Errorf("targeted L4 continuation seal differs")
	}
	if !report.Passed || report.RunID != slL4PriorRun || report.Tenant != "sl-L4-xsra53ckdacsus345hmddtehsj" || report.Schema != "appfault_strictlogs_zg4ub4tnaruvpaojcj5dmmeqpc" || report.SchemaOID != 852843 || report.UUID != uuid || !report.Released || !report.SnapshotVerified || len(report.Before) == 0 || !reflect.DeepEqual(report.Before, report.After) {
		return fmt.Errorf("explicit failed-L4 recovery is not complete or bound")
	}
	return nil
}

// Five existing cases retain their original source and failed aggregate. This
// gate allows only a separate L4 invocation after the unknown prior operation
// has been reconciled and archived by the explicit production recovery flow.
func slL4Prerequisite(a slAcceptance, c slRunnerConfig, current slJournal) (map[string]string, error) {
	hashes := map[string]string{}
	sealPath := filepath.Join(a.ScopeRoot, "logs-l4-continuation.json")
	raw, err := slCleanupHashFile(sealPath, hashes, "")
	if err != nil {
		return nil, err
	}
	var seal slL4Seal
	if err = json.Unmarshal(raw, &seal); err != nil {
		return nil, err
	}
	recovery := filepath.Join(a.ScopeRoot, "evidence/logs-03-recovery")
	var report slL4RecoveryReport
	if err = slReadJSON(filepath.Join(recovery, "report.json"), &report); err != nil {
		return nil, err
	}
	if err = slL4RecoveryGuard(report, seal, current.Identity); err != nil {
		return nil, err
	}
	for _, item := range []struct{ path, hash string }{
		{filepath.Join(a.ScopeRoot, "evidence/logs-03"), seal.Failed},
		{filepath.Join(a.ScopeRoot, "evidence/logs-03-retained-observation"), seal.Observed},
		{recovery, seal.Recovery},
	} {
		if err = slCleanupManifest(item.path, item.hash, hashes); err != nil {
			return nil, err
		}
	}
	if _, err = slCleanupHashFile(filepath.Join(a.ScopeRoot, "acceptance-logs-l4-01.json"), hashes, seal.Acceptance); err != nil {
		return nil, err
	}
	var previous struct {
		Passed bool `json:"passed"`
		Cases  map[string]struct {
			Passed bool `json:"passed"`
		} `json:"cases"`
		Before map[string]string `json:"input_sha256_before"`
		After  map[string]string `json:"input_sha256_after"`
	}
	if err = slReadJSON(filepath.Join(a.ScopeRoot, "evidence/logs-03/acceptance.json"), &previous); err != nil {
		return nil, err
	}
	if previous.Passed || len(previous.Cases) != 5 || !reflect.DeepEqual(previous.Before, previous.After) {
		return nil, fmt.Errorf("original logs03 failure was relabeled or changed")
	}
	for _, name := range []string{"L1", "L2-L3-default", "L3-bytes", "L3-count", "L5"} {
		if !previous.Cases[name].Passed {
			return nil, fmt.Errorf("missing original passing log case: %s", name)
		}
	}
	for _, inputs := range []map[string]string{previous.Before, report.Before} {
		for path, hash := range inputs {
			if !slWithin(a.ScopeRoot, path) {
				return nil, fmt.Errorf("prior input outside dedicated scope")
			}
			if _, err = slCleanupHashFile(path, hashes, hash); err != nil {
				return nil, err
			}
		}
	}
	var original struct {
		Acceptance slAcceptance `json:"acceptance"`
	}
	if err = slReadJSON(filepath.Join(a.ScopeRoot, "evidence/logs-03/preflight.json"), &original); err != nil {
		return nil, err
	}
	if err = slValidateContract(original.Acceptance, c); err != nil {
		return nil, err
	}
	if err = slHistoricalBinding(a, original.Acceptance, original.Acceptance, true); err != nil {
		return nil, err
	}
	var final slJournal
	if err = slReadJSON(filepath.Join(recovery, "final-journal.json"), &final); err != nil {
		return nil, err
	}
	if !slCleanupSameJournal(final, current) {
		return nil, fmt.Errorf("journal changed after explicit L4 recovery")
	}
	if err = slIdle(current); err != nil {
		return nil, err
	}
	for _, row := range current.Tables["operation_logs"] {
		if row["cleanup_state"] != "removed" {
			return nil, fmt.Errorf("old container cleanup remains incomplete")
		}
	}
	// Live read-only SQL confirms that the original schema is actually idle;
	// report booleans cannot authorize a new workload on retained capacity.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	u, err := url.Parse(os.Getenv("FORGE_TEST_DATABASE_URL"))
	if err != nil || u.Hostname() != "127.0.0.1" || u.Port() != "32773" || u.Path != "/forge" || u.Query().Get("search_path") != "" {
		return nil, fmt.Errorf("dedicated readonly preflight database required")
	}
	db, err := pgx.Connect(ctx, u.String())
	if err != nil {
		return nil, fmt.Errorf("prior schema readonly connection unavailable")
	}
	defer db.Close(ctx)
	tx, err := db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	var liveOID int64
	if err = tx.QueryRow(ctx, "SELECT oid::bigint FROM pg_catalog.pg_namespace WHERE nspname=$1", report.Schema).Scan(&liveOID); err != nil {
		return nil, err
	}
	if liveOID != report.SchemaOID {
		return nil, fmt.Errorf("original L4 schema namespace was replaced")
	}
	schema := pgx.Identifier{"appfault_strictlogs_zg4ub4tnaruvpaojcj5dmmeqpc"}.Sanitize()
	queries := []string{
		"SELECT count(*) FROM " + schema + ".runs WHERE state NOT IN ('completed','failed','cancelled','budget_exhausted')",
		"SELECT COALESCE(sum(active_count),0) FROM " + schema + ".tenant_runtime",
		"SELECT COALESCE(sum(reserved_slots),0) FROM " + schema + ".runners",
		"SELECT count(*) FROM " + schema + ".runner_allocations WHERE state<>'released'",
		"SELECT COALESCE(sum(active_requests+reserved_tokens+reserved_microusd),0) FROM " + schema + ".provider_quotas",
		"SELECT count(*) FROM " + schema + ".quota_reservations WHERE status<>'settled' OR NOT request_slot_released",
		"SELECT count(*) FROM " + schema + ".effects WHERE status NOT IN ('succeeded','failed','cancelled')",
		"SELECT count(*) FROM " + schema + ".workspace_cleanup WHERE phase<>'released'",
	}
	for _, query := range queries {
		var n int64
		if err = tx.QueryRow(ctx, query).Scan(&n); err != nil {
			return nil, err
		}
		if n != 0 {
			return nil, fmt.Errorf("prior L4 schema retains active work or budget")
		}
	}
	var state string
	if err = tx.QueryRow(ctx, "SELECT state FROM "+schema+".runs WHERE id=$1 AND tenant_id=$2", slL4PriorRun, "sl-L4-xsra53ckdacsus345hmddtehsj").Scan(&state); err != nil {
		return nil, err
	}
	if state != "cancelled" {
		return nil, fmt.Errorf("old L4 explicit cancellation not terminal")
	}
	return hashes, tx.Commit(ctx)
}

func TestStrictLogsL4ContinuationRequiresActualRecovery(t *testing.T) {
	s := slL4Seal{Purpose: "strict-logs-targeted-l4-v1", RunID: slL4PriorRun, Failed: slL4FailedManifest, Observed: slL4ObservationManifest, Recovery: slL4FailedManifest, Acceptance: slL4FailedManifest}
	r := slL4RecoveryReport{Passed: true, RunID: slL4PriorRun, Tenant: "sl-L4-xsra53ckdacsus345hmddtehsj", Schema: "appfault_strictlogs_zg4ub4tnaruvpaojcj5dmmeqpc", SchemaOID: 852843, UUID: slCleanupUUID, Released: true, SnapshotVerified: true, Before: map[string]string{"bound": "hash"}, After: map[string]string{"bound": "hash"}}
	if err := slL4RecoveryGuard(r, s, slCleanupUUID); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"Passed", "Released", "SnapshotVerified", "RunID", "Tenant", "Schema", "SchemaOID", "UUID", "After"} {
		bad := r
		v := reflect.ValueOf(&bad).Elem().FieldByName(field)
		v.Set(reflect.Zero(v.Type()))
		if slL4RecoveryGuard(bad, s, slCleanupUUID) == nil {
			t.Fatal("unclosed recovery admitted", field)
		}
	}
	for _, field := range []string{"Purpose", "RunID", "Failed", "Observed", "Recovery", "Acceptance"} {
		bad := s
		reflect.ValueOf(&bad).Elem().FieldByName(field).SetString("wrong")
		if slL4RecoveryGuard(r, bad, slCleanupUUID) == nil {
			t.Fatal("unbound continuation admitted", field)
		}
	}
}
