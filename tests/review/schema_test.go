// Package review_test contains independent adversarial regression tests. These
// tests are opt-in and only target a disposable, explicitly selected PostgreSQL.
package review_test

import (
	"context"
	"crypto/rand"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func reviewDB(t *testing.T) (context.Context, *pgx.Conn) {
	t.Helper()
	dsn := os.Getenv("FORGE_REVIEW_DATABASE_URL")
	if dsn == "" {
		t.Skip("set FORGE_REVIEW_DATABASE_URL for the disposable review database")
	}
	if os.Getenv("FORGE_REVIEW_ALLOW_FIXTURES") != "1" {
		t.Fatal("FORGE_REVIEW_ALLOW_FIXTURES=1 is required for rollback-only SQL fixtures")
	}
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal("invalid review database configuration")
	}
	if cfg.Host != "127.0.0.1" && cfg.Host != "localhost" && cfg.Host != "::1" {
		t.Fatal("review tests only accept a loopback database host")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return ctx, conn
}

// SQL/RLS fixtures also need their own migrated schema. A freshly created CI
// database deliberately has no public application tables.
func schemaReviewDB(t *testing.T) (context.Context, *pgx.Conn) {
	t.Helper()
	ctx, store := isolatedStore(t)
	connection, err := store.Pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(connection.Release)
	return ctx, connection.Conn()
}

func exec(t *testing.T, ctx context.Context, tx pgx.Tx, sql string, args ...any) {
	t.Helper()
	if _, err := tx.Exec(ctx, sql, args...); err != nil {
		t.Fatal(err)
	}
}

func expectSQLState(t *testing.T, ctx context.Context, tx pgx.Tx, code, sql string, args ...any) {
	t.Helper()
	savepoint, err := tx.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer savepoint.Rollback(ctx)
	_, err = savepoint.Exec(ctx, sql, args...)
	var pgerr *pgconn.PgError
	if !errors.As(err, &pgerr) || pgerr.Code != code {
		t.Fatalf("expected SQLSTATE %s, got %v", code, err)
	}
}

type fixture struct{ tenantA, tenantB string }

func seed(t *testing.T, ctx context.Context, tx pgx.Tx) fixture {
	t.Helper()
	f := fixture{"review_a_" + rand.Text(), "review_b_" + rand.Text()}
	for _, tenant := range []string{f.tenantA, f.tenantB} {
		exec(t, ctx, tx, `INSERT INTO tenants(id,name) VALUES($1,'independent review fixture')`, tenant)
	}
	exec(t, ctx, tx, `INSERT INTO projects(tenant_id,id,name,repo_source,repo_profile)
		VALUES($1,'project_a','review','local-review','{}'),($2,'project_b','review','local-review','{}')`, f.tenantA, f.tenantB)
	for _, spec := range []struct{ tenant, run, project string }{
		{f.tenantA, "run_a", "project_a"}, {f.tenantA, "run_a_other", "project_a"}, {f.tenantB, "run_b", "project_b"},
	} {
		exec(t, ctx, tx, `INSERT INTO runs(tenant_id,id,project_id,principal_id,task,base_commit,state,version,snapshot,config_snapshot)
			VALUES($1,$2,$3,'review_principal','review fixture','fixed-base','queued',1,'{}','{}')`, spec.tenant, spec.run, spec.project)
	}
	exec(t, ctx, tx, `INSERT INTO steps(tenant_id,run_id,seq,kind,status,input_hash)
		VALUES($1,'run_a',1,'model','completed','review-hash')`, f.tenantA)
	exec(t, ctx, tx, `INSERT INTO effects(tenant_id,run_id,step_seq,ordinal,operation_id,kind,args,args_hash,expected_revision,epoch,policy_version,status)
		VALUES($1,'run_a',1,1,'operation_a','read_file','{}','review-hash',1,1,'review-policy','planned')`, f.tenantA)
	return f
}

func TestReviewEffectsAndApprovalsBindDurableParents(t *testing.T) {
	ctx, conn := schemaReviewDB(t)
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	f := seed(t, ctx, tx)
	t.Run("effect requires its step", func(t *testing.T) {
		expectSQLState(t, ctx, tx, "23503", `INSERT INTO effects(tenant_id,run_id,step_seq,ordinal,operation_id,kind,args,args_hash,expected_revision,epoch,policy_version,status)
			VALUES($1,'run_a',999,1,'orphan_operation','read_file','{}','review-hash',1,1,'review-policy','planned')`, f.tenantA)
	})
	t.Run("approval binds effect and run together", func(t *testing.T) {
		expectSQLState(t, ctx, tx, "23503", `INSERT INTO approvals(tenant_id,id,run_id,effect_id,args_hash,workspace_revision,policy_version)
			VALUES($1,'wrong_run_approval','run_a_other','operation_a','review-hash',1,'review-policy')`, f.tenantA)
	})
	t.Run("run cannot reference another tenant project", func(t *testing.T) {
		expectSQLState(t, ctx, tx, "23503", `INSERT INTO runs(tenant_id,id,project_id,principal_id,task,base_commit,state,version,snapshot,config_snapshot)
			VALUES($1,'cross_tenant_run','project_a','review_principal','review','fixed-base','queued',1,'{}','{}')`, f.tenantB)
	})
}

func TestReviewRLSUsesActualNonOwnerRoleAndLocalContext(t *testing.T) {
	ctx, conn := schemaReviewDB(t)
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	f := seed(t, ctx, tx)
	role := "review_role_" + rand.Text()
	identifier := pgx.Identifier{role}.Sanitize()
	// The role and all fixtures are created in the same rollback-only transaction.
	exec(t, ctx, tx, `CREATE ROLE `+identifier+` NOLOGIN NOSUPERUSER NOBYPASSRLS`)
	var schema string
	if err := tx.QueryRow(ctx, `SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	exec(t, ctx, tx, `GRANT USAGE ON SCHEMA `+pgx.Identifier{schema}.Sanitize()+` TO `+identifier)
	exec(t, ctx, tx, `GRANT SELECT,INSERT,UPDATE,DELETE ON runs,projects TO `+identifier)
	exec(t, ctx, tx, `SET LOCAL ROLE `+identifier)
	var superuser, bypass, owner bool
	err = tx.QueryRow(ctx, `SELECT r.rolsuper,r.rolbypassrls,c.relowner=r.oid
		FROM pg_roles r CROSS JOIN pg_class c WHERE r.rolname=current_user AND c.oid='runs'::regclass`).Scan(&superuser, &bypass, &owner)
	if err != nil || superuser || bypass || owner {
		t.Fatalf("test role could bypass RLS: super=%v bypass=%v owner=%v error=%v", superuser, bypass, owner, err)
	}
	count := func(want int) {
		t.Helper()
		var got int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM runs`).Scan(&got); err != nil || got != want {
			t.Fatalf("visible run count = %d, error=%v; want %d", got, err, want)
		}
	}
	count(0)
	exec(t, ctx, tx, `SELECT set_config('forge.tenant_id',$1,true)`, f.tenantA)
	count(2)
	tag, err := tx.Exec(ctx, `UPDATE runs SET task='unauthorized' WHERE tenant_id=$1 AND id='run_b'`, f.tenantB)
	if err != nil || tag.RowsAffected() != 0 {
		t.Fatalf("cross-tenant update affected %d rows: %v", tag.RowsAffected(), err)
	}
	expectSQLState(t, ctx, tx, "42501", `INSERT INTO projects(tenant_id,id,name,repo_source,repo_profile)
		VALUES($1,'cross_tenant_project','review','local-review','{}')`, f.tenantB)
	exec(t, ctx, tx, `SELECT set_config('forge.tenant_id',$1,true)`, f.tenantB)
	count(1)
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	var leakedContext string
	if err := conn.QueryRow(ctx, `SELECT coalesce(current_setting('forge.tenant_id',true),'')`).Scan(&leakedContext); err != nil || leakedContext != "" {
		t.Fatalf("transaction-local context leaked after rollback: context=%q error=%v", leakedContext, err)
	}
	var roleCount int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM pg_roles WHERE rolname=$1`, role).Scan(&roleCount); err != nil || roleCount != 0 {
		t.Fatalf("review role survived rollback: count=%d error=%v", roleCount, err)
	}
}
