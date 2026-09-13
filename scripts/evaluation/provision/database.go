package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	migrations "github.com/JDinSeattle/forge-runtime/db"
	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	"github.com/JDinSeattle/forge-runtime/internal/quota"
	"github.com/jackc/pgx/v5"
)

type provisionReport struct {
	SchemaVersion         int                  `json:"schema_version"`
	Purpose               string               `json:"purpose"`
	Phase                 string               `json:"phase"`
	Schema                string               `json:"schema"`
	SchemaOID             uint32               `json:"schema_oid"`
	Roles                 map[string]string    `json:"roles"`
	Tenant                domain.ID            `json:"tenant_id"`
	Principal             domain.ID            `json:"principal_id"`
	APIURL                string               `json:"api_url"`
	RunnerID              string               `json:"runner_id"`
	RunnerEndpoint        string               `json:"runner_endpoint"`
	RunnerSlots           int                  `json:"runner_slots"`
	WorkerSlots           int                  `json:"worker_slots"`
	Provider              string               `json:"provider"`
	Model                 string               `json:"model"`
	ConfigID              string               `json:"config_id"`
	CredentialGroup       string               `json:"credential_group"`
	PriceVersion          string               `json:"price_version"`
	ExactPricing          bool                 `json:"exact_pricing"`
	BatchID               string               `json:"batch_id"`
	Budget                budget               `json:"budget"`
	Database              map[string]any       `json:"database"`
	CandidateDir          string               `json:"candidate_dir"`
	Hashes                map[string]string    `json:"candidate_hashes"`
	MigrationVersion      int64                `json:"migration_version"`
	RelationOIDs          map[string]uint32    `json:"relation_oids"`
	MembershipFunctionOID uint32               `json:"membership_function_oid"`
	RoleChecks            map[string]roleCheck `json:"role_checks"`
	Quota                 json.RawMessage      `json:"initial_quota"`
	CreatedAt             time.Time            `json:"created_at"`
	LaunchNotAfter        time.Time            `json:"launch_not_after"`
	TokenExpiresAt        time.Time            `json:"token_expires_at"`
	Scope                 string               `json:"scope"`
}
type budget struct {
	Total int64                   `json:"total_microusd"`
	Tasks map[string]domain.Money `json:"task_budgets_microusd"`
}

func provision(ctx context.Context, b bundle, admin *url.URL) (retErr error) {
	suffix, e := randomSuffix()
	if e != nil {
		return e
	}
	schema, roles, e := names(suffix)
	if e != nil {
		return e
	}
	if e = os.Mkdir(b.Out, 0700); e != nil {
		return e
	}
	if e = syncDirectory(filepath.Dir(b.Out)); e != nil {
		return e
	}
	steps := &stages{out: b.Out, last: "planned"}
	report := provisionReport{SchemaVersion: 1, Purpose: purpose, Phase: "planned", Schema: schema, Roles: roles, Tenant: domain.ID("eval-" + suffix), Principal: domain.ID("eval-operator-" + suffix), APIURL: "http://" + b.Platform.Listen, RunnerID: b.Platform.RunnerID, RunnerEndpoint: "unix://" + b.Platform.Runner.UnixSocket, RunnerSlots: 4, WorkerSlots: 1, Provider: "deepseek", Model: modelID, ConfigID: b.Registration.ConfigID, CredentialGroup: credentialGroup, PriceVersion: b.Registration.Spec.PriceVersion, ExactPricing: false, BatchID: b.BatchID, Budget: budget{Total: 2000000, Tasks: b.Registration.Tasks}, Database: map[string]any{"host": "127.0.0.1", "port": 32773, "name": "forge"}, CandidateDir: b.Dir, Hashes: b.Hashes, RoleChecks: map[string]roleCheck{}, Scope: "one fixed operator batch; provisioning only, no paid authorization renewal or service/Engine/key/pool action; quota window is not a lifetime invoice cap"}
	if e = writeJSON(filepath.Join(b.Out, "recovery.json"), report); e != nil {
		return e
	}
	defer func() {
		if retErr != nil {
			_ = writeJSON(filepath.Join(b.Out, "failure.json"), map[string]any{"phase": steps.last, "sqlstate": sqlState(retErr), "schema": schema, "roles": roles, "outcome": "may be partial; retain all state; no automatic retry, reset or drop"})
			retErr = &stageError{stage: steps.last, err: retErr}
		}
	}()
	// Credentials are generated once and persisted privately before role creation.
	// No password, DSN, token or credential hash enters any descriptor.
	passwords := map[string]string{}
	for _, kind := range []string{"api", "worker", "audit"} {
		passwords[kind] = rand.Text() + rand.Text()
		name, key := "api.env", "FORGE_DATABASE_URL"
		if kind == "worker" {
			name = "worker-db.env"
		}
		if kind == "audit" {
			name, key = "audit.env", "FORGE_EVAL_AUDIT_DSN"
		}
		dsn := scopedURL(admin, schema, roles[kind], passwords[kind])
		if kind == "audit" {
			dsn = auditURL(admin, roles[kind], passwords[kind])
		}
		if e = writePrivate(filepath.Join(b.Out, name), []byte(key+"="+quote(dsn)+"\n")); e != nil {
			return e
		}
	}
	if e = steps.record("schema_creation_started", map[string]any{"schema": schema, "roles": roles}); e != nil {
		return e
	}
	conn, e := pgx.Connect(ctx, admin.String())
	if e != nil {
		return e
	}
	defer conn.Close(ctx)
	var dbname, user string
	var super bool
	if e = conn.QueryRow(ctx, `SELECT current_database(),current_user,rolsuper FROM pg_roles WHERE rolname=current_user`).Scan(&dbname, &user, &super); e != nil {
		return e
	}
	if dbname != "forge" || !super {
		return errors.New("explicit dedicated administrator identity required")
	}
	var collision bool
	if e = conn.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname=$1) OR EXISTS(SELECT 1 FROM pg_roles WHERE rolname=ANY($2))`, schema, []string{roles["api"], roles["worker"], roles["audit"]}).Scan(&collision); e != nil {
		return e
	}
	if collision {
		return errors.New("fresh schema and role identifiers required")
	}
	// Only the random validated namespace is created. No PUBLIC ACL is changed.
	if _, e = conn.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); e != nil {
		return e
	}
	if e = conn.QueryRow(ctx, `SELECT oid FROM pg_namespace WHERE nspname=$1 AND nspowner=(SELECT oid FROM pg_roles WHERE rolname=current_user)`, schema).Scan(&report.SchemaOID); e != nil {
		return e
	}
	if e = steps.record("schema_created", map[string]any{"schema": schema, "schema_oid": report.SchemaOID}); e != nil {
		return e
	}
	scoped := scopedURL(admin, schema, "", "")
	if e = migrations.Migrate(ctx, scoped); e != nil {
		return e
	}
	store, e := persistence.Open(ctx, scoped)
	if e != nil {
		return e
	}
	defer store.Close()
	if e = verifyMigration(ctx, store, schema, user, &report); e != nil {
		return e
	}
	if e = steps.record("migrated", map[string]any{"schema": schema, "schema_oid": report.SchemaOID, "migration_version": report.MigrationVersion, "relation_oids": report.RelationOIDs, "membership_function_oid": report.MembershipFunctionOID}); e != nil {
		return e
	}
	if e = store.BootstrapTenant(ctx, report.Tenant, report.Principal, "admin"); e != nil {
		return e
	}
	tag, e := store.Pool.Exec(ctx, `UPDATE tenant_runtime SET max_active=1 WHERE tenant_id=$1 AND active_count=0`, report.Tenant)
	if e != nil {
		return e
	}
	if tag.RowsAffected() != 1 {
		return errors.New("fresh tenant capacity row required")
	}
	if e = store.RegisterRunner(ctx, report.RunnerID, report.RunnerEndpoint, 4); e != nil {
		return e
	}
	var groups int
	if e = store.Pool.QueryRow(ctx, `SELECT count(*) FROM provider_quotas`).Scan(&groups); e != nil {
		return e
	}
	if groups != 0 {
		return errors.New("refuse reconfiguration of existing quota")
	}
	if e = quota.New(store.Pool).Configure(ctx, quota.Config{CredentialGroup: credentialGroup, MaxConcurrent: 1, MaxTokens: 10000000, MaxCost: 2000000, WindowDuration: 24 * time.Hour, FailureThreshold: 3, BreakerCooldown: 10 * time.Second}); e != nil {
		return e
	}
	if e = store.Pool.QueryRow(ctx, `SELECT to_jsonb(q),clock_timestamp(),clock_timestamp()+interval '2 hours' FROM provider_quotas q WHERE credential_group=$1 AND active_requests=0 AND reserved_tokens=0 AND committed_tokens=0 AND reserved_microusd=0 AND committed_microusd=0 AND window_generation=1`, credentialGroup).Scan(&report.Quota, &report.CreatedAt, &report.LaunchNotAfter); e != nil {
		return e
	}
	if e = steps.record("tenant_runner_quota_created", map[string]any{"tenant_id": report.Tenant, "principal_id": report.Principal, "runner_id": report.RunnerID, "initial_quota": report.Quota, "launch_not_after": report.LaunchNotAfter}); e != nil {
		return e
	}
	if e = createRoles(ctx, store, schema, roles, passwords); e != nil {
		return e
	}
	if e = steps.record("roles_created", map[string]any{"roles": roles, "schema_oid": report.SchemaOID}); e != nil {
		return e
	}
	for _, kind := range []string{"api", "worker", "audit"} {
		dsn := scopedURL(admin, schema, roles[kind], passwords[kind])
		check, e := verifyRole(ctx, dsn, kind, schema, roles[kind], report.SchemaOID, report.RelationOIDs, report.MembershipFunctionOID, report.Tenant, report.Principal)
		if e != nil {
			return e
		}
		report.RoleChecks[kind] = check
	}
	if e = steps.record("roles_verified", report.RoleChecks); e != nil {
		return e
	}
	token, e := store.IssueToken(ctx, report.Principal, 2*time.Hour)
	if e != nil {
		return e
	}
	client, e := clientEnvironment(report.APIURL, report.Tenant, token)
	if e != nil {
		return e
	}
	if e = writePrivate(filepath.Join(b.Out, "client.env"), client); e != nil {
		return e
	}
	// Validate API authentication without logging the token or its persisted hash.
	api, e := persistence.Open(ctx, scopedURL(admin, schema, roles["api"], passwords["api"]))
	if e != nil {
		return e
	}
	identity, e := api.Authenticate(ctx, token, report.Tenant)
	api.Close()
	if e != nil {
		return e
	}
	if identity.PrincipalID != report.Principal || identity.TenantID != report.Tenant || identity.Role != "admin" {
		return errors.New("API token tenant/admin binding mismatch")
	}
	if e = store.Pool.QueryRow(ctx, `SELECT expires_at FROM api_tokens WHERE principal_id=$1 AND revoked_at IS NULL`, report.Principal).Scan(&report.TokenExpiresAt); e != nil {
		return e
	}
	if e = verifyFresh(ctx, store, report); e != nil {
		return e
	}
	// Inputs are small nonsecret JSON. Recheck before sealing the descriptor;
	// never hash or inspect any generated .env or existing signing key.
	for name, hash := range b.Hashes {
		path := filepath.Join(b.Dir, name)
		if name == "authority_runner.json" {
			path = filepath.Join(b.Scope, "runtime", "runner.json")
		}
		raw, e := regularRead(path)
		if e != nil {
			return e
		}
		if sum(raw) != hash {
			return errors.New("candidate/authority changed during provisioning")
		}
	}
	report.Phase = "ready"
	if e = writeJSON(filepath.Join(b.Out, "provision.json"), report); e != nil {
		return e
	}
	return nil
}

func latestMigration() (int64, error) {
	entries, e := fs.ReadDir(migrations.Migrations, "migrations")
	if e != nil {
		return 0, e
	}
	var highest int64
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		v, e := strconv.ParseInt(strings.SplitN(entry.Name(), "_", 2)[0], 10, 64)
		if e != nil {
			return 0, e
		}
		if v > highest {
			highest = v
		}
	}
	if highest < 10 {
		return 0, errors.New("membership authority migration missing")
	}
	return highest, nil
}
func verifyMigration(ctx context.Context, s *persistence.Store, schema, owner string, r *provisionReport) error {
	var current string
	var oid uint32
	if e := s.Pool.QueryRow(ctx, `SELECT current_schema(),(SELECT oid FROM pg_namespace WHERE nspname=current_schema())`).Scan(&current, &oid); e != nil {
		return e
	}
	if current != schema || oid != r.SchemaOID {
		return errors.New("migration namespace changed")
	}
	expected, e := latestMigration()
	if e != nil {
		return e
	}
	if e = s.Pool.QueryRow(ctx, `SELECT max(version_id) FROM goose_db_version WHERE is_applied`).Scan(&r.MigrationVersion); e != nil {
		return e
	}
	if r.MigrationVersion != expected {
		return errors.New("migration version differs from embedded sources")
	}
	r.RelationOIDs = map[string]uint32{}
	rows, e := s.Pool.Query(ctx, `SELECT relname,oid FROM pg_class WHERE relnamespace=$1 AND relkind IN ('r','p') ORDER BY relname`, r.SchemaOID)
	if e != nil {
		return e
	}
	for rows.Next() {
		var name string
		var id uint32
		if e = rows.Scan(&name, &id); e != nil {
			rows.Close()
			return e
		}
		r.RelationOIDs[name] = id
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	for _, name := range []string{"runs", "memberships", "tenants", "api_tokens", "provider_quotas", "quota_reservations", "workspace_cleanup", "goose_db_version"} {
		if r.RelationOIDs[name] == 0 {
			return errors.New("required migrated relation missing")
		}
	}
	var definer, publicExec bool
	var source, fnOwner string
	var config []string
	e = s.Pool.QueryRow(ctx, `SELECT p.oid,p.prosecdef,p.prosrc,pg_get_userbyid(p.proowner),p.proconfig,EXISTS(SELECT 1 FROM aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a WHERE a.grantee=0 AND a.privilege_type='EXECUTE') FROM pg_proc p WHERE p.pronamespace=$1 AND p.oid=to_regprocedure($2)`, r.SchemaOID, pgx.Identifier{schema}.Sanitize()+".lock_current_membership(text,text)").Scan(&r.MembershipFunctionOID, &definer, &source, &fnOwner, &config, &publicExec)
	if e != nil {
		return e
	}
	if !definer || publicExec || fnOwner != owner || len(config) != 1 || config[0] != "search_path=pg_catalog, pg_temp" || !strings.Contains(source, schema+".memberships") || !strings.Contains(source, "forge.tenant_id") {
		return errors.New("schema-bound membership function authority differs")
	}
	return nil
}
func createRoles(ctx context.Context, s *persistence.Store, schema string, roles, passwords map[string]string) error {
	if !validNames(schema, roles) {
		return errors.New("invalid generated identifiers")
	}
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	ns := pgx.Identifier{schema}.Sanitize()
	for _, kind := range []string{"api", "worker", "audit"} {
		name := pgx.Identifier{roles[kind]}.Sanitize()
		bypass := "NOBYPASSRLS"
		if kind == "worker" {
			bypass = "BYPASSRLS"
		}
		// Only cryptographically generated base32 password bytes are interpolated.
		pass := passwords[kind]
		if len(pass) != 52 || strings.IndexFunc(pass, func(r rune) bool { return !(r >= 'A' && r <= 'Z' || r >= '2' && r <= '7') }) >= 0 {
			return errors.New("invalid generated password")
		}
		if _, e = tx.Exec(ctx, "CREATE ROLE "+name+" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION "+bypass+" PASSWORD '"+pass+"'"); e != nil {
			return e
		}
		statements := []string{"GRANT USAGE ON SCHEMA " + ns + " TO " + name}
		if kind == "audit" {
			statements = append(statements, "GRANT SELECT ON ALL TABLES IN SCHEMA "+ns+" TO "+name, "REVOKE ALL ON "+ns+".api_tokens FROM "+name, "ALTER ROLE "+name+" SET default_transaction_read_only = on", "ALTER ROLE "+name+" SET search_path = pg_catalog")
		} else {
			statements = append(statements, "GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA "+ns+" TO "+name, "REVOKE ALL ON "+ns+".goose_db_version,"+ns+".api_tokens,"+ns+".memberships,"+ns+".tenants FROM "+name, "GRANT SELECT ON "+ns+".memberships,"+ns+".tenants TO "+name, "GRANT EXECUTE ON FUNCTION "+ns+".lock_current_membership(text,text) TO "+name)
			if kind == "api" {
				statements = append(statements, "GRANT SELECT ON "+ns+".api_tokens TO "+name)
			}
		}
		for _, q := range statements {
			if _, e = tx.Exec(ctx, q); e != nil {
				return e
			}
		}
	}
	return tx.Commit(ctx)
}

type roleCheck struct {
	Role              string `json:"role"`
	Schema            string `json:"schema"`
	SchemaOID         uint32 `json:"schema_oid"`
	Bypass            bool   `json:"bypass_rls"`
	Tables            int    `json:"checked_tables"`
	OutsidePrivileges int    `json:"outside_table_privileges"`
	MembershipExecute bool   `json:"membership_execute"`
	MembershipRole    string `json:"membership_role,omitempty"`
	ReadOnlyDefault   bool   `json:"read_only_default"`
}

func verifyRole(ctx context.Context, dsn, kind, schema, role string, schemaOID uint32, relations map[string]uint32, functionOID uint32, tenant, principal domain.ID) (roleCheck, error) {
	c := roleCheck{Role: role, Schema: schema, SchemaOID: schemaOID}
	s, e := persistence.Open(ctx, dsn)
	if e != nil {
		return c, e
	}
	defer s.Close()
	if kind == "api" {
		e = s.CheckAPIRole(ctx)
	} else if kind == "worker" {
		e = s.CheckWorkerRole(ctx)
	}
	if e != nil {
		return c, e
	}
	var db, current, session, ns string
	var oid uint32
	var super, createRole, createDB, createDBPermission, inherit, replication, member, createSchema bool
	var readOnly string
	e = s.Pool.QueryRow(ctx, `SELECT current_database(),current_user,session_user,current_schema(),n.oid,r.rolsuper,r.rolbypassrls,r.rolcreaterole,r.rolcreatedb,has_database_privilege(current_user,current_database(),'CREATE'),r.rolinherit,r.rolreplication,EXISTS(SELECT 1 FROM pg_auth_members m WHERE m.member=r.oid),EXISTS(SELECT 1 FROM pg_namespace x WHERE left(x.nspname,3)<>'pg_' AND has_schema_privilege(current_user,x.oid,'CREATE')),current_setting('default_transaction_read_only') FROM pg_roles r JOIN pg_namespace n ON n.nspname=current_schema() WHERE r.rolname=current_user`).Scan(&db, &current, &session, &ns, &oid, &super, &c.Bypass, &createRole, &createDB, &createDBPermission, &inherit, &replication, &member, &createSchema, &readOnly)
	if e != nil {
		return c, e
	}
	c.ReadOnlyDefault = readOnly == "on"
	if db != "forge" || current != role || session != role || ns != schema || oid != schemaOID || super || createRole || createDB || createDBPermission || inherit || replication || member || createSchema || c.Bypass != (kind == "worker") || (kind == "audit" && !c.ReadOnlyDefault) {
		return c, errors.New("role/catalog namespace authority mismatch")
	}
	// Fail closed if inherited PUBLIC ACLs expose any non-system table outside
	// this schema. Never fix other namespaces' grants to make this check pass.
	e = s.Pool.QueryRow(ctx, `SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relkind IN ('r','p','v','m','f') AND n.oid<>$1 AND n.nspname<>'information_schema' AND left(n.nspname,3)<>'pg_' AND has_schema_privilege(current_user,n.oid,'USAGE') AND has_table_privilege(current_user,c.oid,'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')`, schemaOID).Scan(&c.OutsidePrivileges)
	if e != nil {
		return c, e
	}
	if c.OutsidePrivileges != 0 {
		return c, errors.New("role exposes tables outside the new schema")
	}
	for name, id := range relations {
		var actual uint32
		var perms tablePrivileges
		e = s.Pool.QueryRow(ctx, `SELECT to_regclass($1)::oid,has_table_privilege(current_user,$2::oid,'SELECT'),has_table_privilege(current_user,$2::oid,'INSERT'),has_table_privilege(current_user,$2::oid,'UPDATE'),has_table_privilege(current_user,$2::oid,'DELETE'),has_table_privilege(current_user,$2::oid,'TRUNCATE,REFERENCES,TRIGGER'),pg_has_role(current_user,(SELECT relowner FROM pg_class WHERE oid=$2::oid),'MEMBER')`, pgx.Identifier{schema, name}.Sanitize(), id).Scan(&actual, &perms.Select, &perms.Insert, &perms.Update, &perms.Delete, &perms.Extra, &perms.Owner)
		if e != nil {
			return c, e
		}
		if actual != id || !validTablePrivileges(kind, name, perms) {
			return c, errors.New("exact table privileges differ")
		}

		c.Tables++
	}
	if e = s.Pool.QueryRow(ctx, `SELECT has_function_privilege(current_user,$1::oid,'EXECUTE')`, functionOID).Scan(&c.MembershipExecute); e != nil {
		return c, e
	}
	if c.MembershipExecute != (kind != "audit") {
		return c, errors.New("membership function execute grant differs")
	}
	tx, e := s.Tx(ctx, tenant, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if e != nil {
		return c, e
	}
	defer tx.Rollback(ctx)
	if kind != "audit" {
		if e = tx.QueryRow(ctx, "SELECT "+pgx.Identifier{schema}.Sanitize()+".lock_current_membership($1,$2)", tenant, principal).Scan(&c.MembershipRole); e != nil {
			return c, e
		}
		if c.MembershipRole != "admin" {
			return c, errors.New("schema-specific membership lock binding failed")
		}
	} else {
		var n int
		if e = tx.QueryRow(ctx, `SELECT count(*) FROM runs WHERE tenant_id=$1`, tenant).Scan(&n); e != nil {
			return c, e
		}
		if n != 0 {
			return c, errors.New("audit saw unexpected initial runs")
		}
	}
	return c, tx.Commit(ctx)
}
func expectedPrivileges(kind, table string) (bool, bool) {
	if kind == "audit" {
		return table != "api_tokens", false
	}
	if table == "goose_db_version" {
		return false, false
	}
	if table == "api_tokens" {
		return kind == "api", false
	}
	if table == "memberships" || table == "tenants" {
		return true, false
	}
	return true, true
}
func verifyFresh(ctx context.Context, s *persistence.Store, r provisionReport) error {
	var runs, projects, attempts, reservations, groups, tenants, members, tokens, active, maximum, slots, reserved int
	e := s.Pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM runs),(SELECT count(*) FROM projects),(SELECT count(*) FROM model_attempts),(SELECT count(*) FROM quota_reservations),(SELECT count(*) FROM provider_quotas),(SELECT count(*) FROM tenants),(SELECT count(*) FROM memberships),(SELECT count(*) FROM api_tokens),(SELECT active_count FROM tenant_runtime WHERE tenant_id=$1),(SELECT max_active FROM tenant_runtime WHERE tenant_id=$1),(SELECT max_slots FROM runners WHERE id=$2),(SELECT reserved_slots FROM runners WHERE id=$2)`, r.Tenant, r.RunnerID).Scan(&runs, &projects, &attempts, &reservations, &groups, &tenants, &members, &tokens, &active, &maximum, &slots, &reserved)
	if e != nil {
		return e
	}
	if runs != 0 || projects != 0 || attempts != 0 || reservations != 0 || groups != 1 || tenants != 1 || members != 1 || tokens != 1 || active != 0 || maximum != 1 || slots != 4 || reserved != 0 {
		return errors.New("new schema contains unexpected work or capacity")
	}
	return nil
}

type tablePrivileges struct{ Select, Insert, Update, Delete, Extra, Owner bool }

func validTablePrivileges(kind, table string, p tablePrivileges) bool {
	read, write := expectedPrivileges(kind, table)
	return !p.Owner && !p.Extra && p.Select == read && p.Insert == write && p.Update == write && p.Delete == write
}
