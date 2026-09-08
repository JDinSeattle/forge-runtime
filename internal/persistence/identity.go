package persistence

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/jackc/pgx/v5"
)

type Identity struct {
	TenantID    domain.ID `json:"tenant_id"`
	PrincipalID domain.ID `json:"principal_id"`
	Role        string    `json:"role"`
}

func (i Identity) CanWrite() bool { return i.Role == "admin" || i.Role == "developer" }
func (i Identity) IsAdmin() bool  { return i.Role == "admin" }
func tokenHash(token string) string {
	h := sha256.Sum256([]byte(token))
	return hex.EncodeToString(h[:])
}

// BootstrapTenant is for the operator process, never an unauthenticated API.
func (s *Store) BootstrapTenant(ctx context.Context, id, principal domain.ID, role string) error {
	if id.Validate() != nil || principal.Validate() != nil || (role != "admin" && role != "developer" && role != "viewer") {
		return domain.ErrInvalid
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `INSERT INTO tenants(id,name) VALUES($1,$1) ON CONFLICT DO NOTHING`, id); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO tenant_runtime(tenant_id) VALUES($1) ON CONFLICT DO NOTHING`, id); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO memberships(tenant_id,principal_id,role) VALUES($1,$2,$3) ON CONFLICT(tenant_id,principal_id) DO UPDATE SET role=excluded.role`, id, principal, role); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) IssueToken(ctx context.Context, principal domain.ID, lifetime time.Duration) (string, error) {
	if principal.Validate() != nil || lifetime < time.Minute || lifetime > 366*24*time.Hour {
		return "", domain.ErrInvalid
	}
	token := "frg_" + rand.Text() + rand.Text()
	_, err := s.Pool.Exec(ctx, `INSERT INTO api_tokens(token_hash,principal_id,expires_at) VALUES($1,$2,clock_timestamp()+($3*interval '1 second'))`, tokenHash(token), principal, int64(lifetime/time.Second))
	if err != nil {
		return "", err
	}
	return token, nil
}

func (s *Store) Authenticate(ctx context.Context, token string, tenant domain.ID) (Identity, error) {
	var i Identity
	if len(token) < 32 || len(token) > 256 || tenant.Validate() != nil {
		return i, domain.ErrForbidden
	}
	err := s.Pool.QueryRow(ctx, `SELECT m.tenant_id,m.principal_id,m.role FROM api_tokens t JOIN memberships m ON m.principal_id=t.principal_id WHERE t.token_hash=$1 AND m.tenant_id=$2 AND t.revoked_at IS NULL AND t.expires_at>clock_timestamp()`, tokenHash(token), tenant).Scan(&i.TenantID, &i.PrincipalID, &i.Role)
	if errors.Is(err, pgx.ErrNoRows) {
		return i, domain.ErrForbidden
	}
	return i, err
}

// CheckAPIRole refuses deployments that accidentally bypass the second layer
// of tenant isolation. Workers use a separate, controlled database identity.
func (s *Store) CheckAPIRole(ctx context.Context) error {
	var unsafe bool
	err := s.Pool.QueryRow(ctx, `WITH expected(name) AS (VALUES ('projects'),('runs'),('run_snapshots'),('run_messages'),('steps'),('model_attempts'),('effects'),('approvals'),('run_events'),('idempotency_keys'),('quota_reservations'),('artifacts'),('runner_allocations')) SELECT r.rolsuper OR r.rolbypassrls OR EXISTS(SELECT 1 FROM expected e LEFT JOIN pg_class c ON c.oid=to_regclass(e.name) WHERE c.oid IS NULL OR NOT c.relrowsecurity OR pg_has_role(current_user,c.relowner,'MEMBER')) FROM pg_roles r WHERE r.rolname=current_user`).Scan(&unsafe)
	if err != nil {
		return err
	}
	if unsafe {
		return fmt.Errorf("%w: API role owns business tables or bypasses RLS", domain.ErrForbidden)
	}
	return nil
}

// CheckWorkerRole requires an explicit cross-tenant service identity while
// refusing superuser/DDL ownership. API credentials must never use this role.
func (s *Store) CheckWorkerRole(ctx context.Context) error {
	var allowed bool
	err := s.Pool.QueryRow(ctx, `SELECT NOT rolsuper AND rolbypassrls AND NOT rolcreaterole AND NOT rolcreatedb AND NOT pg_has_role(current_user,(SELECT relowner FROM pg_class WHERE oid=to_regclass('runs')),'MEMBER') FROM pg_roles WHERE rolname=current_user`).Scan(&allowed)
	if err != nil {
		return err
	}
	if !allowed {
		return fmt.Errorf("%w: worker requires a nonowner, non-superuser service role with explicit BYPASSRLS", domain.ErrForbidden)
	}
	return nil
}

type Project struct {
	TenantID  domain.ID `json:"tenant_id"`
	ID        domain.ID `json:"id"`
	Name      string    `json:"name"`
	SourceID  string    `json:"source_id"`
	ProfileID string    `json:"profile_id"`
	CreatedAt time.Time `json:"created_at"`
}

func (s *Store) CreateProject(ctx context.Context, tenant domain.ID, name, sourceID, profileID string) (Project, error) {
	if name == "" || len(name) > 128 || sourceID == "" || profileID == "" {
		return Project{}, domain.ErrInvalid
	}
	p := Project{TenantID: tenant, ID: NewID("project"), Name: name, SourceID: sourceID, ProfileID: profileID}
	tx, err := s.Tx(ctx, tenant, pgx.TxOptions{})
	if err != nil {
		return p, err
	}
	defer tx.Rollback(ctx)
	profile, _ := json.Marshal(map[string]string{"id": profileID})
	err = tx.QueryRow(ctx, `INSERT INTO projects(tenant_id,id,name,repo_source,repo_profile) VALUES($1,$2,$3,$4,$5) RETURNING created_at`, tenant, p.ID, name, sourceID, profile).Scan(&p.CreatedAt)
	if err != nil {
		return p, err
	}
	return p, tx.Commit(ctx)
}
func (s *Store) GetProject(ctx context.Context, tenant, id domain.ID) (Project, error) {
	tx, err := s.Tx(ctx, tenant, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return Project{}, err
	}
	defer tx.Rollback(ctx)
	var p Project
	err = tx.QueryRow(ctx, `SELECT tenant_id,id,name,repo_source,repo_profile->>'id',created_at FROM projects WHERE tenant_id=$1 AND id=$2`, tenant, id).Scan(&p.TenantID, &p.ID, &p.Name, &p.SourceID, &p.ProfileID, &p.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return p, domain.ErrNotFound
	}
	if err != nil {
		return p, err
	}
	return p, tx.Commit(ctx)
}

func (s *Store) RegisterRunner(ctx context.Context, id, endpoint string, slots int) error {
	if id == "" || endpoint == "" || slots < 1 || slots > 1024 {
		return domain.ErrInvalid
	}
	_, err := s.Pool.Exec(ctx, `INSERT INTO runners(id,endpoint,max_slots) VALUES($1,$2,$3) ON CONFLICT(id) DO UPDATE SET endpoint=excluded.endpoint,max_slots=excluded.max_slots`, id, endpoint, slots)
	return err
}
