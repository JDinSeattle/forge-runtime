package review_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/httpapi"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	flow "github.com/JDinSeattle/forge-runtime/internal/runtime"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// These are real HTTP/PG authorization tests, with synthetic control evidence.
// No runner, model, container or filesystem effect is started by this fixture.
func reviewApprovalWaiting(t *testing.T, ctx context.Context, owner *persistence.Store) persistence.Run {
	t.Helper()
	r := reviewRun(t, ctx, owner)
	var err error
	r, err = owner.ClaimOnRunner(ctx, "approval-authority-worker", time.Minute, "review_runner")
	if err != nil {
		t.Fatal(err)
	}
	ref := domain.ID("approval_authority_control_fixture")
	if err = owner.PublishArtifact(ctx, persistence.Artifact{TenantID: r.TenantID, RunID: r.ID, ID: ref, Kind: "control_fixture", ObjectKey: string(r.TenantID) + "/" + string(r.ID) + "/fixture", SHA256: strings.Repeat("0", 64)}); err != nil {
		t.Fatal(err)
	}
	args := json.RawMessage(`{"command":["fixture-never-executed"]}`)
	hash := sha256.Sum256(args)
	for _, event := range []flow.Event{
		{Kind: flow.EventWorkspaceReady, WorkspaceRevision: 1, OutputRef: string(ref)},
		{Kind: flow.EventContextBuilt, OutputRef: string(ref)},
		{Kind: flow.EventModelCompleted, Complete: true, OutputRef: string(ref)},
		{Kind: flow.EventToolsValidated, Complete: true, OutputRef: string(ref), Effects: []flow.Effect{{ID: "approval-authority-operation", Kind: "run_command", Args: args, ArgsHash: hex.EncodeToString(hash[:]), PolicyVersion: "approval-authority-v1", RequiresApproval: true}}},
	} {
		event.ExpectedVersion, event.Owner, event.Epoch = r.State.Version, r.State.Lease.Owner, r.State.Lease.Epoch
		r, err = owner.Advance(ctx, r.TenantID, r.ID, event)
		if err != nil {
			t.Fatalf("advance %s: %v", event.Kind, err)
		}
	}
	if r.State.Status != domain.StatusWaitingApproval || r.State.Approval == nil {
		t.Fatalf("fixture has no pending approval: %+v", r.State)
	}
	return r
}

func restrictApprovalAuthTables(t *testing.T, ctx context.Context, owner, api *persistence.Store) {
	t.Helper()
	var role string
	if err := api.Pool.QueryRow(ctx, `SELECT current_user`).Scan(&role); err != nil {
		t.Fatal(err)
	}
	// Match production auth-table permissions, instead of the generic review
	// fixture's broader business-table grants masking a membership-lock failure.
	if _, err := owner.Pool.Exec(ctx, `REVOKE INSERT,UPDATE,DELETE ON memberships,api_tokens,tenants FROM `+pgx.Identifier{role}.Sanitize()); err != nil {
		t.Fatal(err)
	}
}

type approvalHTTPReply struct {
	status int
	body   []byte
	err    error
}

func approvalRequest(ctx context.Context, server *httptest.Server, r persistence.Run, token string, allow bool) approvalHTTPReply {
	body, err := json.Marshal(map[string]any{"binding": r.State.Approval, "approve": allow})
	if err != nil {
		return approvalHTTPReply{err: err}
	}
	request, err := http.NewRequestWithContext(ctx, "POST", server.URL+"/v1/approvals/"+string(r.State.Approval.EffectID)+"_approval/decision", bytes.NewReader(body))
	if err != nil {
		return approvalHTTPReply{err: err}
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("X-Forge-Tenant", string(r.TenantID))
	request.Header.Set("Content-Type", "application/json")
	response, err := server.Client().Do(request)
	if err != nil {
		return approvalHTTPReply{err: err}
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	return approvalHTTPReply{status: response.StatusCode, body: raw, err: err}
}

func waitApprovalBlockedBy(t *testing.T, ctx context.Context, owner *persistence.Store, blocker uint32) {
	t.Helper()
	until := time.Now().Add(time.Second)
	for {
		var blocked bool
		if err := owner.Pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1::int=ANY(pg_blocking_pids(pid)))`, int64(blocker)).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked {
			return
		}
		if time.Now().After(until) {
			t.Fatal("HTTP approval did not reach the controlled database lock")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestReviewApprovalRejectsMembershipChangedAfterAuthentication(t *testing.T) {
	for _, change := range []string{"downgrade", "remove"} {
		t.Run(change, func(t *testing.T) {
			ctx, owner, api := reviewAPIStore(t)
			restrictApprovalAuthTables(t, ctx, owner, api)
			r := reviewApprovalWaiting(t, ctx, owner)
			token, err := owner.IssueToken(ctx, r.PrincipalID, time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer((&httpapi.Server{Store: api}).Handler())
			defer server.Close()
			blocker, err := owner.Pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer blocker.Rollback(context.Background())
			if _, err = blocker.Exec(ctx, `SELECT tenant_id FROM tenant_runtime WHERE tenant_id=$1 FOR UPDATE`, r.TenantID); err != nil {
				t.Fatal(err)
			}
			done := make(chan approvalHTTPReply, 1)
			go func() { done <- approvalRequest(ctx, server, r, token, true) }()
			// A wait inside Decide proves authentication already completed. No
			// guessed sleep or production hook fabricates that ordering.
			waitApprovalBlockedBy(t, ctx, owner, blocker.Conn().PgConn().PID())
			query := `UPDATE memberships SET role='viewer' WHERE tenant_id=$1 AND principal_id=$2`
			if change == "remove" {
				query = `DELETE FROM memberships WHERE tenant_id=$1 AND principal_id=$2`
			}
			if _, err = owner.Pool.Exec(ctx, query, r.TenantID, r.PrincipalID); err != nil {
				t.Fatal(err)
			}
			if err = blocker.Rollback(ctx); err != nil {
				t.Fatal(err)
			}
			reply := <-done
			var decisions, events int
			if err := owner.Pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM approvals WHERE tenant_id=$1 AND run_id=$2 AND decision IS NOT NULL),(SELECT count(*) FROM run_events WHERE tenant_id=$1 AND run_id=$2 AND type='approval.decided')`, r.TenantID, r.ID).Scan(&decisions, &events); err != nil {
				t.Fatal(err)
			}
			current, err := owner.GetRun(ctx, r.TenantID, r.ID)
			if err != nil {
				t.Fatal(err)
			}
			if reply.err != nil || reply.status != 403 || decisions != 0 || events != 0 || current.State.Version != r.State.Version || current.State.Status != domain.StatusWaitingApproval {
				t.Fatalf("stale authority after %s: HTTP=%d decisions=%d decision_events=%d state=%s version=%d want=%d err=%v", change, reply.status, decisions, events, current.State.Status, current.State.Version, r.State.Version, reply.err)
			}
			// Restored current authority may approve once; an exact duplicate
			// returns that same version, while changing allow to deny conflicts.
			if err := owner.BootstrapTenant(ctx, r.TenantID, r.PrincipalID, "developer"); err != nil {
				t.Fatal(err)
			}
			for i := range 2 {
				reply := approvalRequest(ctx, server, r, token, true)
				var approved persistence.Run
				if reply.err != nil || reply.status != 200 || json.Unmarshal(reply.body, &approved) != nil || approved.State.Version != r.State.Version+1 || approved.State.Status != domain.StatusQueued {
					t.Fatalf("valid approval %d: HTTP=%d body=%s err=%v", i, reply.status, reply.body, reply.err)
				}
			}
			if reply := approvalRequest(ctx, server, r, token, false); reply.err != nil || reply.status != 409 {
				t.Fatalf("conflicting duplicate accepted: %+v", reply)
			}
		})
	}
}

func TestReviewApprovalMembershipLockHelperIsScopedAndReadOnly(t *testing.T) {
	ctx, owner, api := reviewAPIStore(t)
	restrictApprovalAuthTables(t, ctx, owner, api)
	r := reviewRun(t, ctx, owner)
	var definer, sameOwner, publicExecute, canUpdate, canInsert, canDelete bool
	var settings []string
	err := owner.Pool.QueryRow(ctx, `SELECT p.prosecdef,p.proowner=(SELECT relowner FROM pg_class WHERE oid='memberships'::regclass),p.proconfig,
	 EXISTS(SELECT 1 FROM aclexplode(p.proacl) WHERE grantee=0 AND privilege_type='EXECUTE')
	 FROM pg_proc p WHERE p.oid='lock_current_membership(text,text)'::regprocedure`).Scan(&definer, &sameOwner, &settings, &publicExecute)
	if err != nil || !definer || !sameOwner || publicExecute || len(settings) != 1 || settings[0] != "search_path=pg_catalog, pg_temp" {
		t.Fatalf("unsafe helper metadata: definer=%t owner=%t public=%t settings=%v err=%v", definer, sameOwner, publicExecute, settings, err)
	}
	if err := api.Pool.QueryRow(ctx, `SELECT has_table_privilege('memberships','UPDATE'),has_table_privilege('memberships','INSERT'),has_table_privilege('memberships','DELETE')`).Scan(&canUpdate, &canInsert, &canDelete); err != nil || canUpdate || canInsert || canDelete {
		t.Fatalf("runtime gained membership write privilege: update=%t insert=%t delete=%t err=%v", canUpdate, canInsert, canDelete, err)
	}
	wantDenied := func(err error) {
		t.Helper()
		var pgerr *pgconn.PgError
		if !errors.As(err, &pgerr) || pgerr.Code != "42501" {
			t.Fatalf("expected permission refusal, got %v", err)
		}
	}
	_, err = api.Pool.Exec(ctx, `UPDATE memberships SET role='admin' WHERE tenant_id=$1 AND principal_id=$2`, r.TenantID, r.PrincipalID)
	wantDenied(err)
	var role *string
	err = api.Pool.QueryRow(ctx, `SELECT lock_current_membership($1,$2)`, r.TenantID, r.PrincipalID).Scan(&role)
	wantDenied(err) // No transaction-local tenant context.
	tx, err := api.Tx(ctx, r.TenantID, pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	err = tx.QueryRow(ctx, `SELECT lock_current_membership($1,$2)`, "different_tenant", r.PrincipalID).Scan(&role)
	wantDenied(err)
	_ = tx.Rollback(ctx)
	tx, err = api.Tx(ctx, r.TenantID, pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if err = tx.QueryRow(ctx, `SELECT lock_current_membership($1,$2)`, r.TenantID, "no_membership").Scan(&role); err != nil || role != nil {
		t.Fatalf("missing membership invented authority: role=%v err=%v", role, err)
	}
	// Even a caller-owned temporary relation cannot redirect the definer's
	// schema-qualified membership read into a forged admin record.
	if _, err = tx.Exec(ctx, `CREATE TEMP TABLE memberships(tenant_id text,principal_id text,role text) ON COMMIT DROP`); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO pg_temp.memberships VALUES($1,$2,'admin')`, r.TenantID, r.PrincipalID); err != nil {
		t.Fatal(err)
	}
	if err = tx.QueryRow(ctx, `SELECT lock_current_membership($1,$2)`, r.TenantID, r.PrincipalID).Scan(&role); err != nil || role == nil || *role != "developer" {
		t.Fatalf("helper followed temporary shadow: role=%v err=%v", role, err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var runtimeRole string
	if err = api.Pool.QueryRow(ctx, `SELECT current_user`).Scan(&runtimeRole); err != nil {
		t.Fatal(err)
	}
	if _, err = owner.Pool.Exec(ctx, `REVOKE EXECUTE ON FUNCTION lock_current_membership(text,text) FROM `+pgx.Identifier{runtimeRole}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	tx, err = api.Tx(ctx, r.TenantID, pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	err = tx.QueryRow(ctx, `SELECT lock_current_membership($1,$2)`, r.TenantID, r.PrincipalID).Scan(&role)
	wantDenied(err) // No inherited PUBLIC fallback after explicit grant removal.
}

func TestReviewApprovalMembershipRemainsLockedUntilDecisionCommit(t *testing.T) {
	ctx, owner, api := reviewAPIStore(t)
	restrictApprovalAuthTables(t, ctx, owner, api)
	r := reviewApprovalWaiting(t, ctx, owner)
	token, err := owner.IssueToken(ctx, r.PrincipalID, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer((&httpapi.Server{Store: api}).Handler())
	defer server.Close()
	blocker, err := owner.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback(context.Background())
	if _, err = blocker.Exec(ctx, `SELECT id FROM runs WHERE tenant_id=$1 AND id=$2 FOR UPDATE`, r.TenantID, r.ID); err != nil {
		t.Fatal(err)
	}
	done := make(chan approvalHTTPReply, 1)
	go func() { done <- approvalRequest(ctx, server, r, token, true) }()
	// Decide reaches the run lock only after acquiring its membership SHARE.
	waitApprovalBlockedBy(t, ctx, owner, blocker.Conn().PgConn().PID())
	changeCtx, cancel := context.WithTimeout(ctx, 80*time.Millisecond)
	_, err = owner.Pool.Exec(changeCtx, `UPDATE memberships SET role='viewer' WHERE tenant_id=$1 AND principal_id=$2`, r.TenantID, r.PrincipalID)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("membership changed between permission check and decision commit: %v", err)
	}
	if err = blocker.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	reply := <-done
	if reply.err != nil || reply.status != 200 {
		t.Fatalf("authorized decision did not finish: status=%d err=%v", reply.status, reply.err)
	}
	// Membership updates become possible after the decision commits.
	if _, err = owner.Pool.Exec(ctx, `UPDATE memberships SET role='viewer' WHERE tenant_id=$1 AND principal_id=$2`, r.TenantID, r.PrincipalID); err != nil {
		t.Fatal(err)
	}
	if reply := approvalRequest(ctx, server, r, token, true); reply.err != nil || reply.status != 403 {
		t.Fatalf("new request retained old role: status=%d err=%v", reply.status, reply.err)
	}
}

func TestReviewApprovalDenialAndTerminalDuplicatesKeepTheirSemantics(t *testing.T) {
	ctx, owner, api := reviewAPIStore(t)
	restrictApprovalAuthTables(t, ctx, owner, api)
	r := reviewApprovalWaiting(t, ctx, owner)
	if err := owner.BootstrapTenant(ctx, r.TenantID, r.PrincipalID, "admin"); err != nil {
		t.Fatal(err)
	}
	token, err := owner.IssueToken(ctx, r.PrincipalID, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer((&httpapi.Server{Store: api}).Handler())
	defer server.Close()
	for range 2 {
		reply := approvalRequest(ctx, server, r, token, false)
		var denied persistence.Run
		if reply.err != nil || reply.status != 200 || json.Unmarshal(reply.body, &denied) != nil || denied.State.Version != r.State.Version+1 || denied.State.Status != domain.StatusCancelRequested {
			t.Fatalf("denial changed semantics: status=%d body=%s err=%v", reply.status, reply.body, reply.err)
		}
	}
	if reply := approvalRequest(ctx, server, r, token, true); reply.err != nil || reply.status != 409 {
		t.Fatalf("changed decision accepted: status=%d err=%v", reply.status, reply.err)
	}
	stopping, err := owner.ClaimOnRunner(ctx, "authority-stop-worker", time.Minute, "review_runner")
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := owner.Advance(ctx, r.TenantID, r.ID, flow.Event{Kind: flow.EventCancellationConfirmed, ExpectedVersion: stopping.State.Version, Owner: stopping.State.Lease.Owner, Epoch: stopping.State.Lease.Epoch, Stop: &flow.StopReceipt{Ref: "approval_authority_control_fixture", NoActiveOperations: true, WorkspaceRevision: 1}})
	if err != nil || !terminal.State.Status.Terminal() {
		t.Fatalf("control fixture did not stop: %s %v", terminal.State.Status, err)
	}
	for range 2 {
		reply := approvalRequest(ctx, server, r, token, false)
		var duplicate persistence.Run
		if reply.err != nil || reply.status != 200 || json.Unmarshal(reply.body, &duplicate) != nil || duplicate.State.Version != terminal.State.Version || duplicate.State.Status != terminal.State.Status {
			t.Fatalf("terminal duplicate reopened run: status=%d body=%s err=%v", reply.status, reply.body, reply.err)
		}
	}
	var decisions, active, reserved int
	if err := owner.Pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM run_events WHERE run_id=$1 AND type='approval.decided'),(SELECT active_count FROM tenant_runtime WHERE tenant_id=$2),(SELECT reserved_slots FROM runners WHERE id='review_runner')`, r.ID, r.TenantID).Scan(&decisions, &active, &reserved); err != nil || decisions != 1 || active != 0 || reserved != 0 {
		t.Fatalf("duplicate decision changed accounting: events=%d active=%d reserved=%d err=%v", decisions, active, reserved, err)
	}
}
