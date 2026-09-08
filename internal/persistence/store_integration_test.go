package persistence

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/db"
	"github.com/JDinSeattle/forge-runtime/internal/domain"
	flow "github.com/JDinSeattle/forge-runtime/internal/runtime"
)

func integrationStore(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("FORGE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set FORGE_TEST_DATABASE_URL to isolated local Forge database")
	}
	u, err := url.Parse(dsn)
	if err != nil || u.Hostname() != "127.0.0.1" || u.Path != "/forge" {
		t.Fatal("integration fixtures require loopback /forge database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err = db.Migrate(ctx, dsn); err != nil {
		t.Fatal(err)
	}
	s, err := Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s
}
func fixture(t *testing.T, s *Store) (Identity, Project, Config) {
	t.Helper()
	ctx := context.Background()
	i := Identity{TenantID: NewID("test"), PrincipalID: NewID("person"), Role: "admin"}
	if err := s.BootstrapTenant(ctx, i.TenantID, i.PrincipalID, i.Role); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		// Fixtures are uniquely scoped; never truncate a shared database.
		for _, table := range []string{"artifacts", "quota_reservations", "idempotency_keys", "run_events", "approvals", "effects", "model_attempts", "steps", "run_messages", "run_snapshots", "runner_allocations", "runs", "projects", "memberships", "tenant_runtime"} {
			if _, err := s.Pool.Exec(context.Background(), "DELETE FROM "+table+" WHERE tenant_id=$1", i.TenantID); err != nil {
				t.Errorf("cleanup %s: %v", table, err)
			}
		}
		_, _ = s.Pool.Exec(context.Background(), `DELETE FROM api_tokens WHERE principal_id=$1`, i.PrincipalID)
		_, _ = s.Pool.Exec(context.Background(), `DELETE FROM tenants WHERE id=$1`, i.TenantID)
	})
	p, err := s.CreateProject(ctx, i.TenantID, "fixture", "fixture-python", "python")
	if err != nil {
		t.Fatal(err)
	}
	return i, p, Config{Provider: "fake", Model: "scripted", MaxModelRounds: 8, MaxToolCalls: 20, MaxCost: 1_000_000, MaxRuntimeSeconds: 300}
}
func submitFixture(t *testing.T, s *Store, i Identity, p Project, c Config, key string) Run {
	t.Helper()
	r, _, err := s.Submit(context.Background(), SubmitRequest{TenantID: i.TenantID, PrincipalID: i.PrincipalID, ProjectID: p.ID, Task: "repair fixture", BaseCommit: "fixture-v1", Config: c}, key)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func TestConcurrentSubmitAndCursor(t *testing.T) {
	s := integrationStore(t)
	i, p, c := fixture(t, s)
	ctx := context.Background()
	req := SubmitRequest{TenantID: i.TenantID, PrincipalID: i.PrincipalID, ProjectID: p.ID, Task: "repair fixture", BaseCommit: "fixture-v1", Config: c}
	const n = 40
	ids := make(chan domain.ID, n)
	errs := make(chan error, n)
	var wg sync.WaitGroup
	for range n {
		wg.Go(func() {
			r, _, err := s.Submit(ctx, req, "same")
			if err != nil {
				errs <- err
			} else {
				ids <- r.ID
			}
		})
	}
	wg.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	var id domain.ID
	for got := range ids {
		if id != "" && got != id {
			t.Fatal("duplicate logical runs")
		}
		id = got
	}
	req.Task = "different"
	if _, _, err := s.Submit(ctx, req, "same"); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("mismatched idempotency %v", err)
	}
	r, err := s.GetRun(ctx, i.TenantID, id)
	if err != nil {
		t.Fatal(err)
	}
	if r.CoveredSeq != 1 || r.State.Version != 1 {
		t.Fatalf("non-atomic initial state %+v", r)
	}
	if _, err = s.Events(ctx, i.TenantID, id, r.CoveredSeq+1, 10); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("future cursor: %v", err)
	}
	other, _, _ := fixture(t, s)
	if _, err = s.GetRun(ctx, other.TenantID, id); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("cross tenant read: %v", err)
	}
	if _, err = s.Events(ctx, other.TenantID, id, 0, 10); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("cross tenant events: %v", err)
	}
}

func TestClaimFencingAndCapacity(t *testing.T) {
	s := integrationStore(t)
	i, p, c := fixture(t, s)
	ctx := context.Background()
	rid := string(NewID("runner"))
	if err := s.RegisterRunner(ctx, rid, "unix:///tmp/test-only.sock", 2); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = s.Pool.Exec(context.Background(), `DELETE FROM runners WHERE id=$1`, rid) })
	// Force these tasks onto the fixture runner; no other suite's runner is used.
	for n := range 3 {
		r := submitFixture(t, s, i, p, c, fmt.Sprint(n))
		if _, err := s.Pool.Exec(ctx, `UPDATE runs SET runner_id=$3 WHERE tenant_id=$1 AND id=$2`, i.TenantID, r.ID, rid); err != nil {
			t.Fatal(err)
		}
	}
	claims := make(chan Run, 2)
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for n := range 2 {
		wg.Go(func() {
			r, err := s.Claim(ctx, fmt.Sprintf("worker-%d", n), time.Second)
			if err != nil {
				errs <- err
			} else {
				claims <- r
			}
		})
	}
	wg.Wait()
	close(claims)
	close(errs)
	// SKIP LOCKED is allowed to return empty on contention; poll boundedly.
	var first Run
	count := 0
	for r := range claims {
		first = r
		count++
	}
	for err := range errs {
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatal(err)
		}
	}
	for count < 2 {
		r, err := s.Claim(ctx, "retry", time.Second)
		if err != nil {
			t.Fatal(err)
		}
		first = r
		count++
	}
	if _, err := s.Claim(ctx, "overflow", time.Second); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("capacity overflow: %v", err)
	}
	var active, slots int
	if err := s.Pool.QueryRow(ctx, `SELECT active_count FROM tenant_runtime WHERE tenant_id=$1`, i.TenantID).Scan(&active); err != nil {
		t.Fatal(err)
	}
	if active != 2 {
		t.Fatalf("active=%d", active)
	}
	if err := s.Pool.QueryRow(ctx, `SELECT reserved_slots FROM runners WHERE id=$1`, rid).Scan(&slots); err != nil {
		t.Fatal(err)
	}
	if slots != 2 {
		t.Fatalf("slots=%d", slots)
	}
	if _, err := s.Pool.Exec(ctx, `UPDATE runs SET lease_until=clock_timestamp()-interval '1 second' WHERE tenant_id=$1 AND id=$2`, i.TenantID, first.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Heartbeat(ctx, i.TenantID, first.ID, first.State.Lease.Owner, first.State.Lease.Epoch, time.Second); !errors.Is(err, domain.ErrFenced) {
		t.Fatalf("expired heartbeat: %v", err)
	}
	recovered, err := s.Claim(ctx, "successor", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.ID != first.ID || recovered.State.Lease.Epoch != first.State.Lease.Epoch+1 {
		t.Fatalf("bad takeover %+v", recovered)
	}
	_, err = s.Advance(ctx, i.TenantID, first.ID, flow.Event{Kind: flow.EventFailed, ExpectedVersion: first.State.Version, Owner: first.State.Lease.Owner, Epoch: first.State.Lease.Epoch, Reason: "late"})
	if !errors.Is(err, domain.ErrConflict) && !errors.Is(err, domain.ErrFenced) {
		t.Fatalf("old writer accepted: %v", err)
	}
}

func TestControlAndReadyEvidence(t *testing.T) {
	s := integrationStore(t)
	i, p, c := fixture(t, s)
	ctx := context.Background()
	r := submitFixture(t, s, i, p, c, "control")
	viewer := i
	viewer.Role = "viewer"
	if _, err := s.Cancel(ctx, viewer, r.ID); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("viewer cancel: %v", err)
	}
	m, reused, err := s.AddMessage(ctx, i, r.ID, "preserve tests", "msg1")
	if err != nil || reused {
		t.Fatalf("message %v", err)
	}
	duplicate, reused, err := s.AddMessage(ctx, i, r.ID, "preserve tests", "msg1")
	if err != nil || !reused || duplicate.Seq != m.Seq {
		t.Fatalf("duplicate message %v", err)
	}
	r, err = s.Cancel(ctx, i, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	v := r.State.Version
	r, err = s.Cancel(ctx, i, r.ID)
	if err != nil || r.State.Version != v {
		t.Fatalf("cancel not idempotent %v", err)
	}
	if r.State.Status != domain.StatusCancelRequested {
		t.Fatal("cancelled before runner ack")
	}
	// An invented receipt cannot justify advancing the durable state.
	_, err = s.Advance(ctx, i.TenantID, r.ID, flow.Event{Kind: flow.EventCancellationConfirmed, ExpectedVersion: r.State.Version, Stop: &flow.StopReceipt{Ref: "invented", NoActiveOperations: true}})
	if !errors.Is(err, domain.ErrUntrusted) {
		t.Fatalf("unpublished evidence accepted: %v", err)
	}
}
