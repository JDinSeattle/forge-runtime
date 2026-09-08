package review_test

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/db"
	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	flow "github.com/JDinSeattle/forge-runtime/internal/runtime"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store methods commit their own transactions, so these tests use a private
// schema rather than the rollback-only fixtures in schema_test.go. The schema
// is uniquely named, migrated independently, and dropped on test cleanup.
func isolatedStore(t *testing.T) (context.Context, *persistence.Store) {
	t.Helper()
	ctx, admin := reviewDB(t)
	schema := "review_" + strings.ToLower(rand.Text())
	identifier := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+identifier); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := admin.Exec(cleanupCtx, `DROP SCHEMA `+identifier+` CASCADE`); err != nil {
			t.Errorf("clean up independent review schema: %v", err)
		}
	})
	u, err := url.Parse(os.Getenv("FORGE_REVIEW_DATABASE_URL"))
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		t.Fatal("Store review tests require a PostgreSQL URL")
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	if err := db.Migrate(ctx, u.String()); err != nil {
		t.Fatal(err)
	}
	s, err := persistence.Open(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return ctx, s
}

func reviewRun(t *testing.T, ctx context.Context, s *persistence.Store) persistence.Run {
	t.Helper()
	if err := s.BootstrapTenant(ctx, "review_tenant", "review_principal", "developer"); err != nil {
		t.Fatal(err)
	}
	if err := s.RegisterRunner(ctx, "review_runner", "http://runner.invalid", 2); err != nil {
		t.Fatal(err)
	}
	p, err := s.CreateProject(ctx, "review_tenant", "Review", "local-fixture", "python")
	if err != nil {
		t.Fatal(err)
	}
	r, _, err := s.Submit(ctx, persistence.SubmitRequest{
		TenantID: "review_tenant", PrincipalID: "review_principal", ProjectID: p.ID,
		Task: "test control-plane lease authority", BaseCommit: "fixed-fixture-base",
		Config: persistence.Config{Provider: "fake", Model: "fixture", MaxModelRounds: 2,
			MaxToolCalls: 3, MaxCost: domain.USD, MaxRuntimeSeconds: 120},
	}, "review-submit")
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestReviewHeartbeatRemainsAuthoritativeAcrossTransitions(t *testing.T) {
	ctx, s := isolatedStore(t)
	reviewRun(t, ctx, s)
	r, err := s.Claim(ctx, "review_worker", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	originalExpiry := r.State.Lease.Until
	renewedExpiry, err := s.Heartbeat(ctx, r.TenantID, r.ID, r.State.Lease.Owner, r.State.Lease.Epoch, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	r, err = s.GetRun(ctx, r.TenantID, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !r.State.Lease.Until.Equal(renewedExpiry) {
		t.Errorf("GetRun reports stale snapshot lease %s instead of renewed DB lease %s", r.State.Lease.Until, renewedExpiry)
	}
	// This is control-plane evidence metadata only; container/artifact-byte
	// integrity is deliberately outside this test's claim.
	const receiptID = domain.ID("review_workspace_receipt")
	if err := s.PublishArtifact(ctx, persistence.Artifact{
		TenantID: r.TenantID, RunID: r.ID, ID: receiptID, Kind: "workspace_receipt",
		ObjectKey: string(r.TenantID) + "/" + string(r.ID) + "/receipt",
		SHA256:    strings.Repeat("0", 64), ByteSize: 0,
	}); err != nil {
		t.Fatal(err)
	}
	r, err = s.Advance(ctx, r.TenantID, r.ID, flow.Event{
		Kind: flow.EventWorkspaceReady, ExpectedVersion: r.State.Version,
		Owner: r.State.Lease.Owner, Epoch: r.State.Lease.Epoch,
		WorkspaceRevision: 1, OutputRef: string(receiptID),
	})
	if err != nil {
		t.Fatal(err)
	}
	proof, _, err := s.LeaseProof(ctx, r.TenantID, r.ID, r.State.Lease.Owner, r.State.Lease.Epoch)
	if err != nil {
		t.Fatal(err)
	}
	if !proof.Until.Equal(renewedExpiry) {
		t.Errorf("Advance shortened the renewed lease from %s to %s", renewedExpiry, proof.Until)
	}
	if delay := time.Until(originalExpiry) + 50*time.Millisecond; delay > 0 {
		time.Sleep(delay)
	}
	if _, err := s.Advance(ctx, r.TenantID, r.ID, flow.Event{
		Kind: flow.EventContextBuilt, ExpectedVersion: r.State.Version,
		Owner: r.State.Lease.Owner, Epoch: r.State.Lease.Epoch, OutputRef: string(receiptID),
	}); err != nil {
		t.Fatalf("renewed owner could not advance after original lease expiry: %v", err)
	}
}

func TestReviewLeaseExpiryDuringEvidenceWaitFencesCommit(t *testing.T) {
	ctx, s := isolatedStore(t)
	reviewRun(t, ctx, s)
	r, err := s.Claim(ctx, "review_worker", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	const receiptID = domain.ID("review_workspace_receipt")
	if err := s.PublishArtifact(ctx, persistence.Artifact{
		TenantID: r.TenantID, RunID: r.ID, ID: receiptID, Kind: "workspace_receipt",
		ObjectKey: string(r.TenantID) + "/" + string(r.ID) + "/receipt",
		SHA256:    strings.Repeat("0", 64), ByteSize: 0,
	}); err != nil {
		t.Fatal(err)
	}
	before, err := s.GetRun(ctx, r.TenantID, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	blocker, err := s.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback(context.Background())
	if _, err := blocker.Exec(ctx, `LOCK TABLE artifacts IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := s.Advance(ctx, r.TenantID, r.ID, flow.Event{Kind: flow.EventWorkspaceReady,
			ExpectedVersion: r.State.Version, Owner: r.State.Lease.Owner, Epoch: r.State.Lease.Epoch,
			WorkspaceRevision: 1, OutputRef: string(receiptID)})
		done <- err
	}()
	// Wait for the actual PostgreSQL lock wait, not a guessed goroutine delay.
	for {
		var waiting bool
		if err := s.Pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE relation='artifacts'::regclass AND mode='AccessShareLock' AND NOT granted)`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("Advance returned before reaching evidence-lock wait: %v", err)
		case <-ctx.Done():
			t.Fatal("Advance did not reach evidence-lock wait")
		case <-time.After(time.Millisecond):
		}
	}
	if delay := time.Until(r.State.Lease.Until) + 50*time.Millisecond; delay > 0 {
		time.Sleep(delay)
	}
	if err := blocker.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, domain.ErrFenced) {
			t.Fatalf("expired owner committed after waiting for evidence: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("Advance did not finish after evidence lock released")
	}
	after, err := s.GetRun(ctx, r.TenantID, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.State.Version != before.State.Version || after.State.Stage != before.State.Stage || after.CoveredSeq != before.CoveredSeq {
		t.Fatal("fenced transaction left state or event side effects")
	}
}

func TestReviewDeferredModelRunCanBeReclaimed(t *testing.T) {
	ctx, s := isolatedStore(t)
	reviewRun(t, ctx, s)
	r, err := s.Claim(ctx, "review_worker", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	const receiptID = domain.ID("review_defer_receipt")
	if err := s.PublishArtifact(ctx, persistence.Artifact{
		TenantID: r.TenantID, RunID: r.ID, ID: receiptID, Kind: "workspace_receipt",
		ObjectKey: string(r.TenantID) + "/" + string(r.ID) + "/defer-receipt",
		SHA256:    strings.Repeat("0", 64), ByteSize: 0,
	}); err != nil {
		t.Fatal(err)
	}
	for _, event := range []flow.Event{
		{Kind: flow.EventWorkspaceReady, WorkspaceRevision: 1, OutputRef: string(receiptID)},
		{Kind: flow.EventContextBuilt, OutputRef: string(receiptID)},
	} {
		event.ExpectedVersion, event.Owner, event.Epoch = r.State.Version, r.State.Lease.Owner, r.State.Lease.Epoch
		r, err = s.Advance(ctx, r.TenantID, r.ID, event)
		if err != nil {
			t.Fatal(err)
		}
	}
	originalEpoch, originalVersion := r.State.Lease.Epoch, r.State.Version
	if err := s.Defer(ctx, r, time.Now().Add(-time.Second), "quota.delayed"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.LeaseProof(ctx, r.TenantID, r.ID, r.State.Lease.Owner, originalEpoch); !errors.Is(err, domain.ErrFenced) {
		t.Fatalf("deferred owner still has an active lease: %v", err)
	}
	reclaimed, err := s.Claim(ctx, "review_replacement", 10*time.Second)
	if err != nil {
		t.Fatalf("deferred running snapshot cannot be reclaimed: %v", err)
	}
	if reclaimed.ID != r.ID || reclaimed.State.Lease.Owner != "review_replacement" || reclaimed.State.Lease.Epoch != originalEpoch+1 || reclaimed.State.Version <= originalVersion {
		t.Fatalf("reclaim did not fence the old owner: %+v", reclaimed.State.Lease)
	}
	if reclaimed.State.Stage != domain.StageAdoptWorkspace {
		t.Fatalf("reclaim skipped workspace adoption: %s", reclaimed.State.Stage)
	}
	if reclaimed.State.Cost != r.State.Cost || reclaimed.State.StepSeq != r.State.StepSeq {
		t.Fatal("defer/reclaim altered completed model accounting")
	}
}

func TestReviewAPIRoleGuardChecksResolvedSchemaAndInheritedOwnership(t *testing.T) {
	// Register role cleanup before the isolated schema cleanup, so no fixture
	// objects remain owned by these unique NOLOGIN roles when they are dropped.
	ctx, admin := reviewDB(t)
	ownerRole := "review_owner_" + strings.ToLower(rand.Text())
	apiRole := "review_api_" + strings.ToLower(rand.Text())
	for _, role := range []string{ownerRole, apiRole} {
		identifier := pgx.Identifier{role}.Sanitize()
		if _, err := admin.Exec(ctx, `CREATE ROLE `+identifier+` NOLOGIN NOSUPERUSER NOBYPASSRLS INHERIT`); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, err := admin.Exec(cleanupCtx, `DROP ROLE `+identifier); err != nil {
				t.Errorf("remove independent role fixture: %v", err)
			}
		})
	}
	_, store := isolatedStore(t)
	var schema string
	if err := store.Pool.QueryRow(ctx, `SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Pool.Exec(ctx, `GRANT USAGE ON SCHEMA `+pgx.Identifier{schema}.Sanitize()+` TO `+pgx.Identifier{apiRole}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	config := store.Pool.Config()
	config.MaxConns, config.MinConns = 1, 0
	config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, `SET ROLE `+pgx.Identifier{apiRole}.Sanitize())
		return err
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	api := &persistence.Store{Pool: pool}
	if err := api.CheckAPIRole(ctx); err != nil {
		t.Fatalf("safe non-owner fixture role rejected: %v", err)
	}
	if _, err := store.Pool.Exec(ctx, `ALTER TABLE projects OWNER TO `+pgx.Identifier{ownerRole}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, `GRANT `+pgx.Identifier{ownerRole}.Sanitize()+` TO `+pgx.Identifier{apiRole}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	if err := api.CheckAPIRole(ctx); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("API guard accepted inherited ownership of resolved projects relation: %v", err)
	}
}

func TestReviewHTMLCharactersPreserveToolArgumentHashAcrossPersistence(t *testing.T) {
	ctx, s := isolatedStore(t)
	reviewRun(t, ctx, s)
	r, err := s.Claim(ctx, "review_worker", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	const ref = domain.ID("review_html_receipt")
	if err := s.PublishArtifact(ctx, persistence.Artifact{TenantID: r.TenantID, RunID: r.ID,
		ID: ref, Kind: "review_fixture", ObjectKey: string(r.TenantID) + "/" + string(r.ID) + "/html",
		SHA256: strings.Repeat("0", 64), ByteSize: 0}); err != nil {
		t.Fatal(err)
	}
	args := json.RawMessage(`{"path":"x < y & z > w.py","end_line":9007199254740993}`)
	sum := sha256.Sum256(args)
	effect := flow.Effect{ID: "review_html_operation", Kind: "read_file", Args: args,
		ArgsHash: hex.EncodeToString(sum[:]), ExpectedRevision: 1, PolicyVersion: "workspace-v1", Status: flow.EffectPlanned}
	for _, event := range []flow.Event{
		{Kind: flow.EventWorkspaceReady, WorkspaceRevision: 1, OutputRef: string(ref)},
		{Kind: flow.EventContextBuilt, OutputRef: string(ref)},
		{Kind: flow.EventModelCompleted, Complete: true, OutputRef: string(ref)},
	} {
		event.ExpectedVersion, event.Owner, event.Epoch = r.State.Version, r.State.Lease.Owner, r.State.Lease.Epoch
		r, err = s.Advance(ctx, r.TenantID, r.ID, event)
		if err != nil {
			t.Fatal(err)
		}
	}
	event := flow.Event{Kind: flow.EventToolsValidated, Complete: true, OutputRef: string(ref), Effects: []flow.Effect{effect},
		ExpectedVersion: r.State.Version, Owner: r.State.Lease.Owner, Epoch: r.State.Lease.Epoch}
	if _, err := s.Advance(ctx, r.TenantID, r.ID, event); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("noncanonical accepted arguments would change on JSON encoding: %v", err)
	}
	unchanged, err := s.GetRun(ctx, r.TenantID, r.ID)
	if err != nil || unchanged.State.Version != r.State.Version || unchanged.CoveredSeq != r.CoveredSeq {
		t.Fatalf("rejected arguments left durable side effects: %v", err)
	}
	// This fixed literal is independent of the implementation canonicalizer.
	// Canonical hashing must happen before effect planning, never after dispatch.
	want := json.RawMessage(`{"end_line":9007199254740993,"path":"x \u003c y \u0026 z \u003e w.py"}`)
	args, err = domain.CanonicalJSON(args)
	if err != nil || string(args) != string(want) {
		t.Fatalf("canonicalization changed a large integer or escaping: %s, %v", args, err)
	}
	sum = sha256.Sum256(args)
	event.Effects[0].Args = args
	event.Effects[0].ArgsHash = hex.EncodeToString(sum[:])
	r, err = s.Advance(ctx, r.TenantID, r.ID, event)
	if err != nil {
		t.Fatal(err)
	}
	if r.State.PendingEffect == nil {
		t.Fatal("planned tool disappeared")
	}
	persistedSum := sha256.Sum256(r.State.PendingEffect.Args)
	if hex.EncodeToString(persistedSum[:]) != r.State.PendingEffect.ArgsHash {
		t.Fatalf("snapshot JSON changed hashed tool bytes: before=%s after=%s", args, r.State.PendingEffect.Args)
	}
	if string(r.State.PendingEffect.Args) != string(want) || len(r.Commands) != 1 || r.Commands[0].Effect == nil || string(r.Commands[0].Effect.Args) != string(want) {
		t.Fatal("canonical argument bytes changed in the snapshot or command envelope")
	}
}
