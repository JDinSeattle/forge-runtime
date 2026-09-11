package persistence_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/dependency"
	"github.com/JDinSeattle/forge-runtime/internal/quota"
	"github.com/JDinSeattle/forge-runtime/internal/testutil"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// pgx releases a canceled connection via puddle.Resource.Destroy, whose
// destructor and pool accounting run asynchronously. Still require all
// resources back within a fixed short window, rather than sampling that race.
func requirePoolReleased(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	started := time.Now()
	until := started.Add(time.Second)
	for pool.Stat().AcquiredConns() != 0 && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	if got := pool.Stat().AcquiredConns(); got != 0 {
		t.Fatalf("pool retained %d acquired connections after bounded cleanup", got)
	}
	t.Logf("pool acquired=0 after %s cleanup observation", time.Since(started))
}

func TestDatabaseDefaultDeadlineReleasesBlockedRead(t *testing.T) {
	s := testutil.Database(t)
	blocker, err := s.Pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback(context.Background())
	if _, err = blocker.Exec(context.Background(), `LOCK TABLE runs IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	_, err = s.GetRun(context.Background(), "tenant", "run")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked read=%v", err)
	}
	if elapsed := time.Since(started); elapsed < dependency.DatabaseTimeout-time.Second || elapsed > dependency.DatabaseTimeout+2*time.Second {
		t.Fatalf("wrong default budget: %s", elapsed)
	}
	if err = blocker.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err = s.DatabaseTime(context.Background()); err != nil {
		t.Fatalf("pool unusable after canceled read: %v", err)
	}
	requirePoolReleased(t, s.Pool)
}

func TestDatabaseDeadlineBoundsWorkerPoolAcquisition(t *testing.T) {
	s := testutil.Database(t)
	var held []*pgxpool.Conn
	defer func() {
		for _, c := range held {
			c.Release()
		}
	}()
	for range int(s.Pool.Config().MaxConns) {
		c, err := s.Pool.Acquire(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, c)
	}
	started := time.Now()
	_, err := s.Claim(context.Background(), "bounded-worker", 3*time.Second)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("pool acquire was unbounded: %v", err)
	}
	if elapsed := time.Since(started); elapsed < dependency.DatabaseTimeout-time.Second || elapsed > dependency.DatabaseTimeout+2*time.Second {
		t.Fatalf("wrong acquire budget: %s", elapsed)
	}
	for _, c := range held {
		c.Release()
	}
	held = nil
	if _, err = s.DatabaseTime(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestTransactionDeadlineCannotResetAndRollbackUsesCleanupContext(t *testing.T) {
	s := testutil.Database(t)
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	tx, err := s.Tx(ctx, "tenant", pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	// The next caller supplies Background, but cannot extend the transaction's
	// original absolute deadline. A canceled caller must still release the pool.
	_, err = tx.Exec(context.Background(), `SELECT pg_sleep(5)`)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("transaction deadline reset: %v", err)
	}
	_ = tx.Rollback(ctx)
	requirePoolReleased(t, s.Pool)
	if _, err = s.DatabaseTime(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestQuotaLockDeadlineLeavesLedgerUsable(t *testing.T) {
	s := testutil.Database(t)
	q := quota.New(s.Pool)
	cfg := quota.Config{CredentialGroup: "bounded", MaxConcurrent: 1, MaxTokens: 100, MaxCost: 100, WindowDuration: time.Minute, FailureThreshold: 3, BreakerCooldown: time.Second}
	if err := q.Configure(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	tx, err := s.Pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(context.Background(), `SELECT credential_group FROM provider_quotas FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err = q.Configure(ctx, cfg); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("quota lock wait=%v", err)
	}
	if err = tx.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = q.Configure(context.Background(), cfg); err != nil {
		t.Fatalf("quota could not recover: %v", err)
	}
}
