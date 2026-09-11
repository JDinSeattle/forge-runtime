// Package testutil creates isolated schemas only in an explicitly selected
// disposable local database. No test truncates the operator's shared tables.
package testutil

import (
	"context"
	"crypto/rand"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/db"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	"github.com/jackc/pgx/v5"
)

func Database(t *testing.T) *persistence.Store {
	t.Helper()
	dsn := os.Getenv("FORGE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("requires FORGE_TEST_DATABASE_URL pointing to isolated loopback /forge")
	}
	u, err := url.Parse(dsn)
	if err != nil || u.Hostname() != "127.0.0.1" || u.Path != "/forge" {
		t.Fatal("refusing test schema outside loopback /forge database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := persistence.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	name := "test_" + strings.ToLower(rand.Text())
	quoted := pgx.Identifier{name}.Sanitize()
	if _, err = admin.Pool.Exec(ctx, "CREATE SCHEMA "+quoted); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := admin.Pool.Exec(ctx, "DROP SCHEMA "+quoted+" CASCADE"); err != nil {
			t.Errorf("drop own test schema: %v", err)
		}
		admin.Close()
	})
	query := u.Query()
	query.Set("search_path", name)
	u.RawQuery = query.Encode()
	if err = db.Migrate(ctx, u.String()); err != nil {
		t.Fatal(err)
	}
	s, err := persistence.Open(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s
}
