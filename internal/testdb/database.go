// Package testdb creates private schemas for integration fixtures without
// importing persistence, so persistence's own tests can use the same boundary.
package testdb

import (
	"context"
	"crypto/rand"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/db"
	"github.com/jackc/pgx/v5"
)

// New returns a migrated, private-schema DSN in the explicitly selected local
// test database. Callers may open multiple pools with this DSN to test shared
// state. Register pool cleanup after New so every pool closes before the schema
// is dropped. No public business table is read, migrated, or deleted here.
func New(t *testing.T) string {
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
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	name := "test_" + strings.ToLower(rand.Text())
	quoted := pgx.Identifier{name}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+quoted); err != nil {
		_ = admin.Close(ctx)
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, end := context.WithTimeout(context.Background(), 10*time.Second)
		defer end()
		if _, err := admin.Exec(cleanup, "DROP SCHEMA "+quoted+" CASCADE"); err != nil {
			t.Errorf("drop own test schema: %v", err)
		}
		_ = admin.Close(cleanup)
	})
	query := u.Query()
	query.Set("search_path", name)
	u.RawQuery = query.Encode()
	if err = db.Migrate(ctx, u.String()); err != nil {
		t.Fatal(err)
	}
	return u.String()
}
