package runner

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
)

func TestJournalIdentityStableAcrossReopenAndDistinctAcrossFiles(t *testing.T) {
	first := ""
	for _, name := range []string{"one", "two"} {
		filename := filepath.Join(t.TempDir(), name+".sqlite")
		j, err := openJournal(filename)
		if err != nil {
			t.Fatal(err)
		}
		id, err := j.identity(context.Background())
		if err != nil || len(id) != 64 {
			t.Fatalf("identity %q %v", id, err)
		}
		j.db.Close()
		j, err = openJournal(filename)
		if err != nil {
			t.Fatal(err)
		}
		again, err := j.identity(context.Background())
		j.db.Close()
		if err != nil || again != id {
			t.Fatal("identity changed across reopen")
		}
		if first == id {
			t.Fatal("distinct journals share an identity")
		}
		first = id
	}
}
func TestExistingV4NeverInventsMissingOrMalformedIdentity(t *testing.T) {
	for _, damage := range []string{"table", "row", "malformed"} {
		t.Run(damage, func(t *testing.T) {
			filename := filepath.Join(t.TempDir(), "journal.sqlite")
			j, err := openJournal(filename)
			if err != nil {
				t.Fatal(err)
			}
			statement := `DROP TABLE journal_identity`
			if damage == "row" {
				statement = `DELETE FROM journal_identity`
			}
			if damage == "malformed" {
				statement = `UPDATE journal_identity SET id='` + strings.Repeat("A", 64) + `'`
			}
			if _, err = j.db.Exec(statement); err != nil {
				t.Fatal(err)
			}
			j.db.Close()
			if reopened, err := openJournal(filename); err == nil {
				reopened.db.Close()
				t.Fatal("existing v4 identity silently repaired")
			}
		})
	}
}
func TestV3MigrationCreatesIdentityWithoutChangingLegacyDispatchEvidence(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "journal.sqlite")
	j, err := openJournal(filename)
	if err != nil {
		t.Fatal(err)
	}
	// Reconstruct the previous schema exactly for the fields the v4 migration owns.
	_, err = j.db.Exec(`DROP TABLE journal_identity; DROP TABLE runner_artifacts;
 ALTER TABLE operations DROP COLUMN dispatch_started;
 ALTER TABLE operations DROP COLUMN docker_start_intent;
 ALTER TABLE volume_leases DROP COLUMN epoch;
 ALTER TABLE volume_leases DROP COLUMN source_hash;
 PRAGMA user_version=3;`)
	if err != nil {
		t.Fatal(err)
	}
	j.db.Close()
	j, err = openJournal(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer j.db.Close()
	if _, err = j.identity(context.Background()); err != nil {
		t.Fatal(err)
	}
	rows, err := j.db.Query(`PRAGMA table_info(operations)`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	found := 0
	for rows.Next() {
		var cid, notnull, pk int
		var name, kind string
		var defaultValue sql.NullString
		if err = rows.Scan(&cid, &name, &kind, &notnull, &defaultValue, &pk); err != nil {
			t.Fatal(err)
		}
		if name == "dispatch_started" || name == "docker_start_intent" {
			found++
			if defaultValue.String != "1" {
				t.Fatalf("legacy %s default became unsafe: %s", name, defaultValue.String)
			}
		}
	}
	if found != 2 {
		t.Fatal("migration lacks dispatch proof fields")
	}
}
