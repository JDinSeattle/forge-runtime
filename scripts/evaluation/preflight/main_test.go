package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/JDinSeattle/forge-runtime/internal/sandbox"
)

func sqliteFixture(t *testing.T) (runnerConfig, string) {
	t.Helper()
	root := t.TempDir()
	c := runnerConfig{JournalPath: filepath.Join(root, "journal.sqlite"), ArtifactRoot: filepath.Join(root, "artifacts")}
	if err := os.Mkdir(c.ArtifactRoot, 0700); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", c.JournalPath)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`PRAGMA user_version=5;
 CREATE TABLE journal_identity(singleton INTEGER PRIMARY KEY,id TEXT);
 CREATE TABLE operations(id TEXT PRIMARY KEY,status TEXT);
 CREATE TABLE workspaces(id TEXT, released INTEGER,active_operation TEXT);
 CREATE TABLE volume_leases(id TEXT,released INTEGER);
 CREATE TABLE operation_logs(operation_id TEXT,cleanup_state TEXT);
 CREATE TABLE volume_slots(id TEXT,spec_json TEXT);
 INSERT INTO operations VALUES('op_fixture','succeeded');
 INSERT INTO workspaces VALUES('workspace_fixture',1,'');
 INSERT INTO volume_leases VALUES('workspace_fixture',1);
 INSERT INTO operation_logs VALUES('op_fixture','removed');`)
	if err != nil {
		t.Fatal(err)
	}
	uuid := strings.Repeat("a", 64)
	if _, err = db.Exec("INSERT INTO journal_identity VALUES(1,?)", uuid); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"slot-001", "slot-002", "slot-003", "slot-004"} {
		v := sandbox.VolumeSpec{ID: id, MountPath: filepath.Join(root, id), ImagePath: filepath.Join(root, id+".img"), ImageBytes: 256 << 20}
		c.VolumeSlots = append(c.VolumeSlots, v)
		raw, _ := json.Marshal(v)
		if _, err = db.Exec("INSERT INTO volume_slots VALUES(?,?)", id, string(raw)); err != nil {
			t.Fatal(err)
		}
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	if err = os.Chmod(c.JournalPath, 0600); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(c.JournalPath))
	body, _ := json.Marshal(map[string]string{"journal_path": c.JournalPath, "journal_id": uuid})
	if err = os.WriteFile(filepath.Join(c.ArtifactRoot, ".runner-journal-"+hex.EncodeToString(sum[:])), body, 0600); err != nil {
		t.Fatal(err)
	}
	return c, uuid
}
func mutate(t *testing.T, path, query string) {
	t.Helper()
	db, e := sql.Open("sqlite", path)
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	if _, e = db.Exec(query); e != nil {
		t.Fatal(e)
	}
}
func TestReadOnlyJournalRequiresActualSettledIdentity(t *testing.T) {
	c, id := sqliteFixture(t)
	before, e := os.ReadFile(c.JournalPath)
	if e != nil {
		t.Fatal(e)
	}
	result, e := journal(context.Background(), c, id)
	if e != nil {
		t.Fatal(e)
	}
	if result["journal_uuid"] != id || result["slots"] != 4 || len(result["operation_ids"].([]string)) != 1 {
		t.Fatal("journal proof missing")
	}
	after, e := os.ReadFile(c.JournalPath)
	if e != nil {
		t.Fatal(e)
	}
	if string(before) != string(after) {
		t.Fatal("read-only journal changed")
	}
	for _, tc := range []struct{ name, query string }{
		{"prepared", "UPDATE operations SET status='prepared'"},
		{"running", "UPDATE operations SET status='running'"},
		{"unknown", "UPDATE operations SET status='unknown'"},
		{"active workspace", "UPDATE workspaces SET active_operation='op_fixture'"},
		{"retained workspace", "UPDATE workspaces SET released=0"},
		{"retained lease", "UPDATE volume_leases SET released=0"},
		{"log container cleanup pending", "UPDATE operation_logs SET cleanup_state='retained'"},
		{"version mismatch", "PRAGMA user_version=4"},
		{"replacement uuid", "UPDATE journal_identity SET id='bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'"},
		{"slot mismatch", "UPDATE volume_slots SET spec_json='{}' WHERE id='slot-001'"},
		{"missing slot", "DELETE FROM volume_slots WHERE id='slot-001'"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, id := sqliteFixture(t)
			mutate(t, c.JournalPath, tc.query)
			if _, e := journal(context.Background(), c, id); e == nil {
				t.Fatal("uncertain/rebound journal admitted")
			}
		})
	}
}
func TestArtifactAuthorityAndRegularInputsFailClosed(t *testing.T) {
	t.Run("foreign registry", func(t *testing.T) {
		c, id := sqliteFixture(t)
		if e := os.WriteFile(filepath.Join(c.ArtifactRoot, ".runner-journal-foreign"), []byte("{}"), 0600); e != nil {
			t.Fatal(e)
		}
		if _, e := journal(context.Background(), c, id); e == nil {
			t.Fatal("second journal authority accepted")
		}
	})
	t.Run("symlink journal", func(t *testing.T) {
		c, id := sqliteFixture(t)
		alias := filepath.Join(filepath.Dir(c.JournalPath), "alias.sqlite")
		if e := os.Symlink(c.JournalPath, alias); e != nil {
			t.Fatal(e)
		}
		c.JournalPath = alias
		if _, e := journal(context.Background(), c, id); e == nil {
			t.Fatal("symlink journal accepted")
		}
	})
	t.Run("hardlink input", func(t *testing.T) {
		p := filepath.Join(t.TempDir(), "source")
		if e := os.WriteFile(p, []byte("x"), 0600); e != nil {
			t.Fatal(e)
		}
		if e := os.Link(p, p+".linked"); e != nil {
			t.Fatal(e)
		}
		if regular(p, 10) == nil {
			t.Fatal("hardlinked authority accepted")
		}
	})
	t.Run("missing journal is never created", func(t *testing.T) {
		root := t.TempDir()
		path := filepath.Join(root, "missing")
		if _, e := journal(context.Background(), runnerConfig{JournalPath: path}, strings.Repeat("a", 64)); e == nil {
			t.Fatal("missing journal accepted")
		}
		if _, e := os.Lstat(path); !os.IsNotExist(e) {
			t.Fatal("read-only preflight created a journal")
		}
	})
}
func TestBinaryAndModeCannotSelectOtherRuntime(t *testing.T) {
	for _, mode := range []string{"", "initialize", "migrate", "reset"} {
		if _, e := check(context.Background(), "/tmp/anything", strings.Repeat("a", 40), mode, ""); e == nil {
			t.Fatal("unsafe mode accepted")
		}
	}
	p := filepath.Join(t.TempDir(), "fake")
	if e := os.WriteFile(p, []byte("not a Go program"), 0700); e != nil {
		t.Fatal(e)
	}
	if _, e := binary(p, strings.Repeat("a", 40), "cmd/forge"); e == nil {
		t.Fatal("non-Go binary identity accepted")
	}
}
