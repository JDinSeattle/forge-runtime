package retention

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/artifact"
	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	"github.com/JDinSeattle/forge-runtime/internal/testdb"
)

func testJournal(t *testing.T) (string, *sql.DB) {
	t.Helper()
	filename := filepath.Join(t.TempDir(), "journal.sqlite")
	j, err := sql.Open("sqlite", filename)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = j.Close() })
	_, err = j.Exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
CREATE TABLE operations(receipt_json TEXT NOT NULL);
CREATE TABLE runner_artifacts(object_key TEXT PRIMARY KEY,ref_json TEXT NOT NULL);
CREATE TABLE journal_identity(singleton INTEGER PRIMARY KEY CHECK(singleton=1), id TEXT NOT NULL);
INSERT INTO journal_identity VALUES(1,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');
PRAGMA user_version=4;`)
	if err != nil {
		t.Fatal(err)
	}
	return filename, j
}

func TestJournalReferenceReaderProtectsOldReceiptsAndNewPinsFailsClosed(t *testing.T) {
	ctx := context.Background()
	filename, j := testJournal(t)
	ref := artifact.Ref{TenantID: "tenant", RunID: "run", Kind: "workspace_snapshot", SHA256: strings.Repeat("a", 64), Size: 5}
	ref.ObjectKey = "tenant/run/" + ref.SHA256
	raw, _ := json.Marshal(ref)
	if _, err := j.Exec(`INSERT INTO runner_artifacts VALUES(?,?)`, ref.ObjectKey, string(raw)); err != nil {
		t.Fatal(err)
	}
	ref.Kind, ref.SHA256 = "operation_receipt", strings.Repeat("b", 64)
	ref.ObjectKey = "tenant/run/" + ref.SHA256
	raw, _ = json.Marshal(ref)
	if _, err := j.Exec(`INSERT INTO operations VALUES(?),('{}')`, string(raw)); err != nil {
		t.Fatal(err)
	}
	refs := map[string]bool{}
	if err := journalReferences(ctx, filename, refs); err != nil || len(refs) != 2 {
		t.Fatalf("lost journal references %+v %v", refs, err)
	}
	if _, err := j.Exec(`UPDATE runner_artifacts SET ref_json='{}'`); err != nil {
		t.Fatal(err)
	}
	if err := journalReferences(ctx, filename, map[string]bool{}); err == nil {
		t.Fatal("corrupt pin accepted")
	}
	if _, err := j.Exec(`PRAGMA user_version=3`); err != nil {
		t.Fatal(err)
	}
	if err := journalReferences(ctx, filename, map[string]bool{}); err == nil {
		t.Fatal("unupgraded journal accepted")
	}
	if err := journalReferences(ctx, filepath.Join(t.TempDir(), "missing.sqlite"), map[string]bool{}); err == nil {
		t.Fatal("missing journal accepted")
	}
}

func TestCrossStoreOrphanCollectionPublicationRaces(t *testing.T) {
	ctx := context.Background()
	dsn := testdb.New(t)
	db, err := persistence.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if err = db.BootstrapTenant(ctx, "tenant", "operator", "admin"); err != nil {
		t.Fatal(err)
	}
	project, err := db.CreateProject(ctx, "tenant", "gc fixture", "fixture", "python")
	if err != nil {
		t.Fatal(err)
	}
	run, _, err := db.Submit(ctx, persistence.SubmitRequest{TenantID: "tenant", PrincipalID: "operator", ProjectID: project.ID, Task: "retention fixture", BaseCommit: "fixture-v1", Config: persistence.Config{Provider: "fake", Model: "scripted", MaxModelRounds: 3, MaxToolCalls: 5, MaxCost: 100, MaxRuntimeSeconds: 60}}, "gc-fixture")
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	objects, err := artifact.NewLocalStore(root, 4096)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = objects.Close() })
	filename, journal := testJournal(t)
	if err = artifact.RegisterJournal(ctx, objects, filename, strings.Repeat("a", 64)); err != nil {
		t.Fatal(err)
	}
	putOld := func(body string) artifact.Ref {
		t.Helper()
		ref, err := objects.Put(ctx, run.TenantID, run.ID, "test_evidence", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		old := time.Now().Add(-48 * time.Hour)
		if err = os.Chtimes(filepath.Join(root, ref.ObjectKey), old, old); err != nil {
			t.Fatal(err)
		}
		return ref
	}
	publish := func(ctx context.Context, ref artifact.Ref, id domain.ID) error {
		return artifact.WithPublication(ctx, objects, func(ctx context.Context) error {
			if _, err := objects.Stat(ctx, ref.TenantID, ref.RunID, ref); err != nil {
				return err
			}
			return db.PublishArtifact(ctx, persistence.Artifact{TenantID: ref.TenantID, ID: id, RunID: ref.RunID, Kind: ref.Kind, ObjectKey: ref.ObjectKey, SHA256: ref.SHA256, ByteSize: ref.Size})
		})
	}
	ready, pinned, orphan := putOld("ready"), putOld("journal only"), putOld("orphan")
	if err = publish(ctx, ready, "ready_id"); err != nil {
		t.Fatal(err)
	}
	// Inject a real PostgreSQL transaction failure after bytes already exist.
	// The constraint is confined to this test's private schema, and the failed
	// publication must neither expose metadata nor make a completion event.
	var beforeSeq, afterSeq uint64
	if err = db.Pool.QueryRow(ctx, `SELECT next_event_seq FROM runs WHERE tenant_id=$1 AND id=$2`, run.TenantID, run.ID).Scan(&beforeSeq); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Pool.Exec(ctx, `ALTER TABLE run_events ADD CONSTRAINT reject_publication_fixture CHECK(type <> 'artifact.ready') NOT VALID`); err != nil {
		t.Fatal(err)
	}
	orphan.Kind = "rejected_fixture"
	if err = publish(ctx, orphan, "failed_publication"); err == nil {
		t.Fatal("injected business transaction unexpectedly committed")
	}
	if _, err = db.GetArtifact(ctx, run.TenantID, "failed_publication"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatal("failed publication exposed READY metadata", err)
	}
	if _, err = objects.Stat(ctx, orphan.TenantID, orphan.RunID, orphan); err != nil {
		t.Fatal("failed transaction lost durable orphan", err)
	}
	if err = db.Pool.QueryRow(ctx, `SELECT next_event_seq FROM runs WHERE tenant_id=$1 AND id=$2`, run.TenantID, run.ID).Scan(&afterSeq); err != nil || afterSeq != beforeSeq {
		t.Fatal("failed event append changed durable sequence", err)
	}
	if _, err = db.Pool.Exec(ctx, `ALTER TABLE run_events DROP CONSTRAINT reject_publication_fixture`); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(pinned)
	if _, err = journal.Exec(`INSERT INTO runner_artifacts VALUES(?,?)`, pinned.ObjectKey, string(raw)); err != nil {
		t.Fatal(err)
	}
	options := artifact.CollectionOptions{MinAge: time.Hour, Limit: 100, Apply: true}
	if _, err = Collect(ctx, objects, db, filepath.Join(t.TempDir(), "missing.sqlite"), "local", options); err == nil {
		t.Fatal("missing journal did not abort sweep")
	}
	if _, err = objects.Stat(ctx, orphan.TenantID, orphan.RunID, orphan); err != nil {
		t.Fatal("failed reference load deleted orphan", err)
	}
	result, err := Collect(ctx, objects, db, filename, "local", options)
	if err != nil || len(result.Objects) != 1 || result.Objects[0].Key != orphan.ObjectKey || result.Protected != 2 {
		t.Fatalf("wrong collection %+v %v", result, err)
	}
	// GC wins. A delayed publisher must verify again under the lock, so it
	// cannot turn the deleted orphan into a READY reference.
	if err = publish(ctx, orphan, "late_id"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("late publisher made missing object ready: %v", err)
	}
	if _, err = db.GetArtifact(ctx, run.TenantID, "late_id"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("late READY row exists: %v", err)
	}
	for _, ref := range []artifact.Ref{ready, pinned} {
		if _, err = objects.Stat(ctx, ref.TenantID, ref.RunID, ref); err != nil {
			t.Fatal("lost recoverable reference", err)
		}
	}
	// Publisher wins with the exact same old key (different kind is allowed).
	reused := putOld("old deduplicated object")
	started, proceed, completed := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		completed <- artifact.WithPublication(ctx, objects, func(ctx context.Context) error {
			ref, err := objects.Put(ctx, run.TenantID, run.ID, "new_kind", strings.NewReader("old deduplicated object"))
			if err != nil {
				return err
			}
			close(started)
			<-proceed
			return publish(ctx, ref, "reused_id")
		})
	}()
	<-started
	blocked, cancel := context.WithTimeout(ctx, 60*time.Millisecond)
	_, err = Collect(blocked, objects, db, filename, "local", options)
	cancel()
	close(proceed)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("GC crossed database publication: %v", err)
	}
	if err = <-completed; err != nil {
		t.Fatal(err)
	}
	result, err = Collect(ctx, objects, db, filename, "local", options)
	if err != nil || len(result.Objects) != 0 || result.Protected != 3 {
		t.Fatalf("dedup publication collected %+v %v", result, err)
	}
	if _, err = objects.Stat(ctx, reused.TenantID, reused.RunID, reused); err != nil {
		t.Fatal(err)
	}
	// A lost journal recreated at the identical pathname must not silently
	// become an empty reference authority for old runner-only artifacts.
	if err = journal.Close(); err != nil {
		t.Fatal(err)
	}
	if err = os.Rename(filename, filename+".saved"); err != nil {
		t.Fatal(err)
	}
	replacement, err := sql.Open("sqlite", filename)
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	_, err = replacement.Exec(`CREATE TABLE journal_identity(singleton INTEGER PRIMARY KEY, id TEXT NOT NULL);
INSERT INTO journal_identity VALUES(1,'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb');
CREATE TABLE operations(receipt_json TEXT NOT NULL);
CREATE TABLE runner_artifacts(object_key TEXT PRIMARY KEY,ref_json TEXT NOT NULL); PRAGMA user_version=4;`)
	if err != nil {
		t.Fatal(err)
	}
	if err = artifact.RegisterJournal(ctx, objects, filename, strings.Repeat("b", 64)); err == nil {
		t.Fatal("replacement overwrote journal registration")
	}
	if _, err = Collect(ctx, objects, db, filename, "local", options); err == nil {
		t.Fatal("same-path replacement became GC authority")
	}
	if _, err = objects.Stat(ctx, pinned.TenantID, pinned.RunID, pinned); err != nil {
		t.Fatal("lost original runner-only evidence", err)
	}
	t.Logf("READY refs=2, runner-only refs=1, old orphan deleted=1; both GC/publication orderings preserved consistency")
}
