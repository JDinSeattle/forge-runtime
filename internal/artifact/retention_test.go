package artifact

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func agedObject(t *testing.T, store *LocalStore, body string) Ref {
	t.Helper()
	ref, err := store.Put(context.Background(), "tenant", "run", "evidence", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-24 * time.Hour)
	if err = os.Chtimes(filepath.Join(store.root.Name(), ref.ObjectKey), old, old); err != nil {
		t.Fatal(err)
	}
	return ref
}

func TestCollectionExcludesOldDeduplicatingPublisherAndPreservesCommittedReference(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	writer, err := NewLocalStore(dir, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	collector, err := NewLocalStore(dir, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer collector.Close()
	ref := agedObject(t, writer, "same old bytes")
	ready := map[string]bool{}
	err = WithPublication(ctx, writer, func(ctx context.Context) error {
		duplicate, err := writer.Put(ctx, ref.TenantID, ref.RunID, "another-kind", bytes.NewBufferString("same old bytes"))
		if err != nil {
			return err
		}
		if duplicate.ObjectKey != ref.ObjectKey {
			t.Fatal("dedup fixture is not the same object")
		}
		blocked, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
		defer cancel()
		readReferences := false
		_, err = collector.Collect(blocked, CollectionOptions{MinAge: time.Hour, Limit: 10, Apply: true}, func(context.Context) (map[string]bool, error) {
			readReferences = true
			return ready, nil
		})
		if !errors.Is(err, context.DeadlineExceeded) || readReferences {
			t.Fatalf("collector crossed active publication: %v", err)
		}
		// This stands for the authoritative DB/journal commit; collection must
		// read it only after the publishing file description is unlocked.
		ready[ref.ObjectKey] = true
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := collector.Collect(ctx, CollectionOptions{MinAge: time.Hour, Limit: 10, Apply: true}, func(context.Context) (map[string]bool, error) { return ready, nil })
	if err != nil || len(result.Objects) != 0 || result.Protected != 1 {
		t.Fatalf("lost ready object: %+v %v", result, err)
	}
	if _, err = writer.Stat(ctx, ref.TenantID, ref.RunID, ref); err != nil {
		t.Fatal(err)
	}
}

func TestCollectionFailsClosedAndCollectsOnlyAgedRecognizedNames(t *testing.T) {
	ctx := context.Background()
	s, err := NewLocalStore(t.TempDir(), 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	orphan := agedObject(t, s, "old orphan")
	fresh, err := s.Put(ctx, "tenant", "run", "log", strings.NewReader("fresh"))
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{".staging-" + strings.Repeat("a", 32), "unrecognized"} {
		p := filepath.Join(s.root.Name(), "tenant", "run", name)
		if err = os.WriteFile(p, []byte("old"), 0600); err != nil {
			t.Fatal(err)
		}
		old := time.Now().Add(-24 * time.Hour)
		if err = os.Chtimes(p, old, old); err != nil {
			t.Fatal(err)
		}
	}
	outside := filepath.Join(t.TempDir(), strings.Repeat("b", 64))
	if err = os.WriteFile(outside, []byte("outside"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink(outside, filepath.Join(s.root.Name(), "tenant", "run", strings.Repeat("b", 64))); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("journal unavailable")
	_, err = s.Collect(ctx, CollectionOptions{MinAge: time.Hour, Limit: 10, Apply: true}, func(context.Context) (map[string]bool, error) { return nil, failure })
	if !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if _, err = s.Stat(ctx, orphan.TenantID, orphan.RunID, orphan); err != nil {
		t.Fatal("reference read failure deleted bytes", err)
	}
	refs := func(context.Context) (map[string]bool, error) { return map[string]bool{}, nil }
	dry, err := s.Collect(ctx, CollectionOptions{MinAge: time.Hour, Limit: 10}, refs)
	if err != nil || len(dry.Objects) != 2 {
		t.Fatalf("dry run %+v %v", dry, err)
	}
	if _, err = s.Stat(ctx, orphan.TenantID, orphan.RunID, orphan); err != nil {
		t.Fatal(err)
	}
	result, err := s.Collect(ctx, CollectionOptions{MinAge: time.Hour, Limit: 1, Apply: true}, refs)
	if err != nil || len(result.Objects) != 1 {
		t.Fatalf("bounded sweep %+v %v", result, err)
	}
	result, err = s.Collect(ctx, CollectionOptions{MinAge: time.Hour, Limit: 10, Apply: true}, refs)
	if err != nil || len(result.Objects) != 1 {
		t.Fatalf("resumed sweep %+v %v", result, err)
	}
	if _, err = s.Stat(ctx, fresh.TenantID, fresh.RunID, fresh); err != nil {
		t.Fatal("fresh object removed", err)
	}
	if got, err := os.ReadFile(outside); err != nil || string(got) != "outside" {
		t.Fatal("followed symlink")
	}
	if _, err = os.Stat(filepath.Join(s.root.Name(), "tenant", "run", "unrecognized")); err != nil {
		t.Fatal("removed unknown name", err)
	}
}

func TestPublicationLockSurvivesAcrossProcessesAndReleasesOnDeath(t *testing.T) {
	if dir := os.Getenv("FORGE_ARTIFACT_LOCK_CHILD"); dir != "" {
		s, err := NewLocalStore(dir, 1024)
		if err != nil {
			t.Fatal(err)
		}
		err = WithPublication(context.Background(), s, func(context.Context) error {
			if err := os.WriteFile(filepath.Join(dir, "child-ready"), []byte("locked"), 0600); err != nil {
				return err
			}
			<-time.After(30 * time.Second)
			return errors.New("parent did not kill the lock holder")
		})
		t.Fatal(err)
	}
	dir := t.TempDir()
	s, err := NewLocalStore(dir, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ref := agedObject(t, s, "abandoned by killed publisher")
	child := exec.Command(os.Args[0], "-test.run=^TestPublicationLockSurvivesAcrossProcessesAndReleasesOnDeath$")
	child.Env = append(os.Environ(), "FORGE_ARTIFACT_LOCK_CHILD="+dir)
	var output bytes.Buffer
	child.Stderr = &output
	if err = child.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = child.Process.Kill(); _ = child.Wait() })
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err = os.Stat(filepath.Join(dir, "child-ready")); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("child never acquired lock: %s", output.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	refs := func(context.Context) (map[string]bool, error) { return map[string]bool{}, nil }
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	_, err = s.Collect(ctx, CollectionOptions{MinAge: time.Hour, Limit: 10, Apply: true}, refs)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("process did not exclude GC: %v", err)
	}
	if err = child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = child.Wait()
	result, err := s.Collect(context.Background(), CollectionOptions{MinAge: time.Hour, Limit: 10, Apply: true}, refs)
	if err != nil || len(result.Objects) != 1 || result.Objects[0].Key != ref.ObjectKey {
		t.Fatalf("crash left collection locked: %+v %v", result, err)
	}
}
