package artifact

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
)

func TestImmutableConcurrentPutOwnershipAndCorruption(t *testing.T) {
	dir := t.TempDir()
	s, err := NewLocalStore(dir, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	results := make(chan Ref, 20)
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := s.Put(ctx, "tenant", "run", "log", bytes.NewBufferString("durable result"))
			if err != nil {
				t.Error(err)
				return
			}
			results <- r
		}()
	}
	wg.Wait()
	close(results)
	var ref Ref
	for r := range results {
		if ref.ObjectKey != "" && r != ref {
			t.Fatal("identical content did not converge")
		}
		ref = r
	}
	f, err := s.Open(ctx, "tenant", "run", ref)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(f)
	_ = f.Close()
	if string(raw) != "durable result" {
		t.Fatal("bad object bytes")
	}
	if _, err = s.Open(ctx, "other", "run", ref); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("cross-tenant object access: %v", err)
	}
	if err = os.WriteFile(filepath.Join(dir, ref.ObjectKey), []byte("corrupt result"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Open(ctx, "tenant", "run", ref); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("same-size corruption undetected: %v", err)
	}
}

func TestArtifactByteLimitAndContextDoNotPublishObjects(t *testing.T) {
	s, err := NewLocalStore(t.TempDir(), 3)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err = s.Put(context.Background(), "tenant", "run", "log", bytes.NewBufferString("four")); !errors.Is(err, domain.ErrCapacity) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = s.Put(ctx, "tenant", "run", "log", bytes.NewBufferString("x")); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(filepath.Join(s.root.Name(), "tenant", "run"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("failed puts left published objects: %v", entries)
	}
}
