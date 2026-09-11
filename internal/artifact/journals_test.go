package artifact

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestJournalRegistrationIsAtomicAndRefusesOmittedAuthorities(t *testing.T) {
	ctx := context.Background()
	s, err := NewLocalStore(t.TempDir(), 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	identity := strings.Repeat("a", 64)
	one, two := filepath.Join(t.TempDir(), "one.sqlite"), filepath.Join(t.TempDir(), "two.sqlite")
	for _, p := range []string{one, two} {
		if err := os.WriteFile(p, nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err = s.CheckJournal(ctx, one, identity); err == nil {
		t.Fatal("accepted unregistered journal")
	}
	var group sync.WaitGroup
	for range 10 {
		group.Go(func() {
			if err := RegisterJournal(ctx, s, one, identity); err != nil {
				t.Error(err)
			}
		})
	}
	group.Wait()
	if err = s.CheckJournal(ctx, one, identity); err != nil {
		t.Fatal(err)
	}
	if err = s.CheckJournal(ctx, one, strings.Repeat("b", 64)); err == nil {
		t.Fatal("accepted new identity at same path")
	}
	if err = RegisterJournal(ctx, s, one, strings.Repeat("b", 64)); err == nil {
		t.Fatal("registered replacement journal over existing authority")
	}
	if err = s.CheckJournal(ctx, two, identity); err == nil {
		t.Fatal("accepted unrelated journal")
	}
	if err = RegisterJournal(ctx, s, two, identity); err != nil {
		t.Fatal(err)
	}
	if err = s.CheckJournal(ctx, one, identity); err == nil {
		t.Fatal("omitted second reference authority")
	}
	if err = s.CheckJournal(ctx, two, identity); err == nil {
		t.Fatal("omitted first reference authority")
	}
}
