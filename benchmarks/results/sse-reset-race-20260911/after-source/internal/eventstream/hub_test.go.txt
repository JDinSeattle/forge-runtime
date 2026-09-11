package eventstream

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
)

type retirementSource struct {
	covered atomic.Uint64
	first   atomic.Bool
	entered chan struct{}
	release chan struct{}
	reset   bool
}

func (s *retirementSource) GetRun(_ context.Context, tenant, run domain.ID) (persistence.Run, error) {
	return persistence.Run{TenantID: tenant, ID: run, CoveredSeq: s.covered.Load()}, nil
}
func (s *retirementSource) Events(ctx context.Context, _, run domain.ID, after uint64, _ int) ([]persistence.Event, error) {
	if s.first.CompareAndSwap(false, true) {
		close(s.entered)
		select {
		case <-s.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		if s.reset {
			return nil, ErrReset
		}
	}
	seq := s.covered.Load()
	if seq > after {
		return []persistence.Event{{RunID: run, Seq: seq, Type: "fixture"}}, nil
	}
	return nil, nil
}

func TestResetDetachesHubBeforeNotification(t *testing.T) {
	for _, reset := range []bool{false, true} {
		name := "history-gap"
		if reset {
			name = "source-reset"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			source := &retirementSource{entered: make(chan struct{}), release: make(chan struct{}), reset: reset}
			source.covered.Store(1)
			manager := New(ctx, source, Config{PollInterval: time.Millisecond})
			defer manager.Close()
			first, err := manager.Subscribe(ctx, "tenant", "run", 1)
			if err != nil {
				t.Fatal(err)
			}
			defer first.Close()
			select {
			case <-source.entered:
			case <-ctx.Done():
				t.Fatal("poll never entered source")
			}
			// Block the admission-map update while letting the source expose a
			// missing sequence. ErrReset must wait for that update: it authorizes
			// the client to reconnect immediately, before its old Close runs.
			manager.mu.Lock()
			source.covered.Store(3)
			close(source.release)
			var resetErr error
			select {
			case resetErr = <-first.Errors:
				t.Error("reset published while the dying hub was still admissible")
			case <-time.After(30 * time.Millisecond):
			}
			manager.mu.Unlock()
			if resetErr == nil {
				select {
				case resetErr = <-first.Errors:
				case <-ctx.Done():
					t.Fatal("reset not published after map unlocked")
				}
			}
			if !errors.Is(resetErr, ErrReset) {
				t.Fatalf("reset error: %v", resetErr)
			}
			second, err := manager.Subscribe(ctx, "tenant", "run", 3)
			if err != nil {
				t.Fatal(err)
			}
			defer second.Close()
			first.Close() // Late cleanup from an older generation.
			source.covered.Store(4)
			select {
			case event, ok := <-second.Events:
				if !ok || event.Seq != 4 {
					t.Fatalf("replacement lost event 4: %+v open=%v", event, ok)
				}
			case err := <-second.Errors:
				t.Fatalf("replacement subscription failed: %v", err)
			case <-ctx.Done():
				t.Fatal("replacement never received event 4")
			}
		})
	}
}
