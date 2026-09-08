package review_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/eventstream"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
)

type reviewEventSource struct {
	mu              sync.Mutex
	covered         uint64
	events          []persistence.Event
	snapshotRead    chan struct{}
	releaseSnapshot chan struct{}
}

func (s *reviewEventSource) GetRun(ctx context.Context, tenant, id domain.ID) (persistence.Run, error) {
	s.mu.Lock()
	covered, observed, release := s.covered, s.snapshotRead, s.releaseSnapshot
	s.snapshotRead, s.releaseSnapshot = nil, nil
	s.mu.Unlock()
	if observed != nil {
		close(observed)
		select {
		case <-release:
		case <-ctx.Done():
			return persistence.Run{}, ctx.Err()
		}
	}
	return persistence.Run{TenantID: tenant, ID: id, CoveredSeq: covered}, nil
}
func (s *reviewEventSource) Events(ctx context.Context, tenant, id domain.ID, after uint64, limit int) ([]persistence.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := []persistence.Event{}
	for _, e := range s.events {
		if e.Seq > after {
			result = append(result, e)
		}
		if len(result) == limit {
			break
		}
	}
	return result, nil
}
func (s *reviewEventSource) set(covered uint64, seqs ...uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.covered = covered
	s.events = nil
	for _, seq := range seqs {
		s.events = append(s.events, persistence.Event{RunID: "run", Seq: seq, Type: "review"})
	}
}
func reviewStreamEvent(t *testing.T, sub eventstream.Subscription, seq uint64) {
	t.Helper()
	select {
	case event, ok := <-sub.Events:
		if !ok || event.Seq != seq {
			t.Fatalf("wanted live sequence %d, got %+v (open=%v)", seq, event, ok)
		}
	case err := <-sub.Errors:
		t.Fatalf("unexpected subscription error: %v", err)
	case <-time.After(time.Second):
		t.Fatalf("live sequence %d was never delivered", seq)
	}
}

func TestReviewSSESnapshotToRegistrationGapReplaysHistory(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	source := &reviewEventSource{covered: 1}
	m := eventstream.New(ctx, source, eventstream.Config{PollInterval: time.Millisecond})
	defer m.Close()
	first, err := m.Subscribe(ctx, "tenant", "run", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	observed, release := make(chan struct{}), make(chan struct{})
	source.mu.Lock()
	source.snapshotRead, source.releaseSnapshot = observed, release
	source.mu.Unlock()
	result := make(chan eventstream.Subscription, 1)
	failure := make(chan error, 1)
	go func() {
		sub, err := m.Subscribe(ctx, "tenant", "run", 1)
		if err != nil {
			failure <- err
			return
		}
		result <- sub
	}()
	select {
	case <-observed:
	case <-ctx.Done():
		t.Fatal("second snapshot was not read")
	}
	source.set(2, 2)
	reviewStreamEvent(t, first, 2)
	close(release)
	select {
	case second := <-result:
		defer second.Close()
		if second.CoveredSeq != 1 {
			t.Fatalf("snapshot cover changed: %d", second.CoveredSeq)
		}
		reviewStreamEvent(t, second, 2)
	case err := <-failure:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal("second subscribe did not finish")
	}
	if _, err := m.Subscribe(ctx, "tenant", "run", 3); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("future cursor accepted: %v", err)
	}
}

func TestReviewSSEReconnectAfterGapStartsFreshHub(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	source := &reviewEventSource{covered: 1}
	m := eventstream.New(ctx, source, eventstream.Config{PollInterval: time.Millisecond})
	defer m.Close()
	first, err := m.Subscribe(ctx, "tenant", "run", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	// Simulate retained SQL history starting after a missing sequence. The old
	// handler has observed reset but has not yet run its deferred Close.
	source.set(3, 3)
	select {
	case err := <-first.Errors:
		if !errors.Is(err, eventstream.ErrReset) {
			t.Fatalf("gap error=%v", err)
		}
	case <-ctx.Done():
		t.Fatal("gap was not detected")
	}
	second, err := m.Subscribe(ctx, "tenant", "run", 3)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	source.set(4, 4)
	reviewStreamEvent(t, second, 4)
}
