package runtime

import (
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
)

func TestCapacityRejectionRequeuesOnlyUninitializedWork(t *testing.T) {
	s := NewState("tenant", "run", Limits{})
	l := domain.Lease{Owner: "worker", Epoch: 1, Until: clock.Add(time.Minute)}
	s, _ = apply(t, s, Event{Kind: EventClaimed, Owner: l.Owner, Epoch: l.Epoch, Lease: &l})
	retry := clock.Add(time.Second)
	e := envelope(s, Event{Kind: EventCapacityRejected, NotBefore: &retry})
	next, commands := apply(t, s, e)
	if next.Status != domain.StatusQueued || next.Stage != domain.StageInitialize || next.Lease.Owner != "" || !next.Lease.Until.IsZero() || next.Lease.Epoch != l.Epoch || len(commands) != 0 {
		t.Fatalf("capacity rejection retained active work: %+v / %+v", next, commands)
	}
	reject(t, next, e, domain.ErrConflict)
	l.Owner, l.Epoch = "other-worker", 2
	claimed, commands := apply(t, next, Event{Kind: EventClaimed, Owner: l.Owner, Epoch: l.Epoch, Lease: &l, At: retry})
	wantCommand(t, commands, CommandInitializeWorkspace)
	if claimed.Lease.Epoch != 2 {
		t.Fatal("reclaim did not retain monotonic fencing epoch")
	}
	for _, invalid := range []State{started(t), dispatched(t)} {
		reject(t, invalid, envelope(invalid, Event{Kind: EventCapacityRejected, NotBefore: &retry}), domain.ErrTransition)
	}
	reject(t, s, envelope(s, Event{Kind: EventCapacityRejected}), domain.ErrInvalid)
	reject(t, s, envelope(s, Event{Kind: EventCapacityRejected, NotBefore: &clock}), domain.ErrInvalid)
	e.Owner = "stale"
	reject(t, s, e, domain.ErrFenced)
}
