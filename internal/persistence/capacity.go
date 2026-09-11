package persistence

import (
	"context"
	"fmt"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/dependency"
	"github.com/JDinSeattle/forge-runtime/internal/domain"
	flow "github.com/JDinSeattle/forge-runtime/internal/runtime"
	"github.com/jackc/pgx/v5"
)

// RunnerCapacityBackoff matches the scheduler's admission delay. The database
// clock determines its absolute deadline, so workers cannot create a hot loop
// through clock skew or an immediate reclaim of the lease they just released.
const RunnerCapacityBackoff = time.Second

// RequeueCapacity is only for a definitive PrepareWorkspace admission rejection,
// before a workspace or operation has started. It is not an unknown-outcome
// recovery API. Runner/workspace identity remains sticky even after rejection.
// Replaying a successful call with its old version is fenced, without changing
// the allocation or another run's counters.
func (s *Store) RequeueCapacity(ctx context.Context, rejected Run) error {
	ctx, cancel := dependency.Database(ctx)
	defer cancel()
	tx, err := s.Tx(ctx, rejected.TenantID, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err = lockTenant(ctx, tx, rejected.TenantID); err != nil {
		return err
	}
	current, err := getRun(ctx, tx, rejected.TenantID, rejected.ID, true)
	if err != nil {
		return err
	}
	event := flow.Event{Kind: flow.EventCapacityRejected, ExpectedVersion: rejected.State.Version, Owner: rejected.State.Lease.Owner, Epoch: rejected.State.Lease.Epoch}
	if err = tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&event.At); err != nil {
		return err
	}
	if current.State.Version != event.ExpectedVersion || current.State.Lease.Owner != event.Owner || current.State.Lease.Epoch != event.Epoch || !current.State.Lease.ValidAt(event.At) || current.RunnerID == "" || current.RunnerID != rejected.RunnerID {
		return domain.ErrFenced
	}
	if len(current.Commands) != 1 || current.Commands[0].Kind != flow.CommandInitializeWorkspace {
		return fmt.Errorf("%w: capacity rejection has no initialization command", domain.ErrTransition)
	}
	notBefore := event.At.Add(RunnerCapacityBackoff)
	event.NotBefore = &notBefore
	next, commands, err := flow.Transition(current.State, event)
	if err != nil {
		return err
	}
	// Initial target verification can have durable system effects before
	// WorkspaceReady. Snapshot revision zero alone cannot prove no work exists.
	var hasWork, allocated bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM effects WHERE tenant_id=$1 AND run_id=$2) OR EXISTS(SELECT 1 FROM model_attempts WHERE tenant_id=$1 AND run_id=$2), EXISTS(SELECT 1 FROM runner_allocations WHERE tenant_id=$1 AND run_id=$2 AND runner_id=$3 AND lease_epoch=$4 AND state='reserved' AND slots>0)`, current.TenantID, current.ID, current.RunnerID, current.State.Lease.Epoch).Scan(&hasWork, &allocated); err != nil {
		return err
	}
	if hasWork || !allocated {
		return fmt.Errorf("%w: capacity rejection has existing work or no initial allocation", domain.ErrTransition)
	}
	if err = persistTransition(ctx, tx, current.State, next, commands, event, "run.capacity_rejected"); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE runs SET not_before=$3 WHERE tenant_id=$1 AND id=$2`, current.TenantID, current.ID, notBefore); err != nil {
		return err
	}
	if err = releaseAllocation(ctx, tx, current.TenantID, current.ID); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	s.wake(ctx, current.ID)
	return nil
}
