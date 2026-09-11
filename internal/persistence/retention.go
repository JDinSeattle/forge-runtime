package persistence

import (
	"context"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/dependency"
	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/jackc/pgx/v5"
)

// TrimEvents removes only old terminal-run transport events, preserving at
// least keepTail recent events and every durable snapshot/effect/receipt ref.
// Boundary advancement and deletion share a run lock and one transaction.
// It is an explicit operator maintenance action, not request-path work.
func (s *Store) TrimEvents(ctx context.Context, before time.Time, keepTail, batch int) (int64, error) {
	ctx, cancel := dependency.Database(ctx)
	defer cancel()
	if keepTail < 1 || keepTail > 10000 || batch < 1 || batch > 128 || before.IsZero() {
		return 0, domain.ErrInvalid
	}
	tx, err := dependency.BeginTx(ctx, s.Pool, pgx.TxOptions{})
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `SELECT tenant_id,id,GREATEST(1,next_event_seq-$2) FROM runs WHERE state IN ('completed','failed','cancelled','budget_exhausted') AND updated_at<$1 AND retained_from_seq<GREATEST(1,next_event_seq-$2) ORDER BY updated_at,id LIMIT $3 FOR UPDATE SKIP LOCKED`, before, keepTail, batch)
	if err != nil {
		return 0, err
	}
	type target struct {
		Tenant, ID string
		First      int64
	}
	items, err := pgx.CollectRows(rows, pgx.RowToStructByPos[target])
	if err != nil {
		return 0, err
	}
	var deleted int64
	for _, r := range items {
		if _, err = tx.Exec(ctx, `UPDATE runs SET retained_from_seq=$3 WHERE tenant_id=$1 AND id=$2`, r.Tenant, r.ID, r.First); err != nil {
			return 0, err
		}
		tag, err := tx.Exec(ctx, `DELETE FROM run_events WHERE tenant_id=$1 AND run_id=$2 AND seq<$3`, r.Tenant, r.ID, r.First)
		if err != nil {
			return 0, err
		}
		deleted += tag.RowsAffected()
	}
	return deleted, tx.Commit(ctx)
}
