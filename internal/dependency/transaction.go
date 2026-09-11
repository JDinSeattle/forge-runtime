package dependency

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type beginner interface {
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
}

// BeginTx owns one deadline covering pool acquisition and the transaction's
// ordinary SQL methods. The caller must defer Rollback and must not hold the
// transaction across external I/O. Conn/LargeObjects are raw pgx escape hatches,
// not used by application SQL paths; they do not acquire a new deadline here.
func BeginTx(ctx context.Context, pool beginner, options pgx.TxOptions) (pgx.Tx, error) {
	ctx, cancel := Database(ctx)
	tx, err := pool.BeginTx(ctx, options)
	if err != nil {
		cancel()
		return nil, err
	}
	return &transaction{Tx: tx, ctx: ctx, cancel: cancel}, nil
}

type transaction struct {
	pgx.Tx
	ctx    context.Context
	cancel context.CancelFunc
}

func (t *transaction) operation(ctx context.Context) (context.Context, context.CancelFunc) {
	deadline, _ := t.ctx.Deadline()
	ctx, cancel := context.WithDeadline(ctx, deadline)
	// The operation owns a timer at the same absolute deadline. Calling its
	// CancelFunc for a transaction timeout races that timer and can turn
	// DeadlineExceeded into Canceled. Forward only an explicit cancellation;
	// let the operation's own deadline preserve the timeout classification.
	if t.ctx.Err() == context.Canceled {
		cancel()
	}
	stop := context.AfterFunc(t.ctx, func() {
		if t.ctx.Err() == context.Canceled {
			cancel()
		}
	})
	return ctx, func() { stop(); cancel() }
}

func (t *transaction) Begin(ctx context.Context) (pgx.Tx, error) {
	ctx, cancel := t.operation(ctx)
	tx, err := t.Tx.Begin(ctx)
	if err != nil {
		cancel()
		return nil, err
	}
	return &transaction{Tx: tx, ctx: ctx, cancel: cancel}, nil
}
func (t *transaction) Commit(ctx context.Context) error {
	ctx, cancel := t.operation(ctx)
	defer cancel()
	defer t.cancel()
	return t.Tx.Commit(ctx)
}
func (t *transaction) Rollback(ctx context.Context) error {
	// Rollback must still run after the original query budget/client was canceled.
	// pgx closes a connection if this bounded rollback cannot resynchronize it.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), CleanupTimeout)
	defer cancel()
	defer t.cancel()
	return t.Tx.Rollback(ctx)
}
func (t *transaction) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	ctx, cancel := t.operation(ctx)
	defer cancel()
	return t.Tx.Exec(ctx, sql, args...)
}
func (t *transaction) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	ctx, cancel := t.operation(ctx)
	rows, err := t.Tx.Query(ctx, sql, args...)
	if err != nil {
		cancel()
		return nil, err
	}
	return &boundedRows{Rows: rows, cancel: cancel}, nil
}
func (t *transaction) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	ctx, cancel := t.operation(ctx)
	return boundedRow{Row: t.Tx.QueryRow(ctx, sql, args...), cancel: cancel}
}
func (t *transaction) CopyFrom(ctx context.Context, table pgx.Identifier, columns []string, source pgx.CopyFromSource) (int64, error) {
	ctx, cancel := t.operation(ctx)
	defer cancel()
	return t.Tx.CopyFrom(ctx, table, columns, source)
}
func (t *transaction) Prepare(ctx context.Context, name, sql string) (*pgconn.StatementDescription, error) {
	ctx, cancel := t.operation(ctx)
	defer cancel()
	return t.Tx.Prepare(ctx, name, sql)
}
func (t *transaction) SendBatch(ctx context.Context, batch *pgx.Batch) pgx.BatchResults {
	ctx, cancel := t.operation(ctx)
	return &boundedBatch{BatchResults: t.Tx.SendBatch(ctx, batch), cancel: cancel}
}

type boundedRows struct {
	pgx.Rows
	cancel context.CancelFunc
}

func (r *boundedRows) Close() { r.Rows.Close(); r.cancel() }
func (r *boundedRows) Next() bool {
	next := r.Rows.Next()
	if !next {
		r.cancel()
	}
	return next
}

type boundedRow struct {
	pgx.Row
	cancel context.CancelFunc
}

func (r boundedRow) Scan(dst ...any) error { defer r.cancel(); return r.Row.Scan(dst...) }

type boundedBatch struct {
	pgx.BatchResults
	cancel context.CancelFunc
}

func (b *boundedBatch) Close() error { defer b.cancel(); return b.BatchResults.Close() }
