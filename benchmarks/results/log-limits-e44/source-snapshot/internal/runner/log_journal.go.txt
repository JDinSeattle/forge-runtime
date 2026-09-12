package runner

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/sandbox"
)

func (j *journal) reserveLogs(ctx context.Context, tx *sql.Tx, o Operation, p sandbox.LogPolicy) error {
	raw, _ := json.Marshal(p)
	// Legacy process history is not silently declared compliant with a new run
	// budget. Existing operations remain inspectable; new strict work needs a new run.
	var legacy int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM operations o LEFT JOIN operation_logs l ON l.operation_id=o.id WHERE o.tenant_id=? AND o.run_id=? AND o.id<>? AND json_extract(o.request_json,'$.kind') IN ('run_command','verify') AND l.operation_id IS NULL`, o.Request.TenantID, o.Request.RunID, o.Request.OperationID).Scan(&legacy); err != nil {
		return err
	}
	if legacy != 0 {
		return fmt.Errorf("%w: legacy log run requires a fresh run", domain.ErrReconciliation)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO log_runs(tenant_id,run_id,policy_json,reserved_bytes,operations)VALUES(?,?,?,0,0)ON CONFLICT DO NOTHING`, o.Request.TenantID, o.Request.RunID, string(raw)); err != nil {
		return err
	}
	var policy string
	var used, count int
	if err := tx.QueryRowContext(ctx, `SELECT policy_json,reserved_bytes,operations FROM log_runs WHERE tenant_id=? AND run_id=?`, o.Request.TenantID, o.Request.RunID).Scan(&policy, &used, &count); err != nil {
		return err
	}
	if policy != string(raw) {
		return fmt.Errorf("%w: run log policy is immutable", domain.ErrConflict)
	}
	if count >= p.MaxOperations || used > p.RunBytes-p.OperationBytes {
		return fmt.Errorf("%w: cumulative run log reservation exhausted", domain.ErrCapacity)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO operation_logs(operation_id,policy_json)VALUES(?,?)`, o.Request.OperationID, string(raw)); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `UPDATE log_runs SET reserved_bytes=reserved_bytes+?,operations=operations+1 WHERE tenant_id=? AND run_id=?`, p.OperationBytes, o.Request.TenantID, o.Request.RunID)
	return err
}

func (j *journal) logPolicy(ctx context.Context, id domain.ID) (*sandbox.LogPolicy, error) {
	var raw string
	err := j.db.QueryRowContext(ctx, `SELECT policy_json FROM operation_logs WHERE operation_id=?`, id).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var p sandbox.LogPolicy
	if json.Unmarshal([]byte(raw), &p) != nil {
		return nil, domain.ErrReconciliation
	}
	normalized, err := p.Normalize()
	if err != nil || normalized != p {
		return nil, domain.ErrReconciliation
	}
	return &p, nil
}

func (j *journal) recordLogContainer(ctx context.Context, id domain.ID, container string) error {
	result, err := j.db.ExecContext(ctx, `UPDATE operation_logs SET container_id=? WHERE operation_id=? AND (container_id='' OR container_id=?)`, container, id, container)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return domain.ErrConflict
	}
	return nil
}
