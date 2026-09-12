package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/sandbox"
)

func (e *Engine) publishLogs(ctx context.Context, o *Operation) error {
	p, err := e.journal.logPolicy(ctx, o.Request.OperationID)
	if err != nil || p == nil {
		return err
	}
	summary, data, preview, err := readLogCapture(e.logDir(o.Request.WorkspaceID), *o, *p)
	if err != nil {
		return err
	}
	var job sandbox.Job
	if len(o.Result) > 0 && json.Unmarshal(o.Result, &job) != nil {
		return domain.ErrReconciliation
	}
	if job.NeverStarted {
		var intent bool
		if err = e.journal.db.QueryRowContext(ctx, `SELECT docker_start_intent FROM operations WHERE id=?`, o.Request.OperationID).Scan(&intent); err != nil {
			return err
		}
		if intent {
			return domain.ErrReconciliation
		}
		summary.Complete = true
		summary.DroppedKnown = true
		summary.Truncated = false
		summary.Reason = "never_started"
	}
	ref, err := e.config.Artifacts.Put(ctx, o.Request.TenantID, o.Request.RunID, "operation_log", bytes.NewReader(data))
	if err != nil {
		return err
	}
	if err = e.journal.pinArtifact(ctx, ref); err != nil {
		return err
	}
	summary.Artifact = ref
	job.Log = &summary
	job.StrictLogs = true
	job.Output = preview
	job.Truncated = summary.Truncated
	if !sandbox.VerificationLogValid(job) && o.Status != Cancelled {
		o.Status = Failed
	}
	o.Result, err = json.Marshal(job)
	return err
}

func (e *Engine) cleanupLogContainers(ctx context.Context, workspace domain.ID) error {
	rows, err := e.journal.db.QueryContext(ctx, `SELECT o.id,o.job_id,o.status,l.container_id FROM operations o JOIN operation_logs l ON l.operation_id=o.id WHERE o.workspace_id=? AND l.cleanup_state<>'removed'`, workspace)
	if err != nil {
		return err
	}
	type item struct {
		id                      domain.ID
		name, status, container string
	}
	var items []item
	for rows.Next() {
		var i item
		if err = rows.Scan(&i.id, &i.name, &i.status, &i.container); err != nil {
			rows.Close()
			return err
		}
		items = append(items, i)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, i := range items {
		if !Status(i.status).Terminal() {
			return domain.ErrReconciliation
		}
		// No persisted container ID is not proof of absent side effects. Only a
		// never-dispatched row may be settled without a deletion acknowledgement.
		if i.container == "" {
			var intent bool
			if err = e.journal.db.QueryRowContext(ctx, `SELECT docker_start_intent FROM operations WHERE id=?`, i.id).Scan(&intent); err != nil {
				return err
			}
			if intent {
				return domain.ErrReconciliation
			}
		} else {
			cleaner, ok := e.config.Backend.(interface {
				RemoveOwned(context.Context, string, string) error
			})
			if !ok {
				return fmt.Errorf("%w: owned container cleanup unavailable", domain.ErrReconciliation)
			}
			if _, err = e.journal.db.ExecContext(ctx, `UPDATE operation_logs SET cleanup_state='removing' WHERE operation_id=?`, i.id); err != nil {
				return err
			}
			if err = cleaner.RemoveOwned(ctx, i.name, i.container); err != nil {
				return err
			}
		}
		if _, err = e.journal.db.ExecContext(ctx, `UPDATE operation_logs SET cleanup_state='removed' WHERE operation_id=?`, i.id); err != nil {
			return err
		}
	}
	return nil
}
