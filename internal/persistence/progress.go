package persistence

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	flow "github.com/JDinSeattle/forge-runtime/internal/runtime"
	"github.com/jackc/pgx/v5"
)

// Called with the same tenant/run locks used by AddMessage and the transition.
// No artifact I/O or runner/model work is allowed while this transaction lives.
func validateContextProgress(ctx context.Context, tx pgx.Tx, r Run, event flow.Event) error {
	if event.Kind != flow.EventContextBuilt || r.State.Limits.MaxNoProgressBatches == 0 {
		return nil
	}
	var latest uint64
	if err := tx.QueryRow(ctx, `SELECT coalesce(max(seq),0) FROM run_messages WHERE tenant_id=$1 AND run_id=$2`, r.TenantID, r.ID).Scan(&latest); err != nil {
		return err
	}
	f := event.Progress
	watermark := uint64(0)
	if f != nil {
		watermark = f.MessageSeq
	}
	if watermark > latest {
		return domain.ErrUntrusted
	}
	if watermark < latest {
		return domain.ErrContextStale
	}
	if f == nil {
		if r.State.StepSeq == 0 {
			return nil
		}
		return domain.ErrUntrusted
	}
	if f.ContextRef != event.OutputRef || f.StepSeq != r.State.StepSeq || f.PolicyVersion != flow.ProgressPolicyVersion || len(f.Observations) > 130 {
		return domain.ErrUntrusted
	}
	var unapplied bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM run_messages WHERE tenant_id=$1 AND run_id=$2 AND seq<=$3 AND (applied_step IS NULL OR applied_step>$4))`, r.TenantID, r.ID, watermark, r.State.StepSeq).Scan(&unapplied); err != nil {
		return err
	}
	if unapplied {
		return domain.ErrUntrusted
	}
	// The exact report represented by the event must have been published READY.
	b, err := json.Marshal(f)
	if err != nil {
		return err
	}
	h := sha256.Sum256(b)
	var found bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM artifacts WHERE tenant_id=$1 AND run_id=$2 AND id=$3 AND kind='progress_report' AND state='ready' AND sha256=$4)`, r.TenantID, r.ID, event.ProgressRef, hex.EncodeToString(h[:])).Scan(&found); err != nil {
		return err
	}
	if !found {
		return domain.ErrUntrusted
	}
	if f.StepSeq == 0 {
		return nil
	}
	var unresolved bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM effects WHERE tenant_id=$1 AND run_id=$2 AND status IN ('in_flight','unknown'))`, r.TenantID, r.ID).Scan(&unresolved); err != nil {
		return err
	}
	if unresolved {
		return domain.ErrReconciliation
	}
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM model_attempts WHERE tenant_id=$1 AND run_id=$2 AND step_seq=$3 AND attempt_id=$4 AND status='completed' AND attempt=(SELECT max(attempt) FROM model_attempts WHERE tenant_id=$1 AND run_id=$2 AND step_seq=$3))`, r.TenantID, r.ID, f.StepSeq, f.AttemptID).Scan(&found); err != nil {
		return err
	}
	if !found || f.VerificationRef != r.State.VerificationReportRef {
		return domain.ErrUntrusted
	}
	var receiptCount int
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM effects WHERE tenant_id=$1 AND run_id=$2 AND step_seq=$3 AND receipt_ref IS NOT NULL AND ((ordinal>=0 AND $4='') OR (ordinal IN (-2,-3) AND $4<>''))`, r.TenantID, r.ID, f.StepSeq, f.VerificationRef).Scan(&receiptCount); err != nil {
		return err
	}
	if receiptCount != len(f.Observations) {
		return domain.ErrUntrusted
	}
	for _, o := range f.Observations {
		if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM effects e JOIN artifacts a ON a.tenant_id=e.tenant_id AND a.run_id=e.run_id AND a.id=e.receipt_ref WHERE e.tenant_id=$1 AND e.run_id=$2 AND e.step_seq=$3 AND e.operation_id=$4 AND e.receipt_ref=$5 AND e.status IN ('succeeded','failed','cancelled') AND a.state='ready' AND ((e.ordinal>=0 AND $6='') OR (e.ordinal IN (-2,-3) AND $6<>'')))`, r.TenantID, r.ID, f.StepSeq, o.EffectID, o.ReceiptRef, f.VerificationRef).Scan(&found); err != nil {
			return err
		}
		if !found {
			return domain.ErrUntrusted
		}
	}
	return nil
}
