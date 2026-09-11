package application

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
)

func TestDriverTerminalHeartbeatBoundary(t *testing.T) {
	for _, target := range []domain.RunStatus{domain.StatusCompleted, domain.StatusWaitingApproval} {
		t.Run(string(target), func(t *testing.T) {
			d, r, p, _ := repairSetup(t)
			ctx := context.Background()
			if target == domain.StatusWaitingApproval {
				p.scripts[0] = toolScript("approval", "run_command", `{"command":["echo","fixture"]}`)
			}
			claimed, err := d.Store.Claim(ctx, "boundary-worker", d.leaseDuration())
			if err != nil {
				t.Fatal(err)
			}
			reached := false
			d.Fault = func(point string) error {
				if point != "after_advance_before_reload" {
					return nil
				}
				fresh, err := d.Store.GetRun(ctx, r.TenantID, r.ID)
				if err != nil {
					return err
				}
				if fresh.State.Status != target {
					return nil
				}
				reached = true
				if _, err = d.Store.Heartbeat(ctx, r.TenantID, r.ID, claimed.State.Lease.Owner, claimed.State.Lease.Epoch, d.leaseDuration()); !errors.Is(err, domain.ErrFenced) {
					t.Errorf("inactive heartbeat=%v", err)
				}
				// Hold the driver after the committed transition, across two normal
				// heartbeat ticks. An expected fence must not cancel the final reload.
				timer := time.NewTimer(2 * d.leaseDuration() / 3)
				defer timer.Stop()
				<-timer.C
				return nil
			}
			if err = d.Drive(ctx, claimed); err != nil {
				t.Fatalf("committed inactive state returned error: %v", err)
			}
			if !reached {
				t.Fatal("commit/reload barrier not reached")
			}
			fresh, err := d.Store.GetRun(ctx, r.TenantID, r.ID)
			if err != nil || fresh.State.Status != target {
				t.Fatalf("state=%s %v", fresh.State.Status, err)
			}
		})
	}
}
