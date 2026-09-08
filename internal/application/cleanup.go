package application

import (
	"context"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	"github.com/JDinSeattle/forge-runtime/internal/runner"
)

// CleanupWorkspace is explicit operator maintenance. It preserves immutable
// code snapshots, receipts and journals; it never handles a recoverable run.
func (d *Driver) CleanupWorkspace(ctx context.Context, tenant, id domain.ID, owner string, minAge time.Duration) (persistence.WorkspaceCleanup, error) {
	c, err := d.Store.AcquireWorkspaceCleanup(ctx, tenant, id, owner, minAge)
	if err != nil {
		return c, err
	}
	if d.RunnerID == "" || d.RunnerID != c.RunnerID {
		return c, domain.ErrForbidden
	}
	if c.Phase == "released" {
		return c, nil
	}
	grant := func(release bool) (runner.WorkspaceRequest, error) {
		until, now, err := d.Store.CleanupProof(ctx, c, release)
		if err != nil {
			return runner.WorkspaceRequest{}, err
		}
		permissions := []string{"cancel", "snapshot", "inspect"}
		if release {
			permissions = []string{"release", "inspect"}
		}
		expires := now.Add(20 * time.Second)
		if limit := until.Add(-d.Signer.Skew); limit.Before(expires) {
			expires = limit
		}
		claims := runner.Claims{TenantID: c.TenantID, RunID: c.RunID, WorkspaceID: c.RunID, Epoch: c.RunnerEpoch, Permissions: permissions, IssuedAt: now, ExpiresAt: expires, Purpose: "cleanup", CleanupID: c.ID}
		token, err := d.Signer.SignCleanup(claims, until)
		return runner.WorkspaceRequest{TenantID: c.TenantID, RunID: c.RunID, WorkspaceID: c.RunID, Epoch: c.RunnerEpoch, Grant: token}, err
	}
	if c.Phase == "requested" {
		request, err := grant(false)
		if err != nil {
			return c, err
		}
		stopped, err := d.Runner.StopWorkspace(ctx, request)
		if err != nil {
			return c, err
		}
		if !stopped.NoActiveOperations || stopped.Workspace.Revision != c.WorkspaceRevision {
			return c, domain.ErrReconciliation
		}
		if _, err = d.publish(ctx, stopped.Ref); err != nil {
			return c, err
		}
		request, err = grant(false)
		if err != nil {
			return c, err
		}
		snapshot, err := d.Runner.SealSnapshot(ctx, request)
		if err != nil {
			return c, err
		}
		if snapshot.Workspace.Revision != c.WorkspaceRevision || snapshot.Workspace.Epoch != c.RunnerEpoch {
			return c, domain.ErrReconciliation
		}
		ref, err := d.publish(ctx, snapshot.Artifact)
		if err != nil {
			return c, err
		}
		if err = d.fault("cleanup_after_snapshot_publication"); err != nil {
			return c, err
		}
		if err = d.Store.SealWorkspaceCleanup(ctx, c, ref); err != nil {
			return c, err
		}
		c.Phase, c.SnapshotRef = "sealed", ref
	}
	request, err := grant(true)
	if err != nil {
		return c, err
	}
	result, err := d.Runner.ReleaseWorkspace(ctx, request)
	if err != nil {
		return c, err
	}
	if !result.Released || result.WorkspaceID != c.RunID {
		return c, domain.ErrReconciliation
	}
	if err = d.fault("cleanup_after_release_before_commit"); err != nil {
		return c, err
	}
	if err = d.Store.CompleteWorkspaceCleanup(ctx, c); err != nil {
		return c, err
	}
	c.Phase = "released"
	return c, nil
}
