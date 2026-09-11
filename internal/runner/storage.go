package runner

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/sandbox"
)

func (e *Engine) openStorage() error {
	if len(e.config.VolumeSlots) == 0 {
		if _, ok := e.config.Backend.(*sandbox.TestBackend); !ok {
			return fmt.Errorf("%w: production runner requires a fixed volume pool", sandbox.ErrUnavailable)
		}
		return nil
	}
	if e.config.TestVolumeVerifier != nil {
		if _, ok := e.config.Backend.(*sandbox.TestBackend); !ok {
			return domain.ErrInvalid
		}
	}
	e.slots = map[string]sandbox.VolumeSpec{}
	e.allocations = map[domain.ID]string{}
	seen := map[string]bool{}
	for _, slot := range e.config.VolumeSlots {
		if domain.ID(slot.ID).Validate() != nil || !filepath.IsAbs(slot.MountPath) || e.slots[slot.ID].ID != "" || seen[slot.MountPath] {
			return domain.ErrInvalid
		}
		if err := e.verifySlot(context.Background(), slot); err != nil {
			return err
		}
		// This lock is on the fixed volume itself, so a different runner root or
		// journal cannot accidentally lease the same physical slot concurrently.
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		unlock, err := lockWorkspace(ctx, filepath.Join(slot.MountPath, ".forge-pool.lock"))
		cancel()
		if err != nil {
			return fmt.Errorf("exclusive pool ownership: %w", err)
		}
		e.storageUnlocks = append(e.storageUnlocks, unlock)
		ownerPath := filepath.Join(slot.MountPath, ".forge-pool.owner")
		owner := []byte(e.config.JournalPath + "\n")
		file, createErr := os.OpenFile(ownerPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if createErr == nil {
			_, createErr = file.Write(owner)
			if createErr == nil {
				createErr = file.Sync()
			}
			file.Close()
			if createErr != nil {
				return createErr
			}
		} else if !os.IsExist(createErr) {
			return createErr
		}
		parent, syncErr := os.Open(slot.MountPath)
		if syncErr != nil {
			return syncErr
		}
		syncErr = parent.Sync()
		parent.Close()
		if syncErr != nil {
			return syncErr
		}
		storedOwner, readErr := os.ReadFile(ownerPath)
		if readErr != nil {
			return readErr
		}
		if string(storedOwner) != string(owner) {
			return fmt.Errorf("%w: volume belongs to another runner journal", domain.ErrConflict)
		}

		raw, _ := json.Marshal(slot)
		_, err = e.journal.db.Exec(`INSERT INTO volume_slots(id,spec_json)VALUES(?,?) ON CONFLICT(id)DO NOTHING`, slot.ID, string(raw))
		if err != nil {
			return err
		}
		var stored string
		if err = e.journal.db.QueryRow(`SELECT spec_json FROM volume_slots WHERE id=?`, slot.ID).Scan(&stored); err != nil {
			return err
		}
		if stored != string(raw) {
			return fmt.Errorf("%w: persisted pool identity changed", domain.ErrConflict)
		}
		e.slots[slot.ID] = slot
		seen[slot.MountPath] = true
	}
	rows, err := e.journal.db.Query(`SELECT workspace_id,slot_id FROM volume_leases `)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var wid domain.ID
		var sid string
		if err = rows.Scan(&wid, &sid); err != nil {
			return err
		}
		if e.slots[sid].ID == "" {
			return fmt.Errorf("%w: retained workspace slot missing from configuration", domain.ErrReconciliation)
		}
		e.allocations[wid] = sid
	}
	if err = rows.Err(); err != nil {
		return err
	}
	rows.Close()
	var missing int
	if err = e.journal.db.QueryRow(`SELECT count(*) FROM workspaces w LEFT JOIN volume_leases l ON l.workspace_id=w.id WHERE w.released=0 AND l.workspace_id IS NULL`).Scan(&missing); err != nil {
		return err
	}
	if missing != 0 {
		return fmt.Errorf("%w: existing workspaces lack durable pool leases", domain.ErrReconciliation)
	}
	return nil
}
func (e *Engine) verifySlot(ctx context.Context, s sandbox.VolumeSpec) error {
	if e.config.TestVolumeVerifier != nil {
		return e.config.TestVolumeVerifier(ctx, s)
	}
	return sandbox.VerifyVolume(ctx, s)
}
func (e *Engine) verifyStorage(ctx context.Context, id domain.ID) error {
	e.mu.Lock()
	sid := e.allocations[id]
	slot := e.slots[sid]
	e.mu.Unlock()
	if sid == "" {
		return nil
	}
	return e.verifySlot(ctx, slot)
}
func (e *Engine) allocateStorage(ctx context.Context, r PrepareRequest, sourceHash string) error {
	if len(e.slots) == 0 {
		return nil
	}
	tx, err := e.journal.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var sid, tenant, run, source, profile, pinnedHash string
	var epoch uint64
	var released bool
	err = tx.QueryRowContext(ctx, `SELECT slot_id,tenant_id,run_id,source_id,profile_id,released,epoch,source_hash FROM volume_leases WHERE workspace_id=?`, r.WorkspaceID).Scan(&sid, &tenant, &run, &source, &profile, &released, &epoch, &pinnedHash)
	if err == nil {
		if tenant != string(r.TenantID) || run != string(r.RunID) || source != r.SourceID || profile != r.ProfileID {
			return domain.ErrConflict
		}
		if pinnedHash == "" {
			return fmt.Errorf("%w: legacy initialization has no pinned source hash", domain.ErrReconciliation)
		}
		if pinnedHash != sourceHash {
			return domain.ErrConflict
		}
		if r.Epoch < epoch {
			return domain.ErrFenced
		}
		if released {
			return domain.ErrFenced
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	} else {
		// Only configured slots participate, and a durable unknown/preparation lease
		// remains occupied until an explicit, fully settled ReleaseWorkspace.
		for _, s := range e.config.VolumeSlots {
			var occupied int
			if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM volume_leases WHERE slot_id=? AND released=0`, s.ID).Scan(&occupied); err != nil {
				return err
			}
			if occupied == 0 {
				sid = s.ID
				break
			}
		}
		if sid == "" {
			return domain.ErrCapacity
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO volume_leases(workspace_id,slot_id,tenant_id,run_id,source_id,profile_id,epoch,source_hash)VALUES(?,?,?,?,?,?,?,?)`, r.WorkspaceID, sid, r.TenantID, r.RunID, r.SourceID, r.ProfileID, r.Epoch, sourceHash)
		if err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, `UPDATE volume_leases SET epoch=? WHERE workspace_id=?`, r.Epoch, r.WorkspaceID); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	e.mu.Lock()
	e.allocations[r.WorkspaceID] = sid
	e.mu.Unlock()
	if err = e.verifyStorage(ctx, r.WorkspaceID); err != nil {
		return err
	}
	return e.faultAt("after_volume_lease", OperationRequest{WorkspaceRequest: r.WorkspaceRequest})
}
func (e *Engine) storageBase(id domain.ID) string {
	e.mu.Lock()
	defer e.mu.Unlock()
	if sid := e.allocations[id]; sid != "" {
		return filepath.Join(e.slots[sid].MountPath, "workspace-"+string(id))
	}
	return ""
}
func (e *Engine) releaseStorage(ctx context.Context, id domain.ID) error {
	base := e.storageBase(id)
	if base == "" {
		if len(e.slots) > 0 {
			return domain.ErrReconciliation
		}
		root, err := os.OpenRoot(e.config.RootDir)
		if err != nil {
			return err
		}
		defer root.Close()
		for _, parent := range []string{"workspaces", "baselines", "candidates"} {
			if err = root.RemoveAll(filepath.Join(parent, string(id))); err != nil {
				return err
			}
		}
		return nil
	}
	if err := e.verifyStorage(ctx, id); err != nil {
		return err
	}
	root, err := os.OpenRoot(filepath.Dir(base))
	if err != nil {
		return err
	}
	defer root.Close()
	if err = root.RemoveAll(filepath.Base(base)); err != nil {
		return err
	}
	f, err := root.Open(".")
	if err != nil {
		return err
	}
	err = f.Sync()
	f.Close()
	if err != nil {
		return err
	}
	// The lease is freed only after durable cleanup; a crash before this commit
	// retains capacity and repeating release is safe.
	_, err = e.journal.db.ExecContext(ctx, `UPDATE volume_leases SET released=1 WHERE workspace_id=?`, id)
	return err
}
