package runner

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"strings"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
)

// resumeImport may only run before the workspace row (and therefore every
// operation FK) exists. Existing bytes must be an exact subset of the pinned
// source. It never overwrites, deletes, ignores caches or guesses around damage.
func (e *Engine) resumeImport(ctx context.Context, expected tree, directory string, r PrepareRequest) error {
	if info, err := os.Lstat(directory); os.IsNotExist(err) {
		if err = os.MkdirAll(directory, 0700); err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else if !info.IsDir() {
		return domain.ErrReconciliation
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return err
	}
	defer root.Close()
	missing := make(tree, len(expected))
	for name, value := range expected {
		missing[name] = value
	}
	err = fs.WalkDir(root.FS(), ".", func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if name == "." {
			return nil
		}
		if entry.IsDir() {
			for file := range expected {
				if strings.HasPrefix(file, name+"/") {
					return nil
				}
			}
			return fmt.Errorf("%w: unexpected initialization directory", domain.ErrReconciliation)
		}
		record, ok := expected[name]
		if !ok {
			return fmt.Errorf("%w: unexpected initialization file", domain.ErrReconciliation)
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || info.Size() != int64(len(record.Content)) {
			return domain.ErrReconciliation
		}
		raw, err := root.ReadFile(name)
		if err != nil {
			return err
		}
		if hashBytes(raw) != record.SHA256 || (info.Mode().Perm()&0111 != 0) != record.Executable {
			return domain.ErrReconciliation
		}
		delete(missing, name)
		return nil
	})
	if err != nil {
		return err
	}
	for _, name := range sortedPaths(missing) {
		if path.Clean(name) != name {
			return domain.ErrInvalid
		}
		if err = e.copyTree(ctx, tree{name: missing[name]}, directory); err != nil {
			return err
		}
		if err = e.faultAt("after_import_file", OperationRequest{WorkspaceRequest: r.WorkspaceRequest}); err != nil {
			return err
		}
	}
	return nil
}

// InitializationState is local operator evidence, not a remote execution API.
type InitializationState struct {
	WorkspaceID     domain.ID `json:"workspace_id"`
	TenantID        domain.ID `json:"tenant_id"`
	RunID           domain.ID `json:"run_id"`
	SlotID          string    `json:"slot_id"`
	SourceID        string    `json:"source_id"`
	ProfileID       string    `json:"profile_id"`
	Epoch           uint64    `json:"epoch"`
	SourceHash      string    `json:"source_hash"`
	Released        bool      `json:"released"`
	WorkspaceExists bool      `json:"workspace_exists"`
	Operations      int       `json:"operations"`
}

func (e *Engine) InspectInitialization(ctx context.Context, id domain.ID) (InitializationState, error) {
	var state InitializationState
	if id.Validate() != nil {
		return state, domain.ErrInvalid
	}
	err := e.journal.db.QueryRowContext(ctx, `SELECT workspace_id,tenant_id,run_id,slot_id,source_id,profile_id,epoch,source_hash,released FROM volume_leases WHERE workspace_id=?`, id).Scan(&state.WorkspaceID, &state.TenantID, &state.RunID, &state.SlotID, &state.SourceID, &state.ProfileID, &state.Epoch, &state.SourceHash, &state.Released)
	if err != nil {
		return state, err
	}
	_, err = e.journal.workspace(ctx, id)
	state.WorkspaceExists = err == nil
	if err != nil && !errors.Is(err, domain.ErrNotFound) {
		return state, err
	}
	err = e.journal.db.QueryRowContext(ctx, `SELECT count(*) FROM operations WHERE workspace_id=?`, id).Scan(&state.Operations)
	return state, err
}
