package runner

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
)

func (e *Engine) candidatePath(o Operation) string {
	base := e.storageBase(o.Request.WorkspaceID)
	if base == "" {
		base = filepath.Join(e.config.RootDir, "candidates", string(o.Request.WorkspaceID))
	}
	return filepath.Join(base, "candidate-"+string(o.Request.OperationID))
}

// cleanCandidate reconstructs exactly the bounded published tree: the trusted
// baseline plus additions, deletions, mode changes and content replacements.
// Ignored runtime caches and virtual environments never enter this manifest.
// The directory is outside the writable checkout and is mounted read-only.
func (e *Engine) cleanCandidate(ctx context.Context, o Operation, current tree) (string, error) {
	w, err := e.journal.workspace(ctx, o.Request.WorkspaceID)
	if err != nil {
		return "", err
	}
	baseline, err := e.readTree(ctx, e.baselinePath(w.ID))
	if err != nil {
		return "", err
	}
	if treeHash(baseline) != w.BaselineHash || treeHash(current) != o.BeforeHash {
		return "", domain.ErrReconciliation
	}
	candidate := make(tree, len(current))
	for name, record := range baseline {
		if _, ok := current[name]; ok {
			candidate[name] = record
		}
	}
	for name, record := range current {
		candidate[name] = record
	}
	path := e.candidatePath(o)
	if _, err = os.Lstat(path); err == nil {
		// Stable operation IDs never overwrite an already-built candidate after a
		// crash. Incomplete construction requires reconciliation, never replay.
		existing, err := e.readTree(ctx, path)
		if err != nil || treeHash(existing) != o.BeforeHash {
			return "", domain.ErrReconciliation
		}
	} else if !os.IsNotExist(err) {
		return "", err
	} else if err = e.copyTree(ctx, candidate, path); err != nil {
		return "", err
	}
	if err = os.Chmod(path, 0755); err != nil {
		return "", err
	}
	return path, nil
}
func (e *Engine) validateVerification(ctx context.Context, o Operation, afterHash string) error {
	if o.Request.Kind != "verify" {
		return nil
	}
	if afterHash != o.BeforeHash {
		return fmt.Errorf("%w: live candidate changed during trusted verification", domain.ErrReconciliation)
	}
	candidate, err := e.readTree(ctx, e.candidatePath(o))
	if err != nil {
		return err
	}
	if treeHash(candidate) != o.BeforeHash {
		return fmt.Errorf("%w: clean verification candidate changed", domain.ErrReconciliation)
	}
	return nil
}
