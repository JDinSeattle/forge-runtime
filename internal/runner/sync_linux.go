//go:build linux

package runner

import (
	"context"
	"os"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/sandbox"
	"golang.org/x/sys/unix"
)

// A successful process exit does not imply that repository writes reached the
// filesystem journal. Flush the entire dedicated volume before publishing its
// operation receipt, including directory metadata and task-owned private files.
func (e *Engine) syncWorkspace(ctx context.Context, id domain.ID) error {
	if _, test := e.config.Backend.(*sandbox.TestBackend); test {
		return nil
	}
	if err := e.verifyStorage(ctx, id); err != nil {
		return err
	}
	root, err := os.OpenRoot(e.workspacePath(id))
	if err != nil {
		return err
	}
	defer root.Close()
	f, err := root.Open(".")
	if err != nil {
		return err
	}
	defer f.Close()
	return unix.Syncfs(int(f.Fd()))
}
