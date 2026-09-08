//go:build linux

package sandbox

import (
	"context"
	"testing"
)

func TestOrdinaryDirectoryIsNotAQuota(t *testing.T) {
	if err := (FilesystemCapacityQuota{}).VerifyWorkspaceQuota(context.Background(), t.TempDir(), 1<<60); err == nil {
		t.Fatal("ordinary host directory falsely treated as a bounded dedicated filesystem")
	}
}
