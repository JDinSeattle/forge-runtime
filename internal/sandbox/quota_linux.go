//go:build linux

package sandbox

import (
	"context"
	"fmt"
	"math"
	"path/filepath"
	"syscall"
)

// FilesystemCapacityQuota verifies a dedicated bounded filesystem mounted at
// exactly the workspace directory. A fixed-size ext4/XFS/btrfs filesystem is an
// enforceable byte boundary: ENOSPC occurs in-kernel while commands are running.
// Provisioning and resizing require the trusted operator. This does not mount,
// format, resize, or claim that a directory on an ordinary host filesystem is a quota.
type FilesystemCapacityQuota struct{}

func (FilesystemCapacityQuota) VerifyWorkspaceQuota(ctx context.Context, directory string, maxBytes int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !filepath.IsAbs(directory) || maxBytes <= 0 {
		return fmt.Errorf("invalid workspace quota request")
	}
	var current, parent syscall.Statfs_t
	if err := syscall.Statfs(directory, &current); err != nil {
		return err
	}
	if err := syscall.Statfs(filepath.Dir(directory), &parent); err != nil {
		return err
	}
	if current.Fsid == parent.Fsid {
		return fmt.Errorf("workspace must be a dedicated filesystem mount, not an unbounded directory")
	}
	switch uint64(current.Type) {
	case 0xef53, 0x58465342, 0x9123683e:
	default:
		return fmt.Errorf("workspace quota requires a durable ext4, XFS or btrfs filesystem")
	}
	if current.Bsize <= 0 || current.Blocks > uint64(math.MaxInt64/current.Bsize) {
		return fmt.Errorf("invalid filesystem capacity")
	}
	capacity := int64(current.Blocks) * current.Bsize
	if capacity <= 0 || capacity > maxBytes {
		return fmt.Errorf("filesystem capacity %d exceeds workspace quota %d", capacity, maxBytes)
	}
	return nil
}
