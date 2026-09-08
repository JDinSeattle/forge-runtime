//go:build linux

package sandbox

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

// VolumeSpec pins a pre-mounted ext4 loop image. Operators provision the image;
// the runner only checks identity and leases a child directory. No mount or
// resize command is executed by this adapter.
type VolumeSpec struct {
	ID             string `json:"id"`
	MountPath      string `json:"mount_path"`
	ImagePath      string `json:"image_path"`
	ImageDevice    uint64 `json:"image_device"`
	ImageInode     uint64 `json:"image_inode"`
	ImageBytes     int64  `json:"image_bytes"`
	Device         string `json:"device"`
	FilesystemUUID string `json:"filesystem_uuid"`
	MaxInodes      uint64 `json:"max_inodes"`
}

type FixedVolumeQuota struct{ Slots []VolumeSpec }

func (q FixedVolumeQuota) VerifyWorkspaceQuota(ctx context.Context, directory string, maxBytes int64) error {
	for _, s := range q.Slots {
		rel, err := filepath.Rel(s.MountPath, directory)
		if err != nil || rel == "." || strings.HasPrefix(rel, "..") || filepath.IsAbs(rel) {
			continue
		}
		if s.ImageBytes > maxBytes {
			return fmt.Errorf("volume image exceeds profile physical quota")
		}
		if err = VerifyVolume(ctx, s); err != nil {
			return err
		}
		var a, b syscall.Stat_t
		if err = syscall.Stat(directory, &a); err != nil {
			return err
		}
		if err = syscall.Stat(s.MountPath, &b); err != nil {
			return err
		}
		if a.Dev != b.Dev {
			return fmt.Errorf("workspace is on a replacement filesystem")
		}
		return nil
	}
	return fmt.Errorf("workspace is not within the configured fixed volume pool")
}

func VerifyVolume(ctx context.Context, s VolumeSpec) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.ID == "" || !filepath.IsAbs(s.MountPath) || !filepath.IsAbs(s.ImagePath) || s.ImageBytes <= 0 || s.ImageDevice == 0 || s.ImageInode == 0 || s.MaxInodes == 0 {
		return fmt.Errorf("incomplete fixed volume identity")
	}
	for _, p := range []string{s.MountPath, s.ImagePath} {
		resolved, err := filepath.EvalSymlinks(p)
		if err != nil {
			return err
		}
		if resolved != filepath.Clean(p) {
			return fmt.Errorf("volume paths must not contain symlinks")
		}
	}
	info, err := os.Lstat(s.ImagePath)
	if err != nil {
		return err
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || info.Mode().Perm()&0022 != 0 || info.Size() != s.ImageBytes || st.Dev != s.ImageDevice || st.Ino != s.ImageInode {
		return fmt.Errorf("backing image identity or protection changed")
	}
	f, err := os.Open(s.ImagePath)
	if err != nil {
		return err
	}
	defer f.Close()
	header := make([]byte, 120)
	if _, err = f.ReadAt(header, 1024); err != nil {
		return err
	}
	uuid := strings.ReplaceAll(strings.ToLower(s.FilesystemUUID), "-", "")
	if len(uuid) != 32 || hex.EncodeToString(header[104:120]) != uuid || header[56] != 0x53 || header[57] != 0xef {
		return fmt.Errorf("ext4 backing image UUID mismatch")
	}
	var sx unix.Statx_t
	if err = unix.Statx(unix.AT_FDCWD, s.MountPath, unix.AT_SYMLINK_NOFOLLOW, unix.STATX_MNT_ID, &sx); err != nil {
		return err
	}
	data, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return err
	}
	matched := false
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 10 {
			continue
		}
		decodedMount := strings.NewReplacer(`\040`, " ", `\011`, "\t", `\134`, `\`).Replace(fields[4])
		if strings.HasPrefix(decodedMount, strings.TrimSuffix(s.MountPath, "/")+"/") {
			return fmt.Errorf("nested mount inside fixed volume")
		}
		mid, _ := strconv.ParseUint(fields[0], 10, 64)
		if mid != sx.Mnt_id {
			continue
		}
		split := strings.Index(line, " - ")
		if split < 0 {
			return fmt.Errorf("invalid mount information")
		}
		tail := strings.Fields(line[split+3:])
		mount := strings.NewReplacer(`\040`, " ", `\011`, "\t", `\134`, `\`).Replace(fields[4])
		flags := "," + fields[5] + ","
		if mount != s.MountPath || fields[2] != s.Device || fields[3] != "/" || tail[0] != "ext4" || !strings.Contains(flags, ",rw,") || !strings.Contains(flags, ",nodev,") || !strings.Contains(flags, ",nosuid,") {
			return fmt.Errorf("fixed volume mount identity or flags changed")
		}
		matched = true
	}
	if !matched {
		return fmt.Errorf("fixed volume mount missing")
	}
	sysbase := filepath.Join("/sys/dev/block", s.Device)
	backing, err := os.ReadFile(filepath.Join(sysbase, "loop/backing_file"))
	if err != nil {
		return err
	}
	if strings.TrimSpace(string(backing)) != s.ImagePath {
		return fmt.Errorf("loop backing path mismatch")
	}
	for name, want := range map[string]int64{"loop/offset": 0, "loop/sizelimit": s.ImageBytes, "size": s.ImageBytes / 512} {
		raw, err := os.ReadFile(filepath.Join(sysbase, name))
		if err != nil {
			return err
		}
		got, err := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)
		if err != nil || got != want {
			return fmt.Errorf("loop %s capacity changed", name)
		}
	}
	var fs syscall.Statfs_t
	if err = syscall.Statfs(s.MountPath, &fs); err != nil {
		return err
	}
	if fs.Type != 0xef53 || fs.Bsize <= 0 || fs.Blocks > uint64(s.ImageBytes/fs.Bsize) || fs.Blocks == 0 || fs.Files == 0 || fs.Files > s.MaxInodes {
		return fmt.Errorf("fixed filesystem capacity or inode ceiling changed")
	}
	return nil
}
