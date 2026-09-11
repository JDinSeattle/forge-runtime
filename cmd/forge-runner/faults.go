package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/runner"
)

type faultPlan struct {
	Point       string    `json:"point"`
	Action      string    `json:"action"`
	TenantID    domain.ID `json:"tenant_id"`
	RunID       domain.ID `json:"run_id"`
	WorkspaceID domain.ID `json:"workspace_id"`
	OperationID domain.ID `json:"operation_id,omitempty"`
	Epoch       uint64    `json:"epoch"`
	ExpiresAt   time.Time `json:"expires_at"`
	DelayMillis int       `json:"delay_millis,omitempty"`
}
type faultController struct {
	plan faultPlan
	file string
	mu   sync.Mutex
}

var allowedFaultPoints = map[string]bool{"after_volume_lease": true, "after_import_file": true, "after_import_before_workspace": true, "after_prepared": true, "after_docker_create": true, "after_docker_start": true, "after_job_start": true, "after_job_exit": true, "before_receipt": true, "after_receipt_before_commit": true, "after_patch_file": true, "after_start_acceptance": true}

func loadFaultController(filename, root string) (*faultController, error) {
	if !filepath.IsAbs(filename) || filepath.Dir(filename) != filepath.Join(root, "operator-faults") {
		return nil, errors.New("fault plan must be a direct child of runner root/operator-faults")
	}
	resolved, err := filepath.EvalSymlinks(filename)
	if err != nil {
		return nil, err
	}
	if resolved != filename {
		return nil, errors.New("fault plan symlinks forbidden")
	}
	for _, p := range []string{filepath.Dir(filename), filename} {
		info, err := os.Lstat(p)
		if err != nil {
			return nil, err
		}
		if info.Mode().Perm()&0077 != 0 {
			return nil, errors.New("fault plan and directory must be owner-only")
		}
		owner, ok := info.Sys().(*syscall.Stat_t)
		if !ok || owner.Uid != uint32(os.Geteuid()) {
			return nil, errors.New("fault plan and directory must belong to runner UID")
		}
	}
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > 8192 {
		return nil, errors.New("invalid fault plan file")
	}
	var p faultPlan
	dec := json.NewDecoder(io.LimitReader(f, 8193))
	dec.DisallowUnknownFields()
	if err = dec.Decode(&p); err != nil {
		return nil, errors.New("invalid fault plan JSON")
	}
	var extra any
	if dec.Decode(&extra) != io.EOF {
		return nil, errors.New("invalid trailing fault plan JSON")
	}
	init := strings.HasPrefix(p.Point, "after_import") || p.Point == "after_volume_lease"
	if !allowedFaultPoints[p.Point] || p.TenantID.Validate() != nil || p.RunID.Validate() != nil || p.WorkspaceID.Validate() != nil || p.Epoch == 0 || (!init && p.OperationID.Validate() != nil) || (init && p.OperationID != "") {
		return nil, errors.New("fault plan needs exact valid scope and a supported point")
	}
	now := time.Now()
	if !p.ExpiresAt.After(now) || p.ExpiresAt.After(now.Add(10*time.Minute)) {
		return nil, errors.New("fault plan expiry must be within ten minutes")
	}
	if p.Action != "exit" && p.Action != "delay" {
		return nil, errors.New("fault action must be exit or delay")
	}
	if p.Action == "delay" && (p.DelayMillis < 1 || p.DelayMillis > 10000) {
		return nil, errors.New("fault delay must be bounded to 1..10000ms")
	}
	if p.Action == "exit" && p.DelayMillis != 0 {
		return nil, errors.New("exit action must not carry a delay")
	}
	return &faultController{plan: p, file: filename}, nil
}
func (c *faultController) hit(point string, r runner.OperationRequest) error {
	p := c.plan
	if point != p.Point || r.TenantID != p.TenantID || r.RunID != p.RunID || r.WorkspaceID != p.WorkspaceID || r.OperationID != p.OperationID || r.Epoch != p.Epoch {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !p.ExpiresAt.After(time.Now()) {
		return nil
	}
	marker := c.file + ".used"
	f, err := os.OpenFile(marker, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if errors.Is(err, os.ErrExist) {
		return nil
	}
	if err != nil {
		return err
	}
	// Grant, args, prompts and credentials are intentionally absent.
	evidence := struct {
		faultPlan
		PID        int       `json:"pid"`
		ObservedAt time.Time `json:"observed_at"`
	}{p, os.Getpid(), time.Now().UTC()}
	data, err := json.Marshal(evidence)
	if err == nil {
		_, err = f.Write(append(data, '\n'))
	}
	if err == nil {
		err = f.Sync()
	}
	f.Close()
	if err != nil {
		return err
	}
	parent, err := os.Open(filepath.Dir(marker))
	if err != nil {
		return err
	}
	err = parent.Sync()
	parent.Close()
	if err != nil {
		return err
	}
	if p.Action == "exit" {
		os.Exit(86)
	}
	time.Sleep(time.Duration(p.DelayMillis) * time.Millisecond)
	return nil
}
func (c *faultController) String() string {
	return fmt.Sprintf("%s for workspace %s", c.plan.Point, c.plan.WorkspaceID)
}
