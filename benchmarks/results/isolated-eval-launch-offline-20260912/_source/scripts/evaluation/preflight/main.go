// Command eval-preflight checks the existing dedicated authority without opening
// an Engine, acquiring credentials, writing a journal, or starting a process.
package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"debug/buildinfo"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"syscall"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/configuration"
	"github.com/JDinSeattle/forge-runtime/internal/runnerclient"
	"github.com/JDinSeattle/forge-runtime/internal/sandbox"
	"golang.org/x/sys/unix"
	_ "modernc.org/sqlite"
)

type runnerConfig struct {
	Logs             sandbox.LogPolicy          `json:"logs,omitempty"`
	RootDir          string                     `json:"root_dir"`
	JournalPath      string                     `json:"journal_path"`
	ArtifactRoot     string                     `json:"artifact_root"`
	SigningKeyFile   string                     `json:"signing_key_file"`
	Sources          map[string]string          `json:"sources"`
	Profiles         map[string]sandbox.Profile `json:"profiles"`
	Server           runnerclient.ServerConfig  `json:"server"`
	DockerHost       string                     `json:"docker_host,omitempty"`
	VolumeSlots      []sandbox.VolumeSpec       `json:"volume_slots"`
	DockerBinary     string                     `json:"docker_binary,omitempty"`
	MaxArtifactBytes int64                      `json:"max_artifact_bytes,omitempty"`
	AllowTestBackend bool                       `json:"allow_test_backend,omitempty"`
}

var digestPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)
var revisionPattern = regexp.MustCompile(`^[a-f0-9]{40}$`)

func regular(path string, limit int64) error {
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil || !filepath.IsAbs(path) || resolved != path {
		return errors.New("canonical existing regular path required")
	}
	st, err := os.Lstat(path)
	if err != nil {
		return err
	}
	raw, ok := st.Sys().(*syscall.Stat_t)
	if !ok || raw.Nlink != 1 || raw.Uid != uint32(os.Getuid()) || !st.Mode().IsRegular() || st.Mode().Perm()&0022 != 0 || st.Size() > limit {
		return errors.New("protected bounded regular input required")
	}
	return nil
}
func decode(path string, v any) error {
	if err := regular(path, 2<<20); err != nil {
		return err
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	d := json.NewDecoder(io.LimitReader(f, 2<<20))
	d.DisallowUnknownFields()
	if err = d.Decode(v); err != nil {
		return err
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return errors.New("one JSON document required")
	}
	return nil
}
func binary(path, revision, pkg string) (map[string]any, error) {
	if err := regular(path, 256<<20); err != nil {
		return nil, err
	}
	st, err := os.Stat(path)
	if err != nil || st.Mode().Perm()&0100 == 0 {
		return nil, errors.New("owner-executable binary required")
	}
	info, err := buildinfo.ReadFile(path)
	if err != nil {
		return nil, err
	}
	settings := map[string]string{}
	for _, x := range info.Settings {
		settings[x.Key] = x.Value
	}
	if info.Path != "github.com/JDinSeattle/forge-runtime/"+pkg || settings["vcs.revision"] != revision || settings["vcs.modified"] != "false" || settings["GOOS"] != "linux" || settings["GOARCH"] != "amd64" {
		return nil, errors.New("binary package or clean source revision differs")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	h := sha256.New()
	if _, err = io.Copy(h, f); err != nil {
		return nil, err
	}
	return map[string]any{"path": path, "sha256": hex.EncodeToString(h.Sum(nil)), "package": info.Path, "go_version": info.GoVersion, "revision": revision, "modified": false}, nil
}
func journal(ctx context.Context, c runnerConfig, want string) (map[string]any, error) {
	if !digestPattern.MatchString(want) {
		return nil, errors.New("pinned journal UUID required")
	}
	if err := regular(c.JournalPath, 512<<20); err != nil {
		return nil, err
	}
	u := url.URL{Scheme: "file", Path: c.JournalPath, RawQuery: url.Values{"mode": {"ro"}, "_pragma": {"busy_timeout(3000)", "query_only(ON)"}}.Encode()}
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var version int
	var id string
	if err = tx.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return nil, err
	}
	if err = tx.QueryRowContext(ctx, "SELECT id FROM journal_identity WHERE singleton=1").Scan(&id); err != nil {
		return nil, err
	}
	if version != 5 || id != want {
		return nil, errors.New("existing journal version or UUID differs")
	}
	counts := map[string]int{}
	for name, query := range map[string]string{
		"unsettled_operations":     "SELECT count(*) FROM operations WHERE status NOT IN('succeeded','failed','cancelled')",
		"unreleased_workspaces":    "SELECT count(*) FROM workspaces WHERE released<>1 OR active_operation<>''",
		"unreleased_leases":        "SELECT count(*) FROM volume_leases WHERE released<>1",
		"unremoved_log_containers": "SELECT count(*) FROM operation_logs WHERE cleanup_state<>'removed'",
	} {
		var n int
		if err = tx.QueryRowContext(ctx, query).Scan(&n); err != nil {
			return nil, err
		}
		counts[name] = n
		if n != 0 {
			return nil, fmt.Errorf("retained authority: %s", name)
		}
	}
	var slots int
	if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM volume_slots").Scan(&slots); err != nil {
		return nil, err
	}
	if slots != 4 || len(c.VolumeSlots) != 4 {
		return nil, errors.New("exact four-slot authority required")
	}
	seen := map[string]bool{}
	for _, slot := range c.VolumeSlots {
		if seen[slot.ID] {
			return nil, errors.New("duplicate slot")
		}
		seen[slot.ID] = true
		var raw string
		if err = tx.QueryRowContext(ctx, "SELECT spec_json FROM volume_slots WHERE id=?", slot.ID).Scan(&raw); err != nil {
			return nil, err
		}
		var prior sandbox.VolumeSpec
		if err = json.Unmarshal([]byte(raw), &prior); err != nil {
			return nil, err
		}
		if prior != slot {
			return nil, errors.New("persisted volume specification changed")
		}
	}
	rows, err := tx.QueryContext(ctx, "SELECT id FROM operations ORDER BY id LIMIT 10001")
	if err != nil {
		return nil, err
	}
	operations := []string{}
	for rows.Next() {
		var operation string
		if err = rows.Scan(&operation); err != nil {
			rows.Close()
			return nil, err
		}
		operations = append(operations, operation)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	if len(operations) > 10000 {
		return nil, errors.New("bounded dedicated journal inventory exceeded")
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	entries, err := filepath.Glob(filepath.Join(c.ArtifactRoot, ".runner-journal-*"))
	if err != nil || len(entries) != 1 {
		return nil, errors.New("exact sole artifact journal registry required")
	}
	nameHash := sha256.Sum256([]byte(c.JournalPath))
	if filepath.Base(entries[0]) != ".runner-journal-"+hex.EncodeToString(nameHash[:]) {
		return nil, errors.New("artifact registry pathname differs")
	}
	var registration struct {
		Path string `json:"journal_path"`
		ID   string `json:"journal_id"`
	}
	if err = decode(entries[0], &registration); err != nil {
		return nil, err
	}
	if registration.Path != c.JournalPath || registration.ID != want {
		return nil, errors.New("artifact journal identity differs")
	}
	return map[string]any{"journal_uuid": id, "version": version, "counts": counts, "slots": slots, "operation_ids": operations}, nil
}
func mounted(ctx context.Context, c runnerConfig) error {
	locks := []int{}
	defer func() {
		for _, fd := range locks {
			unix.Close(fd)
		}
	}()
	for _, slot := range c.VolumeSlots {
		if err := sandbox.VerifyVolume(ctx, slot); err != nil {
			return err
		}
		lock := filepath.Join(slot.MountPath, ".forge-pool.lock")
		if err := regular(lock, 4096); err != nil {
			return err
		}
		fd, err := unix.Open(lock, unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if err != nil {
			return err
		}
		locks = append(locks, fd)
		if err = unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
			return errors.New("another runner owns the fixed pool")
		}
		owner := filepath.Join(slot.MountPath, ".forge-pool.owner")
		if err = regular(owner, 4096); err != nil {
			return err
		}
		raw, err := os.ReadFile(owner)
		if err != nil {
			return err
		}
		if string(raw) != c.JournalPath+"\n" {
			return errors.New("pool owner pathname changed")
		}
	}
	return nil
}
func check(ctx context.Context, root, revision, mode, uuid string) (map[string]any, error) {
	if !revisionPattern.MatchString(revision) || (mode != "static" && mode != "live") {
		return nil, errors.New("explicit revision and static/live mode required")
	}
	if filepath.Base(filepath.Dir(root)) != "evaluation" || filepath.Base(filepath.Dir(filepath.Dir(root))) != "lr20260912_a" {
		return nil, errors.New("dedicated lifecycle evaluation scope required")
	}
	candidate := filepath.Join(root, "candidate")
	var c, authority runnerConfig
	if err := decode(filepath.Join(candidate, "runner.json"), &c); err != nil {
		return nil, err
	}
	scope := filepath.Dir(filepath.Dir(root))
	if err := decode(filepath.Join(scope, "runtime/runner.json"), &authority); err != nil {
		return nil, err
	}
	comparable := c
	comparable.Profiles = authority.Profiles
	comparable.Sources = authority.Sources
	if !reflect.DeepEqual(comparable, authority) || c.AllowTestBackend || len(c.VolumeSlots) != 4 {
		return nil, errors.New("candidate changes existing runner authority")
	}
	platform, err := configuration.Load(filepath.Join(candidate, "platform.json"))
	if err != nil {
		return nil, err
	}
	if err = platform.CheckSources(ctx); err != nil {
		return nil, err
	}
	if platform.Runner.UnixSocket != c.Server.UnixSocket || platform.ArtifactRoot != c.ArtifactRoot || platform.SigningKeyFile != c.SigningKeyFile {
		return nil, errors.New("platform runner authority differs")
	}
	binaries := map[string]any{}
	for name, pkg := range map[string]string{"forge": "cmd/forge", "forge-api": "cmd/forge-api", "forge-worker": "cmd/forge-worker", "forge-runner": "cmd/forge-runner", "forge-admin": "cmd/forge-admin", "eval-provision": "scripts/evaluation/provision", "eval-preflight": "scripts/evaluation/preflight"} {
		value, e := binary(filepath.Join(root, "bin", name), revision, pkg)
		if e != nil {
			return nil, fmt.Errorf("%s: %w", name, e)
		}
		binaries[name] = value
	}
	result := map[string]any{"mode": mode, "revision": revision, "binaries": binaries, "sources_checked": len(platform.Sources), "engine_opened": false, "credentials_read": false}
	if mode == "live" {
		if err = mounted(ctx, c); err != nil {
			return nil, err
		}
		state, e := journal(ctx, c, uuid)
		if e != nil {
			return nil, e
		}
		result["journal"] = state
		result["volumes_checked"] = 4
	}
	return result, nil
}
func main() {
	f := flag.NewFlagSet("eval-preflight", flag.ContinueOnError)
	root := f.String("root", "", "absolute dedicated evaluation root")
	revision := f.String("revision", "", "full frozen revision")
	mode := f.String("mode", "", "static or live")
	uuid := f.String("journal-uuid", "", "existing journal identity")
	if f.Parse(os.Args[1:]) != nil || f.NArg() != 0 {
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	r, err := check(ctx, *root, *revision, *mode, *uuid)
	if err != nil {
		fmt.Fprintln(os.Stderr, "preflight rejected:", err)
		os.Exit(1)
	}
	if json.NewEncoder(os.Stdout).Encode(r) != nil {
		os.Exit(1)
	}
}
