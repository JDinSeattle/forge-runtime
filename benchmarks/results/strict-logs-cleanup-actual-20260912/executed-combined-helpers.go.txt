//go:build linux

package applicationfaults

// Independent evidence readers: none imports runner's private spool parser or
// opens an Engine. Host side effects are reachable only from the opt-in test.
import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/artifact"
	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/runner"
	"github.com/JDinSeattle/forge-runtime/internal/runnerclient"
	flow "github.com/JDinSeattle/forge-runtime/internal/runtime"
	"github.com/JDinSeattle/forge-runtime/internal/sandbox"
	"golang.org/x/sys/unix"
	_ "modernc.org/sqlite"
)

type slAcceptance struct {
	Purpose      string `json:"purpose"`
	FixtureID    string `json:"fixture_id"`
	ScopeRoot    string `json:"scope_root"`
	PoolRoot     string `json:"pool_root"`
	RunnerConfig string `json:"runner_config"`
	EvidenceDir  string `json:"evidence_dir"`
	RunnerBinary string `json:"runner_binary"`
	RunnerSHA    string `json:"runner_sha256"`
	WorkerBinary string `json:"worker_binary"`
	WorkerSHA    string `json:"worker_sha256"`
	TestBinary   string `json:"test_binary"`
	TestSHA      string `json:"test_sha256"`
}
type slRunnerConfig struct {
	RootDir          string                     `json:"root_dir"`
	JournalPath      string                     `json:"journal_path"`
	ArtifactRoot     string                     `json:"artifact_root"`
	SigningKeyFile   string                     `json:"signing_key_file"`
	Sources          map[string]string          `json:"sources"`
	Profiles         map[string]sandbox.Profile `json:"profiles"`
	Server           runnerclient.ServerConfig  `json:"server"`
	DockerHost       string                     `json:"docker_host"`
	DockerBinary     string                     `json:"docker_binary"`
	VolumeSlots      []sandbox.VolumeSpec       `json:"volume_slots"`
	AllowTestBackend bool                       `json:"allow_test_backend"`
	Logs             sandbox.LogPolicy          `json:"logs"`
	MaxArtifactBytes int64                      `json:"max_artifact_bytes"`
}

func slSHA(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }
func slJSON(v any) []byte {
	b, e := json.Marshal(v)
	if e != nil {
		panic(e)
	}
	return b
}
func slSave(path string, v any) error {
	b, e := json.MarshalIndent(v, "", "  ")
	if e != nil {
		return e
	}
	return slWrite(path, append(b, '\n'))
}
func slWrite(path string, b []byte) error {
	f, e := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	_, e = f.Write(b)
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	if e != nil {
		return e
	}
	d, e := os.Open(filepath.Dir(path))
	if e != nil {
		return e
	}
	defer d.Close()
	return d.Sync()
}
func slRead(path string, limit int64) ([]byte, error) {
	// Walk ancestors with no-follow, including the actual file. A symlink in any
	// fixture evidence component cannot redirect a read to another authority.
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, domain.ErrInvalid
	}
	fd, e := unix.Open("/", unix.O_DIRECTORY|unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if e != nil {
		return nil, e
	}
	defer func() { unix.Close(fd) }()
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	for i, p := range parts {
		flags := unix.O_RDONLY | unix.O_CLOEXEC | unix.O_NOFOLLOW
		if i < len(parts)-1 {
			flags |= unix.O_DIRECTORY
		}
		next, err := unix.Openat(fd, p, flags, 0)
		if err != nil {
			return nil, err
		}
		unix.Close(fd)
		fd = next
	}
	var st unix.Stat_t
	if e = unix.Fstat(fd, &st); e != nil {
		return nil, e
	}
	if st.Mode&unix.S_IFMT != unix.S_IFREG || st.Nlink != 1 || st.Size > limit {
		return nil, domain.ErrInvalid
	}
	dup, e := unix.Dup(fd)
	if e != nil {
		return nil, e
	}
	f := os.NewFile(uintptr(dup), path)
	defer f.Close()
	return io.ReadAll(io.LimitReader(f, limit+1))
}
func slReadJSON(path string, v any) error {
	b, e := slRead(path, 16<<20)
	if e != nil {
		return e
	}
	return json.Unmarshal(b, v)
}
func slWithin(root, path string) bool {
	r, e := filepath.Rel(root, path)
	return e == nil && r != "." && r != ".." && !strings.HasPrefix(r, ".."+string(os.PathSeparator)) && filepath.Clean(path) == path
}
func slValidateContract(a slAcceptance, c slRunnerConfig) error {
	if a.Purpose != "worker-runner-sigterm-v1" || a.FixtureID != "lr20260912_a" || !filepath.IsAbs(a.ScopeRoot) || filepath.Base(a.ScopeRoot) != a.FixtureID || filepath.Base(filepath.Dir(a.ScopeRoot)) != "lifecycle-rehearsals" {
		return fmt.Errorf("fixture scope mismatch")
	}
	rt := filepath.Join(a.ScopeRoot, "runtime")
	_, attemptErr := lifecycleAttempt(a.ScopeRoot, a.EvidenceDir)
	if a.PoolRoot != filepath.Join(a.ScopeRoot, "pool-root") || a.RunnerConfig != filepath.Join(rt, "runner.json") || attemptErr != nil {
		return fmt.Errorf("acceptance paths mismatch")
	}
	if filepath.Dir(a.RunnerBinary) != filepath.Dir(a.WorkerBinary) || filepath.Dir(a.RunnerBinary) != filepath.Dir(a.TestBinary) {
		return fmt.Errorf("binary revision directories differ")
	}
	for _, x := range []struct{ path, name, hash string }{{a.RunnerBinary, "forge-runner", a.RunnerSHA}, {a.WorkerBinary, "forge-worker", a.WorkerSHA}, {a.TestBinary, "application-faults.test", a.TestSHA}} {
		if !lifecycleBinaryPath(a.ScopeRoot, x.path, x.name) || len(x.hash) != 64 {
			return fmt.Errorf("binary binding mismatch")
		}
		if _, e := hex.DecodeString(x.hash); e != nil {
			return e
		}
	}
	if c.RootDir != filepath.Join(rt, "engine") || c.JournalPath != filepath.Join(rt, "journal.sqlite") || c.ArtifactRoot != filepath.Join(rt, "artifacts") || c.SigningKeyFile != filepath.Join(rt, "runner.key") || c.AllowTestBackend || c.DockerHost != "unix:///run/user/1000/forge-runtime-docker.sock" {
		return fmt.Errorf("production runner authority differs")
	}
	if len(c.Sources) != 1 || c.Sources["lifecycle"] != filepath.Join(rt, "source") || len(c.Profiles) != 1 {
		return fmt.Errorf("source/profile allowlist differs")
	}
	p, ok := c.Profiles["lifecycle-python"]
	if !ok || p.ID != "lifecycle-python" || p.User != "1000:1000" || p.MemoryBytes != 256<<20 || p.WorkspaceQuotaBytes != 256<<20 || p.CPUs != 1 || p.PIDs != 64 || p.Image != "python@sha256:229a2c5bfa27522db7815ea81f9bed70af17ccb9de9fc7ad142b1877b5830d36" || p.TrustedTestsDir != "" || p.TmpfsExecutable {
		return fmt.Errorf("profile limits differ")
	}
	target := []string{"python", "-I", "-B", "-c", "import runpy; a=runpy.run_path('/workspace/app.py'); assert a['clamp'](5,0,3)==3"}
	regression := []string{"python", "-I", "-B", "-c", "import runpy; a=runpy.run_path('/workspace/app.py'); assert a['clamp'](2,0,3)==2"}
	if !reflect.DeepEqual(p.TargetCommand, target) || !reflect.DeepEqual(p.VerifyCommand, regression) {
		return fmt.Errorf("trusted profile command differs")
	}
	defaults, _ := (sandbox.LogPolicy{}).Normalize()
	if c.Logs != defaults || len(c.VolumeSlots) != 4 || c.Server.UnixSocket != "/tmp/forge-lifecycle-"+a.FixtureID+"/runner.sock" || c.Server.TCPAddress != "" {
		return fmt.Errorf("default limits/four-slot/UDS contract differs")
	}
	seen := map[string]bool{}
	for i, v := range c.VolumeSlots {
		want := fmt.Sprintf("slot-%03d", i+1)
		if v.ID != want || seen[v.MountPath] || v.MountPath != filepath.Join(a.PoolRoot, "var/workspace-storage/mounts", want) || v.ImageBytes != 256<<20 || !slWithin(a.PoolRoot, v.ImagePath) {
			return fmt.Errorf("fixed slot identity differs")
		}
		seen[v.MountPath] = true
	}
	return nil
}

type slFrames struct {
	Bytes       int64     `json:"bytes"`
	Payload     int64     `json:"payload"`
	Records     uint64    `json:"records"`
	MaxEntry    int       `json:"max_entry"`
	StreamBytes [2]int    `json:"stream_bytes"`
	StreamSHA   [2]string `json:"stream_sha256"`
	PrefixSHA   string    `json:"prefix_sha256"`
	InvalidTail int       `json:"invalid_tail"`
	streams     [2][]byte
}

func slParseFrames(raw []byte, p sandbox.LogPolicy, allowTorn bool) (slFrames, error) {
	var out slFrames
	if len(raw) > p.OperationBytes {
		return out, fmt.Errorf("operation exceeds policy")
	}
	pos := 0
	for pos < len(raw) {
		if len(raw)-pos < 32 {
			if allowTorn {
				break
			}
			return out, fmt.Errorf("short header")
		}
		h := raw[pos : pos+32]
		n := int(binary.BigEndian.Uint32(h[16:20]))
		s := h[4]
		valid := string(h[:4]) == "FLG1" && (s == 1 || s == 2) && bytes.Equal(h[5:8], make([]byte, 3)) && bytes.Equal(h[24:32], make([]byte, 8)) && binary.BigEndian.Uint64(h[8:16]) == out.Records && n > 0 && n+32 <= p.EntryBytes && n <= len(raw)-pos-32
		if !valid {
			if allowTorn {
				break
			}
			return out, fmt.Errorf("invalid frame at %d", pos)
		}
		data := raw[pos+32 : pos+32+n]
		if crc32.ChecksumIEEE(data) != binary.BigEndian.Uint32(h[20:24]) {
			if allowTorn {
				break
			}
			return out, fmt.Errorf("CRC mismatch")
		}
		out.streams[s-1] = append(out.streams[s-1], data...)
		out.Payload += int64(n)
		out.Records++
		out.MaxEntry = max(out.MaxEntry, n+32)
		pos += 32 + n
	}
	out.Bytes = int64(pos)
	out.InvalidTail = len(raw) - pos
	out.PrefixSHA = slSHA(raw[:pos])
	for i := range 2 {
		out.StreamBytes[i] = len(out.streams[i])
		out.StreamSHA[i] = slSHA(out.streams[i])
	}
	return out, nil
}
func slBinding(r runner.OperationRequest) string {
	r.Grant = ""
	r.Deadline = r.Deadline.UTC()
	return slSHA(slJSON(r))
}
func slSameRequest(a, b runner.OperationRequest) bool {
	a.Grant = ""
	b.Grant = ""
	a.Deadline = a.Deadline.UTC()
	b.Deadline = b.Deadline.UTC()
	return bytes.Equal(slJSON(a), slJSON(b))
}

type slMeta struct {
	Summary  sandbox.LogSummary `json:"summary"`
	DataHash string             `json:"data_hash"`
	Preview  []byte             `json:"preview"`
}

func slValidateLog(op runner.Operation, request runner.OperationRequest, raw, meta []byte) (slFrames, error) {
	var job sandbox.Job
	if !slSameRequest(op.Request, request) || json.Unmarshal(op.Result, &job) != nil || job.Log == nil || !job.StrictLogs {
		return slFrames{}, fmt.Errorf("receipt/intent/strict-log binding")
	}
	s := job.Log
	p, e := s.Policy.Normalize()
	if e != nil || p != s.Policy || s.SchemaVersion != 1 || s.OperationID != request.OperationID || s.BindingHash != slBinding(request) {
		return slFrames{}, fmt.Errorf("summary identity/policy")
	}
	parsed, e := slParseFrames(raw, p, !s.Complete)
	if e != nil {
		return parsed, e
	}
	if parsed.Bytes != s.RetainedBytes || parsed.Payload != s.RetainedPayload || parsed.Records != s.Records || int64(len(raw)) != s.RetainedBytes {
		return parsed, fmt.Errorf("published bytes/counts differ")
	}
	if len(job.Output) > p.PreviewBytes || s.StdoutSeen < uint64(parsed.StreamBytes[0]) || s.StderrSeen < uint64(parsed.StreamBytes[1]) {
		return parsed, fmt.Errorf("preview/seen bound")
	}
	if s.Complete {
		if !s.DroppedKnown || s.StdoutSeen+s.StderrSeen < uint64(parsed.Payload) || s.DroppedBytes != s.StdoutSeen+s.StderrSeen-uint64(parsed.Payload) || s.TerminationRequested != s.TerminationObserved {
			return parsed, fmt.Errorf("complete drain equation/stop observation")
		}
	} else if s.DroppedKnown || s.DroppedBytes != 0 {
		return parsed, fmt.Errorf("gap cannot claim exact dropped total")
	}
	if (s.Truncated || s.Reason != "" || !s.Complete || s.TerminationRequested) && op.Status == runner.Succeeded {
		return parsed, fmt.Errorf("untrustworthy log falsely succeeded")
	}
	if meta != nil {
		var m slMeta
		if json.Unmarshal(meta, &m) != nil {
			return parsed, fmt.Errorf("invalid metadata")
		}
		expected := *s
		expected.Artifact = artifact.Ref{}
		if !reflect.DeepEqual(m.Summary, expected) || m.DataHash != slSHA(raw) || !bytes.Equal(m.Preview, job.Output) {
			return parsed, fmt.Errorf("sidecar/public receipt differ")
		}
	}
	ref := s.Artifact
	if ref.TenantID != request.TenantID || ref.RunID != request.RunID || ref.Kind != "operation_log" || ref.SHA256 != slSHA(raw) || ref.Size != int64(len(raw)) || ref.ObjectKey == "" {
		return parsed, fmt.Errorf("log object authority/hash")
	}
	return parsed, nil
}

type slJournal struct {
	Identity string                      `json:"identity"`
	Version  int                         `json:"version"`
	Tables   map[string][]map[string]any `json:"tables"`
}

func slJournalSnapshot(ctx context.Context, path string) (slJournal, error) {
	out := slJournal{Tables: map[string][]map[string]any{}}
	if _, e := slRead(path, 512<<20); e != nil {
		return out, e
	}
	u := url.URL{Scheme: "file", Path: path}
	q := u.Query()
	q.Set("mode", "ro")
	q.Set("_pragma", "busy_timeout(3000)")
	u.RawQuery = q.Encode()
	db, e := sql.Open("sqlite", u.String())
	if e != nil {
		return out, e
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	tx, e := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if e != nil {
		return out, e
	}
	defer tx.Rollback()
	if e = tx.QueryRowContext(ctx, "PRAGMA user_version").Scan(&out.Version); e != nil {
		return out, e
	}
	if e = tx.QueryRowContext(ctx, "SELECT id FROM journal_identity WHERE singleton=1").Scan(&out.Identity); e != nil {
		return out, e
	}
	for _, table := range []string{"volume_slots", "volume_leases", "workspaces", "operations", "operation_logs", "log_runs", "runner_artifacts"} {
		rows, err := tx.QueryContext(ctx, "SELECT * FROM "+table)
		if err != nil {
			return out, err
		}
		cols, err := rows.Columns()
		if err != nil {
			rows.Close()
			return out, err
		}
		out.Tables[table] = []map[string]any{}
		for rows.Next() {
			values := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range values {
				ptrs[i] = &values[i]
			}
			if err = rows.Scan(ptrs...); err != nil {
				rows.Close()
				return out, err
			}
			row := map[string]any{}
			for i, c := range cols {
				if b, ok := values[i].([]byte); ok {
					row[c] = string(b)
				} else {
					row[c] = values[i]
				}
			}
			if table == "operations" {
				var req runner.OperationRequest
				if err = json.Unmarshal([]byte(fmt.Sprint(row["request_json"])), &req); err != nil {
					rows.Close()
					return out, err
				}
				req.Grant = ""
				row["request_json"] = string(slJSON(req))
			}
			out.Tables[table] = append(out.Tables[table], row)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return out, err
		}
	}
	return out, tx.Commit()
}
func slNum(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case float64:
		return int64(x)
	case json.Number:
		n, _ := x.Int64()
		return n
	}
	return -1
}
func slIdle(j slJournal) error {
	if len(j.Identity) != 64 || j.Version != 5 || len(j.Tables["volume_slots"]) != 4 {
		return fmt.Errorf("journal/four-slot identity missing")
	}
	for _, r := range j.Tables["operations"] {
		if r["status"] != "succeeded" && r["status"] != "failed" && r["status"] != "cancelled" {
			return fmt.Errorf("unsettled operation %s", r["id"])
		}
	}
	for _, r := range j.Tables["workspaces"] {
		if slNum(r["released"]) != 1 || r["active_operation"] != "" {
			return fmt.Errorf("workspace retained %s", r["id"])
		}
	}
	for _, r := range j.Tables["volume_leases"] {
		if slNum(r["released"]) != 1 {
			return fmt.Errorf("volume still leased")
		}
	}
	return nil
}
func slRow(j slJournal, table, key string, value any) (map[string]any, error) {
	var found map[string]any
	for _, r := range j.Tables[table] {
		if reflect.DeepEqual(r[key], value) {
			if found != nil {
				return nil, fmt.Errorf("duplicate %s identity", table)
			}
			found = r
		}
	}
	if found == nil {
		return nil, fmt.Errorf("missing %s identity %v", table, value)
	}
	return found, nil
}
func slReservation(j slJournal, tenant, run domain.ID, p sandbox.LogPolicy, count int) error {
	var row map[string]any
	for _, r := range j.Tables["log_runs"] {
		if r["tenant_id"] == string(tenant) && r["run_id"] == string(run) {
			if row != nil {
				return fmt.Errorf("duplicate run reservation")
			}
			row = r
		}
	}
	if row == nil || slNum(row["reserved_bytes"]) != int64(count*p.OperationBytes) || slNum(row["operations"]) != int64(count) {
		return fmt.Errorf("reservation counters differ")
	}
	var stored sandbox.LogPolicy
	if json.Unmarshal([]byte(fmt.Sprint(row["policy_json"])), &stored) != nil || stored != p {
		return fmt.Errorf("reservation policy differs")
	}
	actual := 0
	for _, op := range j.Tables["operations"] {
		if op["tenant_id"] != string(tenant) || op["run_id"] != string(run) {
			continue
		}
		var req runner.OperationRequest
		if json.Unmarshal([]byte(fmt.Sprint(op["request_json"])), &req) != nil {
			return fmt.Errorf("invalid durable request")
		}
		if req.Kind != "run_command" && req.Kind != "verify" {
			continue
		}
		actual++
		lr, e := slRow(j, "operation_logs", "operation_id", op["id"])
		if e != nil {
			return e
		}
		var lp sandbox.LogPolicy
		if json.Unmarshal([]byte(fmt.Sprint(lr["policy_json"])), &lp) != nil || lp != p {
			return fmt.Errorf("per-operation reservation policy differs")
		}
	}
	if actual != count {
		return fmt.Errorf("durable process operation count differs")
	}
	return nil
}
func slVerifyPins(j slJournal, refs ...artifact.Ref) error {
	for _, ref := range refs {
		row, e := slRow(j, "runner_artifacts", "object_key", ref.ObjectKey)
		if e != nil {
			return e
		}
		var actual artifact.Ref
		if json.Unmarshal([]byte(fmt.Sprint(row["ref_json"])), &actual) != nil || actual != ref {
			return fmt.Errorf("pin metadata differs")
		}
	}
	return nil
}

// Call after slValidateLog has bound frames, counts, exact loss and receipt.
// A bounded multiplexed prefix has no cross-stream fairness guarantee. Both
// streams must still have been observed, including bytes discarded at the cap.
func slOverflowDrain(status runner.Status, job sandbox.Job, parsed slFrames) error {
	s := job.Log
	if s == nil || status == runner.Succeeded || !s.Complete || !s.Truncated || s.Reason != "output_limit" || !s.TerminationRequested || !s.TerminationObserved || sandbox.VerificationLogValid(job) || s.StdoutSeen == 0 || s.StderrSeen == 0 || parsed.Bytes < 512<<10-32 {
		return fmt.Errorf("actual default overflow/continued drain not proven")
	}
	return nil
}

func TestStrictLogsOverflowAllowsOneRetainedStream(t *testing.T) {
	policy, _ := (sandbox.LogPolicy{}).Normalize()
	req := runner.OperationRequest{WorkspaceRequest: runner.WorkspaceRequest{TenantID: "tenant", RunID: "run", WorkspaceID: "run", Epoch: 1}, OperationID: "op", Kind: "run_command"}
	for _, stream := range []byte{1, 2} {
		var raw []byte
		for seq := 0; seq < 32; seq++ {
			data := bytes.Repeat([]byte{'x'}, policy.EntryBytes-32)
			h := make([]byte, 32)
			copy(h, "FLG1")
			h[4] = stream
			binary.BigEndian.PutUint64(h[8:16], uint64(seq))
			binary.BigEndian.PutUint32(h[16:20], uint32(len(data)))
			binary.BigEndian.PutUint32(h[20:24], crc32.ChecksumIEEE(data))
			raw = append(raw, append(h, data...)...)
		}
		parsed, err := slParseFrames(raw, policy, false)
		if err != nil || parsed.StreamBytes[2-int(stream)] != 0 {
			t.Fatal("expected a valid one-stream prefix", err)
		}
		s := sandbox.LogSummary{SchemaVersion: 1, Policy: policy, OperationID: req.OperationID, BindingHash: slBinding(req), StdoutSeen: 524299, StderrSeen: 524299, RetainedBytes: int64(len(raw)), RetainedPayload: parsed.Payload, Records: parsed.Records, Complete: true, DroppedKnown: true, DroppedBytes: 1048598 - uint64(parsed.Payload), Truncated: true, Reason: "output_limit", TerminationRequested: true, TerminationObserved: true, Artifact: artifact.Ref{TenantID: "tenant", RunID: "run", Kind: "operation_log", ObjectKey: "key", SHA256: slSHA(raw), Size: int64(len(raw))}}
		job := sandbox.Job{StrictLogs: true, Log: &s, ExitCode: 137}
		op := runner.Operation{Request: req, Status: runner.Failed, Result: slJSON(job)}
		if _, err = slValidateLog(op, req, raw, nil); err != nil {
			t.Fatal(err)
		}
		if err = slOverflowDrain(op.Status, job, parsed); err != nil {
			t.Fatal("multiplexing order confused with missing drainage", err)
		}
		for _, mutate := range []func(*sandbox.LogSummary){
			func(s *sandbox.LogSummary) { s.StdoutSeen = 0 },
			func(s *sandbox.LogSummary) { s.StderrSeen = 0 },
			func(s *sandbox.LogSummary) { s.TerminationObserved = false },
			func(s *sandbox.LogSummary) { s.Complete = false },
		} {
			bad := s
			mutate(&bad)
			copyJob := job
			copyJob.Log = &bad
			if slOverflowDrain(op.Status, copyJob, parsed) == nil {
				t.Fatal("missing drainage/stop evidence accepted")
			}
		}
		if slOverflowDrain(runner.Succeeded, job, parsed) == nil {
			t.Fatal("overflow accepted as successful verification")
		}
	}
}

func TestStrictLogsOfflineOracle(t *testing.T) {
	p, _ := (sandbox.LogPolicy{}).Normalize()
	frame := func(seq uint64, stream byte, data []byte) []byte {
		h := make([]byte, 32)
		copy(h, "FLG1")
		h[4] = stream
		binary.BigEndian.PutUint64(h[8:16], seq)
		binary.BigEndian.PutUint32(h[16:20], uint32(len(data)))
		binary.BigEndian.PutUint32(h[20:24], crc32.ChecksumIEEE(data))
		return append(h, data...)
	}
	raw := append(frame(0, 1, []byte{0, 255, 'a'}), frame(1, 2, []byte("stderr"))...)
	parsed, e := slParseFrames(raw, p, false)
	if e != nil || parsed.Records != 2 || parsed.Payload != 9 || parsed.StreamBytes != [2]int{3, 6} {
		t.Fatal(parsed, e)
	}
	for _, mutate := range []func([]byte){func(b []byte) { b[0] = 'X' }, func(b []byte) { b[5] = 1 }, func(b []byte) { b[15] = 9 }, func(b []byte) { b[20] ^= 1 }, func(b []byte) { b[4] = 3 }, func(b []byte) { binary.BigEndian.PutUint32(b[16:20], 16384) }} {
		bad := bytes.Clone(raw)
		mutate(bad)
		if _, e = slParseFrames(bad, p, false); e == nil {
			t.Fatal("malformed frame accepted")
		}
	}
	torn := append(bytes.Clone(raw), []byte("FLG")...)
	prefix, e := slParseFrames(torn, p, true)
	if e != nil || prefix.Bytes != int64(len(raw)) || prefix.InvalidTail != 3 {
		t.Fatal("torn prefix not explicit")
	}
	req := runner.OperationRequest{WorkspaceRequest: runner.WorkspaceRequest{TenantID: "tenant", RunID: "run", WorkspaceID: "run", Epoch: 1}, OperationID: "op", Kind: "run_command", Deadline: time.Now().UTC()}
	sum := sandbox.LogSummary{SchemaVersion: 1, Policy: p, OperationID: "op", BindingHash: slBinding(req), StdoutSeen: 3, StderrSeen: 6, RetainedBytes: int64(len(raw)), RetainedPayload: 9, Records: 2, Complete: true, DroppedKnown: true, TerminationObserved: false, Artifact: artifact.Ref{TenantID: "tenant", RunID: "run", Kind: "operation_log", ObjectKey: "key", SHA256: slSHA(raw), Size: int64(len(raw))}}
	job := sandbox.Job{StrictLogs: true, Log: &sum}
	op := runner.Operation{Request: req, Status: runner.Succeeded, Result: slJSON(job)}
	if _, e = slValidateLog(op, req, raw, nil); e != nil {
		t.Fatal(e)
	}
	cases := map[string]func(*runner.Operation, *sandbox.LogSummary){"wrong receipt run": func(o *runner.Operation, s *sandbox.LogSummary) { o.Request.RunID = "other" }, "wrong artifact tenant": func(o *runner.Operation, s *sandbox.LogSummary) { s.Artifact.TenantID = "other" }, "wrong hash": func(o *runner.Operation, s *sandbox.LogSummary) { s.Artifact.SHA256 = strings.Repeat("0", 64) }, "gap with exact count": func(o *runner.Operation, s *sandbox.LogSummary) { s.Complete = false; o.Status = runner.Failed }, "forged success": func(o *runner.Operation, s *sandbox.LogSummary) { s.Truncated = true }, "seen mismatch": func(o *runner.Operation, s *sandbox.LogSummary) { s.StdoutSeen = 4 }, "missing stop": func(o *runner.Operation, s *sandbox.LogSummary) { s.TerminationRequested = true }}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			copyOp, copySum := op, sum
			change(&copyOp, &copySum)
			j := job
			j.Log = &copySum
			copyOp.Result = slJSON(j)
			if _, e := slValidateLog(copyOp, req, raw, nil); e == nil {
				t.Fatal("tampered chain accepted")
			}
		})
	}
	zoned := req
	zoned.Deadline = req.Deadline.In(time.FixedZone("west", -7*3600))
	zoned.Grant = "ephemeral"
	if !slSameRequest(req, zoned) || slBinding(req) != slBinding(zoned) {
		t.Fatal("semantic deadline/grant handling")
	}
	gap := slJournal{Version: 5, Identity: strings.Repeat("a", 64), Tables: map[string][]map[string]any{"volume_slots": make([]map[string]any, 4), "operations": {{"id": "op", "status": "unknown"}}}}
	if slIdle(gap) == nil {
		t.Fatal("unknown idle accepted")
	}
}

func slExampleContract() (slAcceptance, slRunnerConfig) {
	scope := "/home/fixture/forge-runtime/var/lifecycle-rehearsals/lr20260912_a"
	rt := filepath.Join(scope, "runtime")
	sum := strings.Repeat("a", 64)
	a := slAcceptance{Purpose: "worker-runner-sigterm-v1", FixtureID: "lr20260912_a", ScopeRoot: scope, PoolRoot: filepath.Join(scope, "pool-root"), RunnerConfig: filepath.Join(rt, "runner.json"), EvidenceDir: filepath.Join(scope, "evidence", "sigterm-01"), RunnerBinary: filepath.Join(scope, "bin", "forge-runner"), WorkerBinary: filepath.Join(scope, "bin", "forge-worker"), TestBinary: filepath.Join(scope, "bin", "application-faults.test"), RunnerSHA: sum, WorkerSHA: sum, TestSHA: sum}
	policy, _ := (sandbox.LogPolicy{}).Normalize()
	p := sandbox.Profile{ID: "lifecycle-python", Image: "python@sha256:229a2c5bfa27522db7815ea81f9bed70af17ccb9de9fc7ad142b1877b5830d36", User: "1000:1000", MemoryBytes: 256 << 20, WorkspaceQuotaBytes: 256 << 20, CPUs: 1, PIDs: 64, TargetCommand: []string{"python", "-I", "-B", "-c", "import runpy; a=runpy.run_path('/workspace/app.py'); assert a['clamp'](5,0,3)==3"}, VerifyCommand: []string{"python", "-I", "-B", "-c", "import runpy; a=runpy.run_path('/workspace/app.py'); assert a['clamp'](2,0,3)==2"}}
	c := slRunnerConfig{RootDir: filepath.Join(rt, "engine"), JournalPath: filepath.Join(rt, "journal.sqlite"), ArtifactRoot: filepath.Join(rt, "artifacts"), SigningKeyFile: filepath.Join(rt, "runner.key"), Sources: map[string]string{"lifecycle": filepath.Join(rt, "source")}, Profiles: map[string]sandbox.Profile{"lifecycle-python": p}, Logs: policy, DockerHost: "unix:///run/user/1000/forge-runtime-docker.sock", Server: runnerclient.ServerConfig{UnixSocket: "/tmp/forge-lifecycle-lr20260912_a/runner.sock"}}
	for i := 1; i <= 4; i++ {
		id := fmt.Sprintf("slot-%03d", i)
		c.VolumeSlots = append(c.VolumeSlots, sandbox.VolumeSpec{ID: id, ImagePath: filepath.Join(a.PoolRoot, "var/workspace-storage/images", id+".img"), MountPath: filepath.Join(a.PoolRoot, "var/workspace-storage/mounts", id), ImageBytes: 256 << 20})
	}
	return a, c
}
func TestStrictLogsOfflinePreflight(t *testing.T) {
	a, c := slExampleContract()
	if e := slValidateContract(a, c); e != nil {
		t.Fatal(e)
	}
	tests := map[string]func(*slAcceptance, *slRunnerConfig){
		"shared pool":    func(a *slAcceptance, c *slRunnerConfig) { c.VolumeSlots[0].MountPath = "/shared/mount" },
		"shared journal": func(a *slAcceptance, c *slRunnerConfig) { c.JournalPath = "/shared/journal.sqlite" },
		"three slots":    func(a *slAcceptance, c *slRunnerConfig) { c.VolumeSlots = c.VolumeSlots[:3] },
		"fake backend":   func(a *slAcceptance, c *slRunnerConfig) { c.AllowTestBackend = true },
		"unbounded profile": func(a *slAcceptance, c *slRunnerConfig) {
			p := c.Profiles["lifecycle-python"]
			p.MemoryBytes = 0
			c.Profiles["lifecycle-python"] = p
		},
		"wrong oracle": func(a *slAcceptance, c *slRunnerConfig) {
			p := c.Profiles["lifecycle-python"]
			p.TargetCommand[4] = "import runpy; a=runpy.run_path('/workspace/app.py'); pass"
			c.Profiles["lifecycle-python"] = p
		},
		"wrong daemon":         func(a *slAcceptance, c *slRunnerConfig) { c.DockerHost = "unix:///var/run/docker.sock" },
		"rebound source":       func(a *slAcceptance, c *slRunnerConfig) { c.Sources["lifecycle"] = "/shared/source" },
		"weaker default quota": func(a *slAcceptance, c *slRunnerConfig) { c.Logs.RunBytes = 2 << 20 },
		"foreign binary":       func(a *slAcceptance, c *slRunnerConfig) { a.RunnerBinary = "/usr/bin/true" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			aa, cc := slExampleContract()
			mutate(&aa, &cc)
			if slValidateContract(aa, cc) == nil {
				t.Fatal("unsafe fixture authority accepted")
			}
		})
	}
	if !slMapping([]byte("0 1000 1\n1 100000 65536\n")) || slMapping([]byte("0 0 4294967295\n")) || slMapping([]byte("0 1000 1\n")) {
		t.Fatal("host root or incomplete mapping accepted")
	}
	dir := t.TempDir()
	file := filepath.Join(dir, "owned")
	if e := os.WriteFile(file, []byte("bounded"), 0600); e != nil {
		t.Fatal(e)
	}
	link := filepath.Join(dir, "alias")
	if e := os.Symlink(file, link); e != nil {
		t.Fatal(e)
	}
	if _, e := slRead(link, 100); e == nil {
		t.Fatal("symlink evidence accepted")
	}
	if _, e := slRead(file, 2); e == nil {
		t.Fatal("oversized evidence accepted")
	}
	if b, e := slRead(file, 100); e != nil || string(b) != "bounded" {
		t.Fatal(e)
	}
	if !strings.Contains(slOverflowProgram, "range(128)") || !strings.Contains(slENOSPCProgram, "range(300)") || strings.Index(slCancelProgram, "signal.signal") > strings.Index(slCancelProgram, "os.fork") {
		t.Fatal("finite output or inherited cancellation program changed")
	}
}

func slStoredOperation(row map[string]any, op runner.Operation) error {
	var req runner.OperationRequest
	var receipt artifact.Ref
	if json.Unmarshal([]byte(fmt.Sprint(row["request_json"])), &req) != nil || json.Unmarshal([]byte(fmt.Sprint(row["receipt_json"])), &receipt) != nil || !slSameRequest(req, op.Request) || receipt != op.Receipt {
		return fmt.Errorf("durable operation intent/receipt differs")
	}
	result, e := domain.CanonicalJSON([]byte(fmt.Sprint(row["result_json"])))
	if e != nil {
		return e
	}
	expected, e := domain.CanonicalJSON(op.Result)
	if e != nil {
		return e
	}
	if row["id"] != string(op.Request.OperationID) || row["tenant_id"] != string(op.Request.TenantID) || row["run_id"] != string(op.Request.RunID) || row["workspace_id"] != string(op.Request.WorkspaceID) || row["status"] != string(op.Status) || row["job_id"] != op.JobID || row["before_hash"] != op.BeforeHash || row["after_hash"] != op.AfterHash || slNum(row["after_revision"]) != int64(op.AfterRevision) || !bytes.Equal(result, expected) {
		return fmt.Errorf("durable operation authority/result differs")
	}
	return nil
}
func TestStrictLogsDurableBindingOracle(t *testing.T) {
	op := runner.Operation{Request: runner.OperationRequest{WorkspaceRequest: runner.WorkspaceRequest{TenantID: "tenant", RunID: "run", WorkspaceID: "run", Epoch: 2}, OperationID: "op", Kind: "run_command", Deadline: time.Now().UTC()}, Status: runner.Failed, JobID: "job", Result: json.RawMessage(`{"strict_logs":true}`), AfterRevision: 3, Receipt: artifact.Ref{TenantID: "tenant", RunID: "run", Kind: "operation_receipt", ObjectKey: "receipt", SHA256: strings.Repeat("a", 64), Size: 100}}
	row := func() map[string]any {
		return map[string]any{"id": "op", "tenant_id": "tenant", "run_id": "run", "workspace_id": "run", "request_json": string(slJSON(op.Request)), "receipt_json": string(slJSON(op.Receipt)), "result_json": string(op.Result), "status": "failed", "job_id": "job", "before_hash": "", "after_hash": "", "after_revision": int64(3)}
	}
	if e := slStoredOperation(row(), op); e != nil {
		t.Fatal(e)
	}
	for _, key := range []string{"tenant_id", "run_id", "status", "job_id", "before_hash", "after_hash", "after_revision", "result_json", "request_json", "receipt_json"} {
		t.Run(key, func(t *testing.T) {
			bad := row()
			bad[key] = "tampered"
			if slStoredOperation(bad, op) == nil {
				t.Fatal("altered durable authority accepted")
			}
		})
	}
}

// A signal report is not a launch attestation. Check the separately captured
// original launch identity, exact expected argv, process start ticks and all
// available after-signal observations rather than trusting summary scalars.
type slProcessIdentity struct {
	PID        int64     `json:"pid"`
	ExePath    string    `json:"exe_path"`
	SHA        string    `json:"exe_sha256"`
	Ticks      string    `json:"proc_start_ticks"`
	ObservedAt time.Time `json:"observed_at"`
	Args       []string  `json:"args"`
}
type slSignalRecord struct {
	slProcessIdentity
	SentAt   time.Time `json:"signal_sent_at"`
	DBBefore time.Time `json:"db_before_signal"`
	DBAfter  time.Time `json:"db_after_signal_return"`
	ExitedAt time.Time `json:"exited_at"`
	ExitCode *int      `json:"exit_code"`
}

func slNumericTicks(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	n, e := strconv.ParseUint(s, 10, 64)
	return e == nil && n > 0
}
func slAuditSignal(launchRaw, signalRaw []byte, path, hash string, args []string) (slSignalRecord, error) {
	var launch slProcessIdentity
	var signal slSignalRecord
	if json.Unmarshal(launchRaw, &launch) != nil || json.Unmarshal(signalRaw, &signal) != nil {
		return signal, fmt.Errorf("invalid process record")
	}
	for _, p := range []slProcessIdentity{launch, signal.slProcessIdentity} {
		if p.PID <= 0 || p.PID > 1<<31-1 || !slNumericTicks(p.Ticks) || p.ExePath != path || p.SHA != hash || p.ObservedAt.IsZero() || !reflect.DeepEqual(p.Args, args) {
			return signal, fmt.Errorf("missing or mismatched process identity")
		}
	}
	if launch.PID != signal.PID || launch.Ticks != signal.Ticks || signal.ObservedAt.Before(launch.ObservedAt) || signal.SentAt.IsZero() || signal.DBBefore.IsZero() || signal.DBAfter.IsZero() || signal.ExitedAt.IsZero() || signal.DBBefore.Before(signal.ObservedAt) || signal.SentAt.Before(signal.DBBefore) || signal.DBAfter.Before(signal.SentAt) || signal.ExitedAt.Before(signal.DBAfter) || !signal.ExitedAt.After(signal.SentAt) || signal.ExitedAt.Sub(signal.SentAt) > 8*time.Second || signal.ExitCode == nil || *signal.ExitCode != 0 {
		return signal, fmt.Errorf("process launch/signal/exit binding differs")
	}
	return signal, nil
}
func slAuditAfterSignal(signal slSignalRecord, clockRaw, dockerRaw []byte, cid, jobID string) error {
	var clock struct {
		ObservedAt time.Time `json:"observed_at"`
	}
	var docker []struct {
		ID    string `json:"Id"`
		Name  string
		State struct{ Running bool }
	}
	if json.Unmarshal(clockRaw, &clock) != nil || json.Unmarshal(dockerRaw, &docker) != nil || clock.ObservedAt.IsZero() || clock.ObservedAt.Before(signal.ExitedAt) || len(docker) != 1 || docker[0].ID != cid || docker[0].Name != "/"+jobID || !docker[0].State.Running {
		return fmt.Errorf("after-signal raw does not bind original running container")
	}
	return nil
}
func slAuditSignalSummary(signal slSignalRecord, raw []byte) error {
	var summary sigtermBoundary
	if json.Unmarshal(raw, &summary) != nil || !summary.SentAt.Equal(signal.SentAt) || !summary.DBBefore.Equal(signal.DBBefore) || !summary.DBAfter.Equal(signal.DBAfter) || !summary.ExitedAt.Equal(signal.ExitedAt) {
		return fmt.Errorf("signal summary differs from exact signal record")
	}
	return nil
}

type slClaimSnapshot struct {
	TenantID   domain.ID  `json:"tenant_id"`
	RunID      domain.ID  `json:"run_id"`
	Version    uint64     `json:"version"`
	Body       flow.State `json:"body"`
	InputState flow.State `json:"input_state"`
	InputEvent flow.Event `json:"input_event"`
}
type slExitedRun struct {
	TenantID domain.ID  `json:"tenant_id"`
	ID       domain.ID  `json:"id"`
	State    string     `json:"state"`
	Version  uint64     `json:"version"`
	Owner    string     `json:"lease_owner"`
	Epoch    uint64     `json:"lease_epoch"`
	Until    time.Time  `json:"lease_until"`
	Snapshot flow.State `json:"snapshot"`
}
type slClaimAudit struct {
	Version        uint64       `json:"version"`
	OldLease       domain.Lease `json:"old_lease"`
	SuccessorLease domain.Lease `json:"successor_lease"`
	ClaimedAt      time.Time    `json:"claimed_at"`
	LiveRowSHA     string       `json:"live_row_sha256"`
}

func slFirstSuccessor(rows []json.RawMessage) (slClaimSnapshot, json.RawMessage, error) {
	var selected slClaimSnapshot
	var original json.RawMessage
	seen := map[uint64]bool{}
	for _, raw := range rows {
		var r slClaimSnapshot
		if json.Unmarshal(raw, &r) != nil {
			return selected, nil, fmt.Errorf("invalid claim snapshot")
		}
		if seen[r.Version] {
			return selected, nil, fmt.Errorf("duplicate snapshot version")
		}
		seen[r.Version] = true
		if r.InputEvent.Kind != flow.EventClaimed || r.InputEvent.Lease == nil || r.InputEvent.Lease.Owner != "lifecycle-successor-0" {
			continue
		}
		if original == nil || r.Version < selected.Version {
			selected, original = r, raw
		}
	}
	if original == nil {
		return selected, nil, fmt.Errorf("durable successor claim missing")
	}
	return selected, original, nil
}
func slAuditClaim(afterBoth, afterAdoption []byte, live []json.RawMessage, op runner.OperationRequest, summaryUntil, summaryClaim time.Time) (slClaimAudit, error) {
	var out slClaimAudit
	var before map[string]json.RawMessage
	var adopted map[string]json.RawMessage
	if json.Unmarshal(afterBoth, &before) != nil || json.Unmarshal(afterAdoption, &adopted) != nil {
		return out, fmt.Errorf("invalid PG capture")
	}
	var runs []slExitedRun
	var snapshots []json.RawMessage
	if json.Unmarshal(before["runs"], &runs) != nil || len(runs) != 1 || json.Unmarshal(adopted["run_snapshots"], &snapshots) != nil {
		return out, fmt.Errorf("missing exact PG run/snapshots")
	}
	old := runs[0]
	if old.TenantID != op.TenantID || old.ID != op.RunID || old.State != "running" || old.Version == 0 || old.Owner != "lifecycle-original-0" || old.Epoch != op.Epoch || old.Until.IsZero() {
		return out, fmt.Errorf("after-exit SQL lease identity differs")
	}
	historical, historicalRaw, e := slFirstSuccessor(snapshots)
	if e != nil {
		return out, e
	}
	current, currentRaw, e := slFirstSuccessor(live)
	if e != nil {
		return out, e
	}
	canonicalBefore, e := domain.CanonicalJSON(historicalRaw)
	if e != nil {
		return out, e
	}
	canonicalNow, e := domain.CanonicalJSON(currentRaw)
	if e != nil {
		return out, e
	}
	if !bytes.Equal(canonicalBefore, canonicalNow) {
		return out, fmt.Errorf("historical claimed snapshot differs from live PG")
	}
	event := current.InputEvent
	prior := current.InputState
	next := current.Body
	if historical.Version != current.Version || current.TenantID != op.TenantID || current.RunID != op.RunID || event.Lease == nil || event.At.IsZero() || event.Kind != flow.EventClaimed || event.Owner != "lifecycle-successor-0" || event.Epoch != op.Epoch+1 || event.Lease.Epoch != event.Epoch || event.Lease.Owner != event.Owner || event.Lease.Until.Sub(event.At) != 30*time.Second || event.At.Before(old.Until) || event.ExpectedVersion != old.Version || prior.Version != old.Version || current.Version != prior.Version+1 || next.Version != current.Version {
		return out, fmt.Errorf("durable claim epoch/version/30-second lease boundary differs")
	}
	expectedOld := old.Snapshot
	expectedOld.Lease = domain.Lease{Owner: old.Owner, Epoch: old.Epoch, Until: old.Until.UTC()}
	prior.Lease.Until = prior.Lease.Until.UTC()
	if !bytes.Equal(slJSON(expectedOld), slJSON(prior)) || prior.TenantID != op.TenantID || prior.RunID != op.RunID || next.TenantID != op.TenantID || next.RunID != op.RunID || next.Lease.Owner != event.Lease.Owner || next.Lease.Epoch != event.Lease.Epoch || !next.Lease.Until.Equal(event.Lease.Until) {
		return out, fmt.Errorf("claimed input/body not bound to post-exit SQL authority")
	}
	if !summaryUntil.Equal(old.Until) || !summaryClaim.Equal(event.At) {
		return out, fmt.Errorf("acceptance lease/claim summaries differ from durable PG")
	}
	out = slClaimAudit{Version: current.Version, OldLease: expectedOld.Lease, SuccessorLease: *event.Lease, ClaimedAt: event.At, LiveRowSHA: slSHA(canonicalNow)}
	return out, nil
}

func TestStrictLogsSignalHistoryOracle(t *testing.T) {
	now := time.Date(2026, 9, 12, 1, 2, 3, 0, time.UTC)
	path := "/fixture/bin/forge-worker"
	hash := strings.Repeat("a", 64)
	args := []string{path, "-config", "/fixture/runtime/worker.json", "-id", "original"}
	zero := 0
	launch := slProcessIdentity{PID: 123, ExePath: path, SHA: hash, Ticks: "456", ObservedAt: now, Args: args}
	signal := slSignalRecord{slProcessIdentity: launch, SentAt: now.Add(3 * time.Second), DBBefore: now.Add(2 * time.Second), DBAfter: now.Add(3500 * time.Millisecond), ExitedAt: now.Add(4 * time.Second), ExitCode: &zero}
	signal.ObservedAt = now.Add(time.Second)
	if _, e := slAuditSignal(slJSON(launch), slJSON(signal), path, hash, args); e != nil {
		t.Fatal(e)
	}
	changes := map[string]func(map[string]any){"missing ticks": func(m map[string]any) { delete(m, "proc_start_ticks") }, "nonnumeric ticks": func(m map[string]any) { m["proc_start_ticks"] = "not-a-number" }, "wrong ticks": func(m map[string]any) { m["proc_start_ticks"] = "789" }, "numeric type ticks": func(m map[string]any) { m["proc_start_ticks"] = 456 }, "replacement PID": func(m map[string]any) { m["pid"] = 999 }, "fractional PID": func(m map[string]any) { m["pid"] = 123.5 }, "argv change": func(m map[string]any) { m["args"] = []string{path, "-config", "other"} }, "missing exit code": func(m map[string]any) { delete(m, "exit_code") }}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			var bad map[string]any
			if e := json.Unmarshal(slJSON(signal), &bad); e != nil {
				t.Fatal(e)
			}
			change(bad)
			if _, e := slAuditSignal(slJSON(launch), slJSON(bad), path, hash, args); e == nil {
				t.Fatal("unbound signal accepted")
			}
		})
	}
	summary := sigtermBoundary{SentAt: signal.SentAt, DBBefore: signal.DBBefore, DBAfter: signal.DBAfter, ExitedAt: signal.ExitedAt}
	if e := slAuditSignalSummary(signal, slJSON(summary)); e != nil {
		t.Fatal(e)
	}
	summary.SentAt = summary.SentAt.Add(-time.Second)
	if slAuditSignalSummary(signal, slJSON(summary)) == nil {
		t.Fatal("signal summary substitution accepted")
	}
	docker := []any{map[string]any{"Id": "cid", "Name": "/job", "State": map[string]bool{"Running": true}}}
	clock := map[string]any{"observed_at": now.Add(5 * time.Second)}
	if e := slAuditAfterSignal(signal, slJSON(clock), slJSON(docker), "cid", "job"); e != nil {
		t.Fatal(e)
	}
	clock["observed_at"] = now
	if slAuditAfterSignal(signal, slJSON(clock), slJSON(docker), "cid", "job") == nil {
		t.Fatal("observation before signal exit accepted")
	}
}
func TestStrictLogsClaimHistoryOracle(t *testing.T) {
	now := time.Date(2026, 9, 12, 1, 2, 3, 0, time.UTC)
	until := now.Add(30 * time.Second)
	at := until.Add(time.Second)
	op := runner.OperationRequest{WorkspaceRequest: runner.WorkspaceRequest{TenantID: "tenant", RunID: "run", WorkspaceID: "run", Epoch: 2}, OperationID: "op"}
	prior := flow.State{TenantID: "tenant", RunID: "run", Version: 5, Status: domain.StatusRunning, Lease: domain.Lease{Owner: "lifecycle-original-0", Epoch: 2, Until: until}}
	stored := prior
	stored.Lease.Until = now.Add(20 * time.Second) // Heartbeat updates SQL header independently.
	old := slExitedRun{TenantID: "tenant", ID: "run", State: "running", Version: 5, Owner: prior.Lease.Owner, Epoch: 2, Until: until, Snapshot: stored}
	next := prior
	next.Version = 6
	next.Lease = domain.Lease{Owner: "lifecycle-successor-0", Epoch: 3, Until: at.Add(30 * time.Second)}
	snap := slClaimSnapshot{TenantID: "tenant", RunID: "run", Version: 6, Body: next, InputState: prior, InputEvent: flow.Event{Kind: flow.EventClaimed, ExpectedVersion: 5, Owner: next.Lease.Owner, Epoch: 3, At: at, Lease: &next.Lease}}
	before := slJSON(map[string]any{"runs": []slExitedRun{old}})
	adoption := slJSON(map[string]any{"run_snapshots": []slClaimSnapshot{snap}})
	live := []json.RawMessage{slJSON(snap)}
	if _, e := slAuditClaim(before, adoption, live, op, until, at); e != nil {
		t.Fatal(e)
	}
	for _, name := range []string{"summary lease changed", "summary claim changed", "both summaries moved", "live claim changed", "historical claim changed", "prior epoch changed"} {
		t.Run(name, func(t *testing.T) {
			u, a := until, at
			h := bytes.Clone(adoption)
			l := append([]json.RawMessage{}, live...)
			switch name {
			case "summary lease changed":
				u = u.Add(-time.Second)
			case "summary claim changed":
				a = a.Add(time.Second)
			case "both summaries moved":
				u = now.Add(-2 * time.Hour)
				a = now.Add(-time.Hour)
			case "live claim changed":
				bad := snap
				bad.InputEvent.At = at.Add(time.Second)
				l = []json.RawMessage{slJSON(bad)}
			case "historical claim changed":
				bad := snap
				bad.InputEvent.At = at.Add(time.Second)
				h = slJSON(map[string]any{"run_snapshots": []slClaimSnapshot{bad}})
			case "prior epoch changed":
				bad := snap
				bad.InputState.Lease.Epoch = 1
				h = slJSON(map[string]any{"run_snapshots": []slClaimSnapshot{bad}})
				l = []json.RawMessage{slJSON(bad)}
			}
			if _, e := slAuditClaim(before, h, l, op, u, a); e == nil {
				t.Fatal("summary or durable claim substitution accepted")
			}
		})
	}
}

func slBindProcess(launch, current slProcessIdentity) error {
	if launch.PID <= 0 || current.PID != launch.PID || !slNumericTicks(launch.Ticks) || current.Ticks != launch.Ticks || launch.ExePath == "" || current.ExePath != launch.ExePath || len(launch.SHA) != 64 || current.SHA != launch.SHA || len(launch.Args) == 0 || !reflect.DeepEqual(current.Args, launch.Args) || launch.ObservedAt.IsZero() || current.ObservedAt.Before(launch.ObservedAt) {
		return fmt.Errorf("process identity changed or incomplete")
	}
	return nil
}

func slPauseWindow(dbNow, until, sent, confirmed, continued time.Time, watchdog bool) error {
	if dbNow.IsZero() || until.IsZero() || sent.IsZero() || confirmed.IsZero() || continued.IsZero() || until.Sub(dbNow) < 24*time.Second || sent.Before(dbNow) || confirmed.Before(sent) || confirmed.Sub(sent) > time.Second || continued.Before(confirmed) || !continued.Before(sent.Add(18*time.Second)) || !continued.Before(until.Add(-time.Second)) || watchdog {
		return fmt.Errorf("paused worker exceeded confirmed short DB-lease window")
	}
	return nil
}
func TestStrictLogsPauseControlOracle(t *testing.T) {
	now := time.Date(2026, 9, 12, 1, 2, 3, 0, time.UTC)
	until := now.Add(30 * time.Second)
	sent := now.Add(time.Second)
	confirmed := sent.Add(25 * time.Millisecond)
	continued := sent.Add(10 * time.Second)
	if e := slPauseWindow(now, until, sent, confirmed, continued, false); e != nil {
		t.Fatal(e)
	}
	if slPauseWindow(now, now.Add(23*time.Second), sent, confirmed, continued, false) == nil || slPauseWindow(now, until, sent, confirmed, sent.Add(18*time.Second), false) == nil || slPauseWindow(now, until, sent, confirmed, continued, true) == nil || slPauseWindow(now, until, sent, time.Time{}, continued, false) == nil {
		t.Fatal("unsafe pause or watchdog timeout accepted")
	}
	original := slProcessIdentity{PID: 123, ExePath: "/fixture/worker", SHA: strings.Repeat("a", 64), Ticks: "987", Args: []string{"/fixture/worker"}, ObservedAt: now}
	if e := slBindProcess(original, original); e != nil {
		t.Fatal(e)
	}
	for _, name := range []string{"missing tick", "replacement PID", "new start", "changed executable", "changed arguments"} {
		t.Run(name, func(t *testing.T) {
			bad := original
			switch name {
			case "missing tick":
				bad.Ticks = ""
			case "replacement PID":
				bad.PID++
			case "new start":
				bad.Ticks = "988"
			case "changed executable":
				bad.SHA = strings.Repeat("b", 64)
			case "changed arguments":
				bad.Args = []string{"other"}
			}
			if slBindProcess(original, bad) == nil {
				t.Fatal("watchdog could target a different process")
			}
		})
	}
}

// Caller holds the pause mutex across this decision AND the actual signal. A
// watchdog that has already resumed the child must permanently close admission
// to a delayed STOP, even if the initial goroutine wakes after the deadline.
type slPauseGate struct{ stopped, resumed bool }

func (g *slPauseGate) stop(now, deadline time.Time) error {
	if g.resumed || g.stopped || deadline.IsZero() || !now.Before(deadline) {
		return fmt.Errorf("late or duplicate worker stop rejected")
	}
	g.stopped = true
	return nil
}
func (g *slPauseGate) resume() { g.resumed = true }
func TestStrictLogsPauseSignalOrdering(t *testing.T) {
	now := time.Now()
	deadline := now.Add(18 * time.Second)
	var alreadyContinued slPauseGate
	alreadyContinued.resume()
	if alreadyContinued.stop(now, deadline) == nil || alreadyContinued.stopped {
		t.Fatal("watchdog CONT allowed a later STOP")
	}
	var expired slPauseGate
	if expired.stop(deadline, deadline) == nil || expired.stopped {
		t.Fatal("expired setup sent STOP")
	}
	var normal slPauseGate
	if e := normal.stop(now, deadline); e != nil {
		t.Fatal(e)
	}
	normal.resume()
	if !normal.stopped || !normal.resumed || normal.stop(now, deadline) == nil {
		t.Fatal("stop/resume order was not final")
	}
}
