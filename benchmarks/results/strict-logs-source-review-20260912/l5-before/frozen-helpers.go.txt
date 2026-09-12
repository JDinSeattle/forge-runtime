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
	"strings"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/artifact"
	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/runner"
	"github.com/JDinSeattle/forge-runtime/internal/runnerclient"
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
	if a.PoolRoot != filepath.Join(a.ScopeRoot, "pool-root") || a.RunnerConfig != filepath.Join(rt, "runner.json") || a.EvidenceDir != filepath.Join(a.ScopeRoot, "evidence", "sigterm-01") {
		return fmt.Errorf("acceptance paths mismatch")
	}
	for _, x := range []struct{ path, name, hash string }{{a.RunnerBinary, "forge-runner", a.RunnerSHA}, {a.WorkerBinary, "forge-worker", a.WorkerSHA}, {a.TestBinary, "application-faults.test", a.TestSHA}} {
		if x.path != filepath.Join(a.ScopeRoot, "bin", x.name) || len(x.hash) != 64 {
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
