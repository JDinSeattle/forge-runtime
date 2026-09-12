package runner

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/sandbox"
)

func captureFixture(t *testing.T, p sandbox.LogPolicy) (string, Operation, *logCapture) {
	t.Helper()
	p, err := p.Normalize()
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(t.TempDir(), "logs")
	o := Operation{Request: request(WorkspaceRequest{TenantID: "tenant", RunID: "run", WorkspaceID: "workspace", Epoch: 1}, "capture", "run_command", CommandArgs{Command: []string{"fixture"}}, 1)}
	c, err := newLogCapture(dir, o, p)
	if err != nil {
		t.Fatal(err)
	}
	return dir, o, c
}

func TestStrictLogShutdownPreservesJobAndGapAcrossReopen(t *testing.T) {
	started := make(chan struct{})
	var launches, cancels atomic.Int64
	backend := &sandbox.TestBackend{
		StartFunc: func(ctx context.Context, s sandbox.JobSpec) (sandbox.Job, error) {
			launches.Add(1)
			s.Capture.Write(1, []byte("before shutdown"))
			close(started)
			<-ctx.Done()
			if s.DetachOnCancel == nil || !s.DetachOnCancel() {
				t.Error("shutdown distinction lost")
			}
			s.Capture.Finish(false, "runner_shutdown", false, false)
			return sandbox.Job{}, sandbox.ErrCaptureGap
		},
		InspectFunc: func(_ context.Context, id string) (sandbox.Job, error) {
			return sandbox.Job{ID: id, Started: true, Running: true, StrictLogs: true}, nil
		},
		CancelFunc: func(_ context.Context, id string) (sandbox.Job, error) {
			cancels.Add(1)
			return sandbox.Job{ID: id, Started: true, Interrupted: true, StrictLogs: true, ExitCode: 137}, nil
		},
	}
	e, cfg, r := fixture(t, nil, backend)
	p, _ := (sandbox.LogPolicy{}).Normalize()
	e.config.Logs = &p
	cfg.Logs = &p
	req := request(r, "shutdown-log", "run_command", CommandArgs{Command: []string{"fixture"}}, 1)
	if _, err := e.StartOperation(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if cancels.Load() != 0 {
		t.Fatal("runner exit killed job")
	}
	e, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	inspected, err := e.InspectOperation(context.Background(), InspectRequest{WorkspaceRequest: r, OperationID: req.OperationID})
	if err != nil || inspected.Status != Unknown {
		t.Fatal("lost capture did not remain unknown", inspected.Status, err)
	}
	if _, err = e.StartOperation(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if launches.Load() != 1 || cancels.Load() != 0 {
		t.Fatal("reopen/retry restarted or killed original")
	}
	cancelled, err := e.CancelOperation(context.Background(), InspectRequest{WorkspaceRequest: r, OperationID: req.OperationID})
	if err != nil {
		t.Fatal(err)
	}
	var job sandbox.Job
	json.Unmarshal(cancelled.Result, &job)
	if cancelled.Status != Cancelled || job.Log == nil || job.Log.Complete || job.Log.Reason != "runner_shutdown" || cancels.Load() != 1 {
		t.Fatalf("business cancel did not preserve gap: %+v %s", job, cancelled.Status)
	}
}

func TestStrictLogReceiptCrashReusesCapturedBytesAndReservation(t *testing.T) {
	var launches atomic.Int64
	job := sandbox.Job{ID: "", Started: true}
	backend := &sandbox.TestBackend{StartFunc: func(_ context.Context, s sandbox.JobSpec) (sandbox.Job, error) {
		launches.Add(1)
		if err := s.Capture.Write(2, []byte("complete stderr")); err != nil {
			return sandbox.Job{}, err
		}
		if err := s.Capture.Finish(true, "", false, false); err != nil {
			return sandbox.Job{}, err
		}
		j := job
		j.ID = s.ID
		return j, nil
	}, InspectFunc: func(_ context.Context, id string) (sandbox.Job, error) { j := job; j.ID = id; return j, nil }}
	fail := true
	e, cfg, r := fixture(t, func(point string) error {
		if point == "after_receipt_before_commit" && fail {
			return errors.New("receipt commit crash fixture")
		}
		return nil
	}, backend)
	p, _ := (sandbox.LogPolicy{}).Normalize()
	e.config.Logs = &p
	cfg.Logs = &p
	req := request(r, "receipt-log", "run_command", CommandArgs{Command: []string{"fixture"}}, 1)
	if _, err := e.StartOperation(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	waitOperation(t, e, r, req.OperationID, false)
	e.Close()
	fail = false
	cfg.Fault = nil
	e, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	recovered, err := e.InspectOperation(context.Background(), InspectRequest{WorkspaceRequest: r, OperationID: req.OperationID})
	if err != nil || recovered.Status != Succeeded {
		t.Fatal(recovered.Status, err)
	}
	var result sandbox.Job
	json.Unmarshal(recovered.Result, &result)
	var count int
	e.journal.db.QueryRow(`SELECT operations FROM log_runs`).Scan(&count)
	if launches.Load() != 1 || count != 1 || !result.Log.Complete || string(result.Output) != "complete stderr" {
		t.Fatal("crash replay duplicated sampling or lost completed capture")
	}
}

func TestStrictLogFramesBoundedAndBinaryStreamsPreserved(t *testing.T) {
	dir, o, c := captureFixture(t, sandbox.LogPolicy{EntryBytes: 128, OperationBytes: 1024, RunBytes: 2048, PreviewBytes: 128})
	out := bytes.Repeat([]byte{'a', 0, 0xff}, 100)
	stderr := []byte("中文😀\n")
	if err := c.Write(1, out); err != nil {
		t.Fatal(err)
	}
	if err := c.Write(2, stderr); err != nil {
		t.Fatal(err)
	}
	if err := c.Finish(true, "", false, false); err != nil {
		t.Fatal(err)
	}
	if err := c.Finish(false, "later error", false, false); err != nil {
		t.Fatal(err)
	}
	summary, data, preview, err := readLogCapture(dir, o, c.summary.Policy)
	if err != nil {
		t.Fatal(err)
	}
	if !summary.Complete || summary.Truncated || len(preview) != 128 || summary.RetainedPayload != int64(len(out)+len(stderr)) {
		t.Fatalf("capture facts: %+v", summary)
	}
	streams := map[byte][]byte{}
	for at := 0; at < len(data); {
		n := int(binary.BigEndian.Uint32(data[at+16 : at+20]))
		if n+logHeaderBytes > 128 {
			t.Fatal("oversized entry")
		}
		streams[data[at+4]] = append(streams[data[at+4]], data[at+32:at+32+n]...)
		at += 32 + n
	}
	if !bytes.Equal(streams[1], out) || !bytes.Equal(streams[2], stderr) {
		t.Fatal("binary/stream identity changed")
	}
	if err := c.Write(1, []byte("late")); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatal("write after seal", err)
	}
	wrong := o
	wrong.Request.RunID = "other"
	if _, _, _, err = readLogCapture(dir, wrong, c.summary.Policy); !errors.Is(err, domain.ErrReconciliation) {
		t.Fatal("cross-run capture accepted", err)
	}
}

func TestStrictLogFloodStopsStorageButContinuesCountingBothStreams(t *testing.T) {
	dir, o, c := captureFixture(t, sandbox.LogPolicy{EntryBytes: 128, OperationBytes: 512, RunBytes: 1024, PreviewBytes: 64})
	input := bytes.Repeat([]byte{0xff}, 1<<20)
	var wg sync.WaitGroup
	for _, stream := range []byte{1, 2} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 3 {
				if !errors.Is(c.Write(stream, input), sandbox.ErrLogLimit) {
					t.Error("flood did not hit limit")
				}
			}
		}()
	}
	wg.Wait()
	if err := c.Finish(true, "output_limit", true, true); err != nil {
		t.Fatal(err)
	}
	s, data, preview, err := readLogCapture(dir, o, c.summary.Policy)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) > 512 || len(preview) > 64 || s.StdoutSeen != 3<<20 || s.StderrSeen != 3<<20 || !s.Truncated || !s.Complete || !s.DroppedKnown || s.DroppedBytes != 6<<20-uint64(s.RetainedPayload) || s.Reason != "output_limit" {
		t.Fatalf("flood accounting: %+v", s)
	}
	if sandbox.VerificationLogValid(sandbox.Job{Log: &s}) {
		t.Fatal("policy kill accepted as verification")
	}
}

func TestStrictLogCrashPrefixNeverInventsCompleteCapture(t *testing.T) {
	dir, o, c := captureFixture(t, sandbox.LogPolicy{})
	if err := c.Write(1, []byte("durable-prefix")); err != nil {
		t.Fatal(err)
	}
	if err := c.file.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.file.Write([]byte("FLG1 partial")); err != nil {
		t.Fatal(err)
	}
	c.file.Close()
	c.root.Close() // equivalent persisted bytes; no Finish metadata
	s, data, _, err := readLogCapture(dir, o, c.summary.Policy)
	if err != nil {
		t.Fatal(err)
	}
	if s.Complete || s.DroppedKnown || !s.Truncated || s.Reason != "capture_gap" || s.Records != 1 || len(data) != 32+len("durable-prefix") {
		t.Fatalf("invented completion: %+v", s)
	}
	if _, err := newLogCapture(dir, o, c.summary.Policy); !errors.Is(err, os.ErrExist) {
		t.Fatal("crashed operation spool reopened for append", err)
	}
}

func TestStrictLogCorruptionAndSymlinkFailClosed(t *testing.T) {
	dir, o, c := captureFixture(t, sandbox.LogPolicy{})
	c.Write(2, []byte("stderr"))
	if err := c.Finish(true, "", false, false); err != nil {
		t.Fatal(err)
	}
	filename := filepath.Join(dir, string(o.Request.OperationID)+".spool")
	f, err := os.OpenFile(filename, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteAt([]byte{0}, 32)
	f.Close()
	if _, _, _, err = readLogCapture(dir, o, c.summary.Policy); !errors.Is(err, domain.ErrReconciliation) {
		t.Fatal("corrupted sealed bytes accepted", err)
	}
	if err = os.Remove(filename); err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink("/etc/passwd", filename); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err = readLogCapture(dir, o, c.summary.Policy); err == nil {
		t.Fatal("symlink spool accepted")
	}
}

func TestStrictLogRunReservationSurvivesReopenAndDuplicate(t *testing.T) {
	backend := &sandbox.TestBackend{StartFunc: func(ctx context.Context, s sandbox.JobSpec) (sandbox.Job, error) {
		if err := s.Capture.Write(1, []byte("okay")); err != nil {
			return sandbox.Job{}, err
		}
		if err := s.Capture.Finish(true, "", false, false); err != nil {
			return sandbox.Job{}, err
		}
		summary, preview := s.Capture.Snapshot()
		return sandbox.Job{ID: s.ID, Started: true, Output: preview, Log: &summary}, nil
	}}
	e, cfg, r := fixture(t, nil, backend)
	p, err := (sandbox.LogPolicy{EntryBytes: 128, OperationBytes: 512, RunBytes: 1024, MaxOperations: 2, PreviewBytes: 64}).Normalize()
	if err != nil {
		t.Fatal(err)
	}
	e.config.Logs = &p
	cfg.Logs = &p
	original := request(r, "log-one", "run_command", CommandArgs{Command: []string{"fixture"}}, 1)
	first, err := e.StartOperation(context.Background(), original)
	if err != nil {
		t.Fatal(err)
	}
	_ = first
	first = waitOperation(t, e, r, original.OperationID, true)
	if first.Status != Succeeded {
		t.Fatalf("strict fixture: %+v", first)
	}
	var job sandbox.Job
	if json.Unmarshal(first.Result, &job) != nil || !job.Log.Complete || job.Log.Artifact.Kind != "operation_log" {
		t.Fatal("missing log artifact")
	}
	for range 8 {
		repeated, err := e.StartOperation(context.Background(), original)
		if err != nil || repeated.Receipt != first.Receipt {
			t.Fatal("duplicate reservation/receipt changed", err)
		}
	}
	e.Close()
	e, err = Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	second := request(r, "log-two", "run_command", CommandArgs{Command: []string{"fixture"}}, 1)
	if _, err = e.StartOperation(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	waitOperation(t, e, r, second.OperationID, true)
	third := request(r, "log-three", "run_command", CommandArgs{Command: []string{"fixture"}}, 1)
	if _, err = e.StartOperation(context.Background(), third); !errors.Is(err, domain.ErrCapacity) {
		t.Fatal("run budget bypassed", err)
	}
	var used, count, ops int
	e.journal.db.QueryRow(`SELECT reserved_bytes,operations FROM log_runs WHERE tenant_id='tenant' AND run_id='run'`).Scan(&used, &count)
	e.journal.db.QueryRow(`SELECT count(*) FROM operations WHERE id='log-three'`).Scan(&ops)
	if used != 1024 || count != 2 || ops != 0 {
		t.Fatalf("partial/duplicate admission: bytes=%d count=%d third=%d", used, count, ops)
	}
}

func TestStrictLogV4MigrationPreservesIdentityAndLegacyWithoutInventingBudget(t *testing.T) {
	file := filepath.Join(t.TempDir(), "journal.sqlite")
	j, err := openJournal(file)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := j.identity(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = j.db.Exec(`DROP TABLE operation_logs;DROP TABLE log_runs;PRAGMA user_version=4`); err != nil {
		t.Fatal(err)
	}
	j.db.Close()
	j, err = openJournal(file)
	if err != nil {
		t.Fatal(err)
	}
	defer j.db.Close()
	again, err := j.identity(context.Background())
	if err != nil || again != identity {
		t.Fatal("identity changed", err)
	}
	var version, counts int
	j.db.QueryRow(`PRAGMA user_version`).Scan(&version)
	j.db.QueryRow(`SELECT count(*) FROM log_runs`).Scan(&counts)
	if version != 5 || counts != 0 {
		t.Fatal("migration invented log budgets")
	}
}

func TestStrictLogReadRejectsSymlinkDirectoryAndInvalidMetadata(t *testing.T) {
	dir, o, c := captureFixture(t, sandbox.LogPolicy{})
	if err := c.Write(1, []byte("bound output")); err != nil {
		t.Fatal(err)
	}
	if err := c.Finish(true, "", false, false); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(filepath.Dir(dir), alias); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := readLogCapture(filepath.Join(alias, filepath.Base(dir)), o, c.summary.Policy); err == nil {
		t.Fatal("ancestor symlink followed")
	}
	path := filepath.Join(dir, string(o.Request.OperationID)+".spool.meta")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*captureMetadata){"seen_underflow": func(m *captureMetadata) { m.Summary.StdoutSeen = 0 }, "invented_drop": func(m *captureMetadata) { m.Summary.DroppedBytes = 1 }, "unknown_complete": func(m *captureMetadata) { m.Summary.DroppedKnown = false }, "observed_without_request": func(m *captureMetadata) { m.Summary.TerminationObserved = true }, "forged_success": func(m *captureMetadata) { m.Summary.StdoutSeen++; m.Summary.DroppedBytes = 1 }, "unknown_reason": func(m *captureMetadata) { m.Summary.Reason = "unknown-policy" }} {
		t.Run(name, func(t *testing.T) {
			var meta captureMetadata
			if err := json.Unmarshal(raw, &meta); err != nil {
				t.Fatal(err)
			}
			mutate(&meta)
			changed, _ := json.Marshal(meta)
			if err := os.WriteFile(path, changed, 0600); err != nil {
				t.Fatal(err)
			}
			if _, _, _, err := readLogCapture(dir, o, c.summary.Policy); !errors.Is(err, domain.ErrReconciliation) {
				t.Fatal("malformed metadata accepted", err)
			}
		})
	}
}
