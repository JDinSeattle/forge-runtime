package runner

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/sandbox"
	"golang.org/x/sys/unix"
)

const logHeaderBytes = 32

type logCapture struct {
	mu       sync.Mutex
	root     *os.Root
	file     *os.File
	name     string
	summary  sandbox.LogSummary
	preview  []byte
	failed   error
	finished bool
}

type captureMetadata struct {
	Summary  sandbox.LogSummary `json:"summary"`
	DataHash string             `json:"data_hash"`
	Preview  []byte             `json:"preview"`
}

func logBinding(o Operation) string {
	r := o.Request
	r.Grant = ""
	r.Deadline = r.Deadline.UTC()
	raw, _ := json.Marshal(r)
	return hashBytes(raw)
}

func newLogCapture(dir string, o Operation, p sandbox.LogPolicy) (*logCapture, error) {
	if o.Request.OperationID.Validate() != nil {
		return nil, domain.ErrInvalid
	}
	root, err := openLogRoot(dir, true)
	if err != nil {
		return nil, err
	}
	name := string(o.Request.OperationID) + ".spool"
	f, err := root.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY|unix.O_NOFOLLOW, 0600)
	if err != nil {
		root.Close()
		return nil, err
	}
	if err = syncLogRoot(root); err != nil {
		f.Close()
		root.Close()
		return nil, err
	}
	return &logCapture{root: root, file: f, name: name, summary: sandbox.LogSummary{SchemaVersion: 1, Policy: p, OperationID: o.Request.OperationID, BindingHash: logBinding(o)}}, nil
}

// Walk every absolute path component with O_NOFOLLOW. /proc/self/fd then opens
// an already pinned directory into os.Root; renames cannot redirect it.
func openLogRoot(dir string, create bool) (*os.Root, error) {
	if !filepath.IsAbs(dir) || filepath.Clean(dir) != dir {
		return nil, domain.ErrInvalid
	}
	fd, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	defer func() { unix.Close(fd) }()
	parts := strings.Split(strings.TrimPrefix(dir, "/"), "/")
	for _, part := range parts {
		if part == "" {
			return nil, domain.ErrInvalid
		}
		next, e := unix.Openat(fd, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		if errors.Is(e, unix.ENOENT) && create {
			if e = unix.Mkdirat(fd, part, 0700); e != nil && !errors.Is(e, unix.EEXIST) {
				return nil, e
			}
			if e = unix.Fsync(fd); e != nil {
				return nil, e
			}
			next, e = unix.Openat(fd, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		}
		if e != nil {
			return nil, e
		}
		unix.Close(fd)
		fd = next
	}
	var st unix.Stat_t
	if err = unix.Fstat(fd, &st); err != nil {
		return nil, err
	}
	if st.Mode&0077 != 0 {
		return nil, domain.ErrForbidden
	}
	return os.OpenRoot("/proc/self/fd/" + strconv.Itoa(fd))
}
func syncLogRoot(root *os.Root) error {
	f, err := root.Open(".")
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func saturatingAdd(value *uint64, n int) {
	if uint64(n) > math.MaxUint64-*value {
		*value = math.MaxUint64
	} else {
		*value += uint64(n)
	}
}

func (c *logCapture) Write(stream byte, data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.finished {
		return io.ErrClosedPipe
	}
	if stream == 1 {
		saturatingAdd(&c.summary.StdoutSeen, len(data))
	} else if stream == 2 {
		saturatingAdd(&c.summary.StderrSeen, len(data))
	} else {
		return domain.ErrInvalid
	}
	if c.failed != nil {
		return c.failed
	}
	for len(data) > 0 {
		available := c.summary.Policy.OperationBytes - int(c.summary.RetainedBytes) - logHeaderBytes
		if available <= 0 {
			c.failed = sandbox.ErrLogLimit
			c.summary.Truncated = true
			c.summary.Reason = "output_limit"
			return c.failed
		}
		n := min(len(data), c.summary.Policy.EntryBytes-logHeaderBytes, available)
		header := make([]byte, logHeaderBytes)
		copy(header, "FLG1")
		header[4] = stream
		binary.BigEndian.PutUint64(header[8:16], c.summary.Records)
		binary.BigEndian.PutUint32(header[16:20], uint32(n))
		binary.BigEndian.PutUint32(header[20:24], crc32.ChecksumIEEE(data[:n]))
		frame := append(header, data[:n]...)
		written, err := c.file.Write(frame)
		if err != nil || written != len(frame) {
			if err == nil {
				err = io.ErrShortWrite
			}
			c.failed = err
			c.summary.Truncated = true
			c.summary.Reason = "spool_io_error"
			return err
		}
		c.summary.RetainedBytes += int64(len(frame))
		c.summary.RetainedPayload += int64(n)
		c.summary.Records++
		left := c.summary.Policy.PreviewBytes - len(c.preview)
		if left > 0 {
			c.preview = append(c.preview, data[:min(n, left)]...)
		}
		data = data[n:]
	}
	return nil
}

func (c *logCapture) Snapshot() (sandbox.LogSummary, []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.summary, append([]byte(nil), c.preview...)
}

func (c *logCapture) Finish(complete bool, reason string, requested, observed bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.finished {
		return c.failed
	}
	c.finished = true
	defer c.root.Close()
	if err := c.file.Sync(); err != nil {
		c.failed = err
		complete = false
		reason = "spool_sync_error"
	}
	if err := c.file.Close(); err != nil {
		c.failed = err
		complete = false
		reason = "spool_close_error"
	}
	c.summary.Complete = complete
	c.summary.DroppedKnown = complete
	if complete {
		seen := saturatedTotal(c.summary.StdoutSeen, c.summary.StderrSeen)
		if seen >= uint64(c.summary.RetainedPayload) {
			c.summary.DroppedBytes = seen - uint64(c.summary.RetainedPayload)
		}
	}
	c.summary.TerminationRequested = requested
	c.summary.TerminationObserved = observed
	if c.summary.Reason == "" {
		c.summary.Reason = reason
	}
	if !complete {
		c.summary.Truncated = true
		if c.summary.Reason == "" {
			c.summary.Reason = "capture_gap"
		}
	}
	data, err := readLogBounded(c.root, c.name, c.summary.Policy.OperationBytes)
	if err != nil {
		return err
	}
	metadata, _ := json.Marshal(captureMetadata{Summary: c.summary, DataHash: hashBytes(data), Preview: c.preview})
	tmp := c.name + ".meta.tmp"
	f, err := c.root.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY|unix.O_NOFOLLOW, 0600)
	if err != nil {
		return err
	}
	_, err = f.Write(metadata)
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = c.root.Rename(tmp, c.name+".meta"); err != nil {
		return err
	}
	d, err := c.root.Open(".")
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func readLogBounded(root *os.Root, name string, max int) ([]byte, error) {
	f, err := root.OpenFile(name, os.O_RDONLY|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > int64(max) {
		return nil, domain.ErrInvalid
	}
	data, err := io.ReadAll(io.LimitReader(f, int64(max)+1))
	if len(data) > max {
		return nil, domain.ErrInvalid
	}
	return data, err
}

// readLogCapture never resumes append after process death. A complete metadata
// record must authenticate the exact bounded spool. Otherwise return only a
// checked frame prefix and explicitly incomplete capture facts.
func readLogCapture(dir string, o Operation, p sandbox.LogPolicy) (sandbox.LogSummary, []byte, []byte, error) {
	summary := sandbox.LogSummary{SchemaVersion: 1, Policy: p, OperationID: o.Request.OperationID, BindingHash: logBinding(o), Reason: "capture_gap", Truncated: true}
	root, err := openLogRoot(dir, false)
	if errors.Is(err, os.ErrNotExist) {
		return summary, nil, nil, nil
	}
	if err != nil {
		return summary, nil, nil, err
	}
	defer root.Close()
	name := string(o.Request.OperationID) + ".spool"
	data, err := readLogBounded(root, name, p.OperationBytes)
	if errors.Is(err, os.ErrNotExist) {
		return summary, nil, nil, nil
	}
	if err != nil {
		return summary, nil, nil, err
	}
	prefix, preview := []byte{}, []byte{}
	for offset := 0; offset < len(data); {
		if len(data)-offset < logHeaderBytes {
			break
		}
		h := data[offset : offset+logHeaderBytes]
		n := int(binary.BigEndian.Uint32(h[16:20]))
		if string(h[:4]) != "FLG1" || h[4] < 1 || h[4] > 2 || !bytes.Equal(h[5:8], make([]byte, 3)) || !bytes.Equal(h[24:32], make([]byte, 8)) || binary.BigEndian.Uint64(h[8:16]) != summary.Records || n < 1 || n > p.EntryBytes-logHeaderBytes || n > len(data)-offset-logHeaderBytes {
			break
		}
		payload := data[offset+logHeaderBytes : offset+logHeaderBytes+n]
		if crc32.ChecksumIEEE(payload) != binary.BigEndian.Uint32(h[20:24]) {
			break
		}
		if h[4] == 1 {
			saturatingAdd(&summary.StdoutSeen, n)
		} else {
			saturatingAdd(&summary.StderrSeen, n)
		}
		summary.Records++
		summary.RetainedPayload += int64(n)
		summary.RetainedBytes += int64(logHeaderBytes + n)
		if left := p.PreviewBytes - len(preview); left > 0 {
			preview = append(preview, payload[:min(left, n)]...)
		}
		offset += logHeaderBytes + n
		prefix = data[:offset]
	}
	raw, metaErr := readLogBounded(root, name+".meta", 128<<10)
	if metaErr == nil {
		var meta captureMetadata
		if json.Unmarshal(raw, &meta) != nil || meta.Summary.OperationID != o.Request.OperationID || meta.Summary.BindingHash != logBinding(o) || meta.Summary.Policy != p || meta.Summary.SchemaVersion != 1 || meta.DataHash != hashBytes(data) || len(prefix) != len(data) || !bytes.Equal(meta.Preview, preview) || meta.Summary.Records != summary.Records || meta.Summary.RetainedBytes != summary.RetainedBytes || meta.Summary.RetainedPayload != summary.RetainedPayload || !validLogMetadata(meta.Summary, summary) {
			return summary, nil, nil, domain.ErrReconciliation
		}
		summary = meta.Summary
	} else if !errors.Is(metaErr, os.ErrNotExist) {
		return summary, nil, nil, metaErr
	}
	return summary, prefix, preview, nil
}

func saturatedTotal(a, b uint64) uint64 {
	if b > math.MaxUint64-a {
		return math.MaxUint64
	}
	return a + b
}
func validLogMetadata(m, prefix sandbox.LogSummary) bool {
	if m.StdoutSeen < prefix.StdoutSeen || m.StderrSeen < prefix.StderrSeen || m.Artifact.ObjectKey != "" {
		return false
	}
	if m.TerminationObserved && !m.TerminationRequested {
		return false
	}
	switch m.Reason {
	case "", "output_limit", "spool_io_error", "spool_sync_error", "spool_close_error", "capture_gap", "runner_shutdown", "capture_drain_timeout":
	default:
		return false
	}
	if m.Complete != m.DroppedKnown {
		return false
	}
	if !m.Complete {
		return m.Truncated && m.Reason != "" && m.DroppedBytes == 0
	}
	dropped := saturatedTotal(m.StdoutSeen, m.StderrSeen) - uint64(m.RetainedPayload)
	if dropped != m.DroppedBytes {
		return false
	}
	if m.Reason == "" {
		return dropped == 0 && !m.Truncated && !m.TerminationRequested
	}
	return m.Truncated
}

func (e *Engine) logDir(id domain.ID) string {
	if base := e.storageBase(id); base != "" {
		return filepath.Join(base, "logs")
	}
	return filepath.Join(e.config.RootDir, "logs", string(id))
}
