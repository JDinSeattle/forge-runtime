package sandbox

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
)

type testCapture struct {
	seen   [3]int
	kept   []byte
	limit  int
	finish int
}

func (c *testCapture) Write(stream byte, data []byte) error {
	c.seen[stream] += len(data)
	left := c.limit - len(c.kept)
	if left > 0 {
		c.kept = append(c.kept, data[:min(left, len(data))]...)
	}
	if left < len(data) {
		return ErrLogLimit
	}
	return nil
}
func (c *testCapture) Finish(bool, string, bool, bool) error { c.finish++; return nil }
func (c *testCapture) Snapshot() (LogSummary, []byte)        { return LogSummary{}, c.kept }

func TestStrictLogDockerMultiplexAlwaysDrainsAfterLimit(t *testing.T) {
	var frames bytes.Buffer
	for _, stream := range []byte{1, 2, 1, 2} {
		payload := bytes.Repeat([]byte{stream}, 1<<20)
		var h [8]byte
		h[0] = stream
		binary.BigEndian.PutUint32(h[4:], uint32(len(payload)))
		frames.Write(h[:])
		frames.Write(payload)
	}
	capture := &testCapture{limit: 1024}
	signal := make(chan error, 1)
	if err := drainAttach(&frames, capture, signal); err != nil {
		t.Fatal(err)
	}
	if frames.Len() != 0 || len(capture.kept) != 1024 || capture.seen[1] != 2<<20 || capture.seen[2] != 2<<20 {
		t.Fatalf("not drained: %v %d", capture.seen, frames.Len())
	}
	if !errors.Is(<-signal, ErrLogLimit) || len(signal) != 0 {
		t.Fatal("limit was not signaled once")
	}
}

func TestStrictLogDockerMultiplexRejectsBrokenFrames(t *testing.T) {
	for name, raw := range map[string][]byte{"short_header": {1, 0}, "invalid_stream": {3, 0, 0, 0, 0, 0, 0, 0}, "reserved_bytes": {1, 1, 0, 0, 0, 0, 0, 0}, "truncated_body": {2, 0, 0, 0, 0, 0, 0, 5, 'x'}, "huge_length_bounded_reader": {1, 0, 0, 0, 255, 255, 255, 255}} {
		t.Run(name, func(t *testing.T) {
			if err := drainAttach(bytes.NewReader(raw), &testCapture{limit: 128}, make(chan error, 1)); err == nil || err == io.EOF {
				t.Fatal("broken stream accepted")
			}
		})
	}
}

func TestStrictLogPolicyHardCeilingsAndVerificationGuard(t *testing.T) {
	defaults, err := (LogPolicy{}).Normalize()
	if err != nil || defaults.EntryBytes != 16<<10 || defaults.OperationBytes != 512<<10 || defaults.RunBytes != 16<<20 || defaults.MaxOperations != 32 || defaults.PreviewBytes != 64<<10 {
		t.Fatal(defaults, err)
	}
	for _, p := range []LogPolicy{{EntryBytes: -1}, {EntryBytes: 16<<10 + 1}, {OperationBytes: 512<<10 + 1}, {RunBytes: 16<<20 + 1}, {MaxOperations: 33}, {PreviewBytes: 64<<10 + 1}, {EntryBytes: 1}, {OperationBytes: 32}, {RunBytes: 100}, {PreviewBytes: 128, OperationBytes: 64}} {
		if _, err := p.Normalize(); err == nil {
			t.Fatal("invalid policy accepted", p)
		}
	}
	valid := LogSummary{Complete: true}
	if !VerificationLogValid(Job{Log: &valid}) || !VerificationLogValid(Job{}) {
		t.Fatal("normal or legacy verification refused")
	}
	for _, s := range []LogSummary{{Complete: false}, {Complete: true, Truncated: true}, {Complete: true, Reason: "output_limit"}, {Complete: true, TerminationRequested: true}} {
		if VerificationLogValid(Job{Log: &s}) {
			t.Fatal("incomplete/policy outcome accepted", s)
		}
	}
}

func TestStrictLogAttachUpgradePreservesBufferedFirstFrame(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()
	sent := make(chan error, 1)
	upgraded := make(chan struct{})
	go func() {
		r := bufio.NewReader(server)
		request, err := http.ReadRequest(r)
		if err != nil {
			sent <- err
			return
		}
		if request.URL.Query().Get("stream") != "1" || request.URL.Query().Get("stdin") != "0" {
			sent <- errors.New("wrong attach parameters")
			return
		}
		_, err = server.Write(append([]byte("HTTP/1.1 101 Switching Protocols\r\nConnection: keep-alive, Upgrade\r\nUpgrade: tcp\r\n\r\n"), []byte{1, 0, 0, 0, 0, 0, 0, 3, 'x', 0, 255}...))
		sent <- err
		<-upgraded
		server.Close()
	}()
	conn, reader, err := upgradeAttach(context.Background(), client, "operation")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	close(upgraded)
	capture := &testCapture{limit: 100}
	if err = drainAttach(reader, capture, make(chan error, 1)); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(capture.kept, []byte{'x', 0, 255}) {
		t.Fatal("upgrade discarded already-buffered output")
	}
	if err = <-sent; err != nil {
		t.Fatal(err)
	}
}
func TestStrictLogAttachHandshakeCancellationAndBounds(t *testing.T) {
	for name, response := range map[string]string{"wrong_status": "HTTP/1.1 200 OK\r\n\r\n", "wrong_token": "HTTP/1.1 101 Switching Protocols\r\nConnection: notupgrade\r\nUpgrade: tcp\r\n\r\n", "long_header": "HTTP/1.1 101 Switching Protocols\r\nX: " + strings.Repeat("x", 1<<20) + "\r\n\r\n", "cancel": ""} {
		t.Run(name, func(t *testing.T) {
			client, server := net.Pipe()
			defer server.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			serverDone := make(chan struct{})
			go func() {
				defer close(serverDone)
				_, err := http.ReadRequest(bufio.NewReader(server))
				if err == nil {
					if name == "cancel" {
						cancel()
					} else {
						server.Write([]byte(response))
					}
				}
				server.Close()
			}()
			_, _, err := upgradeAttach(ctx, client, "operation")
			if err == nil {
				t.Fatal("bad/cancelled handshake accepted")
			}
			<-serverDone
		})
	}
	for _, host := range []string{"", "tcp://localhost:2375", "unix:relative", "unix:///socket?x=y", "unix:///socket#fragment", "unix://name/socket"} {
		if _, err := (&Docker{Host: host}).logSocket(); err == nil {
			t.Fatal("invalid strict endpoint", host)
		}
	}
}
