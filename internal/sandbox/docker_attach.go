package sandbox

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
)

// attach uses the official non-TTY Engine API v1.51 multiplex protocol. An
// acknowledged upgrade is obtained before start; no daemon log replay exists
// with log-driver=none. Only operator configured Unix endpoints are supported.
func (d *Docker) attach(ctx context.Context, id string) (net.Conn, *bufio.Reader, error) {
	if domain.ID(id).Validate() != nil {
		return nil, nil, domain.ErrInvalid
	}
	socket, err := d.logSocket()
	if err != nil {
		return nil, nil, err
	}
	dialer := net.Dialer{Timeout: 10 * time.Second}
	conn, err := dialer.DialContext(ctx, "unix", socket)
	if err != nil {
		return nil, nil, err
	}
	return upgradeAttach(ctx, conn, id)
}

func (d *Docker) logSocket() (string, error) {
	host, err := url.Parse(d.Host)
	if err != nil || host.Scheme != "unix" || host.Host != "" || !filepath.IsAbs(host.Path) || host.RawQuery != "" || host.Fragment != "" || host.User != nil {
		return "", fmt.Errorf("%w: strict logs require an explicit Unix Docker endpoint", ErrUnavailable)
	}
	return host.Path, nil
}

func upgradeAttach(ctx context.Context, conn net.Conn, id string) (net.Conn, *bufio.Reader, error) {
	stopCancellation := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stopCancellation()
	fail := func(err error) (net.Conn, *bufio.Reader, error) { conn.Close(); return nil, nil, err }
	deadline := time.Now().Add(10 * time.Second)
	if v, ok := ctx.Deadline(); ok && v.Before(deadline) {
		deadline = v
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return fail(err)
	}
	request := "POST /v1.51/containers/" + url.PathEscape(id) + "/attach?stream=1&stdout=1&stderr=1&stdin=0 HTTP/1.1\r\nHost: localhost\r\nConnection: Upgrade\r\nUpgrade: tcp\r\nContent-Length: 0\r\n\r\n"
	if _, err := io.WriteString(conn, request); err != nil {
		return fail(err)
	}
	reader := bufio.NewReaderSize(conn, 4096)
	statusBytes, err := reader.ReadSlice('\n')
	status := string(statusBytes)
	if err != nil {
		return fail(err)
	}
	if !strings.HasPrefix(status, "HTTP/1.1 101 ") {
		return fail(fmt.Errorf("%w: Docker attach did not upgrade", ErrCaptureGap))
	}
	total := len(status)
	upgrade, connection := "", ""
	for {
		lineBytes, err := reader.ReadSlice('\n')
		line := string(lineBytes)
		total += len(line)
		if err != nil {
			return fail(err)
		}
		if total > 16<<10 {
			return fail(domain.ErrInvalid)
		}
		if line == "\r\n" {
			break
		}
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			return fail(domain.ErrInvalid)
		}
		switch strings.ToLower(key) {
		case "upgrade":
			upgrade = strings.TrimSpace(value)
		case "connection":
			connection = strings.ToLower(strings.TrimSpace(value))
		}
	}
	hasUpgrade := false
	for _, token := range strings.Split(connection, ",") {
		if strings.TrimSpace(token) == "upgrade" {
			hasUpgrade = true
		}
	}
	if strings.ToLower(upgrade) != "tcp" || !hasUpgrade {
		return fail(domain.ErrInvalid)
	}
	if err = conn.SetDeadline(time.Time{}); err != nil {
		return fail(err)
	}
	return conn, reader, nil
}

func drainAttach(reader io.Reader, capture LogCapture, failed chan<- error) error {
	var header [8]byte
	var buffer [16 << 10]byte
	notified := false
	for {
		_, err := io.ReadFull(reader, header[:])
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if (header[0] != 1 && header[0] != 2) || header[1] != 0 || header[2] != 0 || header[3] != 0 {
			return ErrCaptureGap
		}
		remaining := uint64(binary.BigEndian.Uint32(header[4:]))
		for remaining > 0 {
			n := min(remaining, uint64(len(buffer)))
			if _, err = io.ReadFull(reader, buffer[:int(n)]); err != nil {
				if err == io.EOF {
					return io.ErrUnexpectedEOF
				}
				return err
			}
			if err = capture.Write(header[0], buffer[:int(n)]); err != nil && !notified {
				failed <- err
				notified = true
			}
			remaining -= n
		}
	}
}

// Strict Start waits for capture completion inside the runner's already
// asynchronous executor. StartOperation's durable acknowledgement is unchanged.
func (d *Docker) startCaptured(ctx context.Context, s JobSpec) (job Job, returned error) {
	conn, reader, err := d.attach(ctx, s.ID)
	if err != nil {
		return Job{}, err
	}
	done := make(chan error, 1)
	failed := make(chan error, 1)
	go func() { done <- drainAttach(reader, s.Capture, failed) }()
	complete, requested, observed := false, false, false
	reason := "capture_gap"
	drained := false
	defer func() {
		conn.Close()
		if !drained {
			<-done
		}
		if err := s.Capture.Finish(complete, reason, requested, observed && requested); err != nil && returned == nil {
			returned = err
		}
		summary, preview := s.Capture.Snapshot()
		job.Log = &summary
		job.Output = preview
		job.Truncated = summary.Truncated
		job.StrictLogs = true
	}()
	if s.BeforeStart != nil {
		if err = s.BeforeStart(ctx); err != nil {
			return Job{}, err
		}
	}
	if _, err = d.run(ctx, "start", s.ID); err != nil {
		return Job{}, err
	}
	if d.Fault != nil {
		if err = d.Fault("after_docker_start", s); err != nil {
			return Job{}, err
		}
	}
	cancelChannel := ctx.Done()
	var drainTimeout <-chan time.Time
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	kill := func() error {
		stopCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		var err error
		job, err = d.Cancel(stopCtx, s.ID)
		if err != nil {
			return err
		}
		if job.Running {
			return domain.ErrReconciliation
		}
		observed = observed || job.Interrupted
		timer = time.NewTimer(15 * time.Second)
		drainTimeout = timer.C
		return nil
	}
	for {
		select {
		case err = <-done:
			drained = true
			inspectCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			final, inspectErr := d.Inspect(inspectCtx, s.ID)
			cancel()
			if inspectErr != nil {
				return job, inspectErr
			}
			final.Interrupted = job.Interrupted && !requested
			job = final
			if err != nil || job.Running || !job.Started {
				return job, ErrCaptureGap
			}
			complete = true
			if !requested {
				reason = ""
			}
			return job, nil
		case outputErr := <-failed:
			// Only an actual sink limit/error requests a kill. Runner context shutdown
			// never reaches this branch and never masquerades as output policy.
			requested = true
			reason = "spool_io_error"
			if errors.Is(outputErr, ErrLogLimit) {
				reason = "output_limit"
			}
			cancelChannel = nil
			if err = kill(); err != nil {
				return job, err
			}
		case <-cancelChannel:
			cancelChannel = nil
			if s.DetachOnCancel != nil && s.DetachOnCancel() {
				reason = "runner_shutdown"
				return job, ErrCaptureGap
			}
			// Business cancellation or the fixed operation deadline may stop the job;
			// keep reading until EOF rather than cancelling the attach reader first.
			if err = kill(); err != nil {
				return job, err
			}
		case <-drainTimeout:
			reason = "capture_drain_timeout"
			return job, ErrCaptureGap
		}
	}
}

// RemoveOwned refuses running jobs and validates all persisted identity facts.
// It is only called after the operation's durable terminal receipt commits.
func (d *Docker) RemoveOwned(ctx context.Context, name, containerID string) error {
	decoded, decodeErr := hex.DecodeString(containerID)
	if decodeErr != nil || len(decoded) != 32 || hex.EncodeToString(decoded) != containerID || domain.ID(name).Validate() != nil {
		return domain.ErrInvalid
	}
	job, err := d.Inspect(ctx, containerID)
	if errors.Is(err, ErrJobNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if job.Running || job.ContainerID != containerID || job.ContainerName != "/"+name || !job.StrictLogs {
		return domain.ErrReconciliation
	}
	_, err = d.run(ctx, "container", "rm", containerID)
	return err
}
