package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

	api "github.com/JDinSeattle/forge-runtime/internal/httpcontract"
	"github.com/spf13/cobra"
)

const maxEventBytes = 1 << 20

var errStreamProtocol = errors.New("invalid event stream")
var errEventOutput = errors.New("event output failed")

type watchOptions struct {
	after         uint64
	maxReconnects int
	idleTimeout   time.Duration
	wait          func(context.Context, time.Duration) error
}

func watchCommand(s *settings) *cobra.Command {
	opt := watchOptions{maxReconnects: 20, idleTimeout: 45 * time.Second, wait: waitContext}
	cmd := &cobra.Command{Use: "watch RUN_ID", Short: "Watch SSE with cursor replay, deduplication, and snapshot recovery", Args: oneID, RunE: func(cmd *cobra.Command, args []string) error {
		if opt.maxReconnects < 0 || opt.maxReconnects > 1000 {
			return errors.New("--max-reconnects must be 0..1000")
		}
		client, err := s.client()
		if err != nil {
			return err
		}
		return watch(cmd.Context(), client, args[0], cmd.OutOrStdout(), cmd.ErrOrStderr(), opt)
	}}
	cmd.Flags().Uint64Var(&opt.after, "after", 0, "Resume after this event sequence")
	cmd.Flags().IntVar(&opt.maxReconnects, "max-reconnects", 20, "Maximum reconnects without progress")
	return cmd
}
func waitContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
func watch(ctx context.Context, client *api.Client, runID string, out, diagnostics io.Writer, opt watchOptions) error {
	after := opt.after
	failures := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		cursor := strconv.FormatUint(after, 10)
		response, err := client.WatchRun(ctx, runID, &api.WatchRunParams{LastEventID: &cursor}, func(ctx context.Context, r *http.Request) error {
			r.Header.Set("Accept", "text/event-stream")
			return nil
		})
		before := after
		delay := 500 * time.Millisecond
		if err == nil {
			switch response.StatusCode {
			case http.StatusGone:
				response.Body.Close()
				snapshotCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
				snapshotResponse, getErr := client.GetSnapshot(snapshotCtx, runID)
				var run api.Run
				if getErr == nil {
					getErr = decodeResponse(snapshotResponse, &run, 200)
				}
				cancel()
				if getErr != nil {
					return fmt.Errorf("snapshot reset failed: %w", getErr)
				}
				if run.Id != runID || run.State.RunId != runID || run.CoveredSeq < after {
					return fmt.Errorf("%w: snapshot identity or cursor regressed", errStreamProtocol)
				}
				after = run.CoveredSeq
				if err = printJSON(out, map[string]any{"type": "snapshot.reset", "run": run}); err != nil {
					return err
				}
				if terminal(run.State.Status) {
					return nil
				}
			case http.StatusOK:
				media, _, parseErr := mime.ParseMediaType(response.Header.Get("Content-Type"))
				if parseErr != nil || media != "text/event-stream" {
					response.Body.Close()
					return fmt.Errorf("%w: expected text/event-stream", errStreamProtocol)
				}
				timer := time.AfterFunc(opt.idleTimeout, func() { response.Body.Close() })
				done, readErr := consumeEvents(&activityReader{Reader: response.Body, touch: func() { timer.Reset(opt.idleTimeout) }}, runID, &after, out)
				timer.Stop()
				response.Body.Close()
				if done {
					return nil
				}
				if errors.Is(readErr, errStreamProtocol) || errors.Is(readErr, errEventOutput) {
					return readErr
				}
				err = readErr
			case http.StatusTooManyRequests, http.StatusServiceUnavailable:
				if value := response.Header.Get("Retry-After"); value != "" {
					if seconds, parseErr := strconv.ParseInt(value, 10, 32); parseErr == nil && seconds > 0 {
						delay = time.Duration(seconds) * time.Second
					} else if at, parseErr := http.ParseTime(value); parseErr == nil && time.Until(at) > 0 {
						delay = time.Until(at)
					}
				}
				var ignored any
				err = decodeResponse(response, &ignored, http.StatusOK)
			default:
				var ignored any
				return decodeResponse(response, &ignored, http.StatusOK)
			}
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if after > before {
			failures = 0
		} else {
			failures++
		}
		if failures > opt.maxReconnects {
			return fmt.Errorf("watch reconnect budget exhausted at event %d: %v", after, err)
		}
		if delay > 30*time.Second {
			return fmt.Errorf("server requested a long retry delay (%s); resume watch later with --after %d", delay, after)
		}
		if failures > 1 {
			backoff := 500 * time.Millisecond
			for n := 1; n < failures && backoff < 5*time.Second; n++ {
				backoff *= 2
			}
			if backoff > 5*time.Second {
				backoff = 5 * time.Second
			}
			if backoff > delay {
				delay = backoff
			}
		}
		fmt.Fprintf(diagnostics, "Reconnecting after event %d\n", after)
		if err = opt.wait(ctx, delay); err != nil {
			return err
		}
	}
}
func terminal(status api.RunStatus) bool {
	switch status {
	case api.RunStatusCompleted, api.RunStatusCancelled, api.RunStatusFailed, api.RunStatusBudgetExhausted:
		return true
	}
	return false
}

type activityReader struct {
	io.Reader
	touch func()
}

func (r *activityReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if n > 0 {
		r.touch()
	}
	return n, err
}

func consumeEvents(reader io.Reader, runID string, after *uint64, out io.Writer) (bool, error) {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 4096), maxEventBytes)
	var id, kind string
	var data bytes.Buffer
	dispatch := func() (bool, error) {
		defer func() { id = ""; kind = ""; data.Reset() }()
		if data.Len() == 0 {
			return false, nil
		}
		seq, err := strconv.ParseUint(id, 10, 63)
		if err != nil || seq == 0 {
			return false, fmt.Errorf("%w: missing or invalid event ID", errStreamProtocol)
		}
		raw := bytes.TrimSuffix(data.Bytes(), []byte{'\n'})
		if err = checkJSONDepth(raw, 64); err != nil {
			return false, fmt.Errorf("%w: %v", errStreamProtocol, err)
		}
		var e api.Event
		if err = json.Unmarshal(raw, &e); err != nil {
			return false, fmt.Errorf("%w: invalid event JSON", errStreamProtocol)
		}
		if e.RunId != runID || e.Seq != seq || e.Type == "" || (kind != "" && kind != e.Type) || e.SchemaVersion != 1 {
			return false, fmt.Errorf("%w: envelope identity, type, or schema version mismatch", errStreamProtocol)
		}
		if seq <= *after {
			return false, nil
		}
		if seq != *after+1 {
			return false, fmt.Errorf("%w: sequence gap after %d", errStreamProtocol, *after)
		}
		if err = printJSON(out, json.RawMessage(raw)); err != nil {
			return false, fmt.Errorf("%w: %v", errEventOutput, err)
		}
		*after = seq
		return e.Type == "run.finished", nil
	}
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			done, err := dispatch()
			if done || err != nil {
				return done, err
			}
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		field, value, found := strings.Cut(line, ":")
		if !found {
			value = ""
		}
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "id":
			if id != "" {
				return false, fmt.Errorf("%w: duplicate event ID field", errStreamProtocol)
			}
			id = value
		case "event":
			kind = value
		case "data":
			if len(value)+1 > maxEventBytes-data.Len() {
				return false, fmt.Errorf("%w: event exceeds size limit", errStreamProtocol)
			}
			data.WriteString(value)
			data.WriteByte('\n')
		}
	}
	if err := scanner.Err(); err != nil {
		if errors.Is(err, bufio.ErrTooLong) {
			return false, fmt.Errorf("%w: line exceeds size limit", errStreamProtocol)
		}
		return false, err
	}
	// An unterminated frame is deliberately discarded; reconnect from the last
	// completely printed event instead of acknowledging partial JSON.
	return false, io.EOF
}
