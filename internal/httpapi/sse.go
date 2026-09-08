package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/eventstream"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	"github.com/JDinSeattle/forge-runtime/internal/telemetry"
)

func (s *Server) events(w http.ResponseWriter, r *http.Request) {
	var after uint64
	if value := r.Header.Get("Last-Event-ID"); value != "" {
		n, err := strconv.ParseUint(value, 10, 63)
		if err != nil {
			s.fail(w, r, domain.ErrInvalid)
			return
		}
		after = n
	}
	i := identity(r)
	sub, err := s.Streams.Subscribe(r.Context(), i.TenantID, id(r), after)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	defer sub.Close()
	reason := telemetry.SSEClientClosed
	if s.Telemetry != nil {
		end := s.Telemetry.StartSSE()
		defer func() { end(reason) }()
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache, no-transform")
	w.Header().Set("X-Accel-Buffering", "no")
	ctl := http.NewResponseController(w)
	written := false
	write := func(e persistence.Event) error {
		body, err := json.Marshal(e)
		if err != nil {
			reason = telemetry.SSEFailed
			return err
		}
		_ = ctl.SetWriteDeadline(time.Now().Add(5 * time.Second))
		written = true
		if _, err = fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", e.Seq, e.Type, body); err != nil {
			return err
		}
		return ctl.Flush()
	}
	for after < sub.CoveredSeq {
		events, err := s.Store.Events(r.Context(), i.TenantID, id(r), after, 128)
		if err != nil {
			if errors.Is(err, domain.ErrReset) {
				reason = telemetry.SSEReset
			} else {
				reason = telemetry.SSEFailed
			}
			if !written {
				s.fail(w, r, err)
			}
			return
		}
		if len(events) == 0 {
			return
		}
		for _, e := range events {
			if e.Seq > sub.CoveredSeq {
				break
			}
			if e.Seq != after+1 {
				if !written {
					s.fail(w, r, domain.ErrReset)
				}
				return
			}
			if err = write(e); err != nil {
				return
			}
			after = e.Seq
			if e.Type == "run.finished" {
				reason = telemetry.SSEFinished
				return
			}
		}
	}
	_ = ctl.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err = fmt.Fprint(w, ": connected\n\n"); err != nil {
		return
	}
	if err = ctl.Flush(); err != nil {
		return
	}
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case e, ok := <-sub.Events:
			if !ok {
				return
			}
			if e.Seq <= after {
				continue
			}
			if err = write(e); err != nil {
				return
			}
			after = e.Seq
			if e.Type == "run.finished" {
				reason = telemetry.SSEFinished
				return
			}
		case streamErr, ok := <-sub.Errors:
			if !ok {
				return
			}
			if errors.Is(streamErr, eventstream.ErrSlow) {
				reason = telemetry.SSESlowSubscriber
			} else if errors.Is(streamErr, domain.ErrReset) {
				reason = telemetry.SSEReset
			} else {
				reason = telemetry.SSEShutdown
			}
			return
		case <-ticker.C:
			// Long-lived subscriptions recheck token expiry/revocation and membership.
			token := r.Header.Get("Authorization")[len("Bearer "):]
			if _, err = s.Store.Authenticate(r.Context(), token, i.TenantID); err != nil {
				reason = telemetry.SSEUnauthorized
				return
			}
			_ = ctl.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if _, err = fmt.Fprint(w, ": heartbeat\n\n"); err != nil {
				return
			}
			if err = ctl.Flush(); err != nil {
				return
			}
		}
	}
}
