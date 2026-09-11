// Package httpapi exposes a control plane. It never invokes models or shell.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/artifact"
	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/eventstream"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	flow "github.com/JDinSeattle/forge-runtime/internal/runtime"
	"github.com/JDinSeattle/forge-runtime/internal/telemetry"
	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Source struct {
	BaseCommit string `json:"base_commit"`
	ProfileID  string `json:"profile_id"`
}
type Server struct {
	Store     *persistence.Store
	Artifacts artifact.Store
	Streams   *eventstream.Manager
	Configs   map[string]persistence.Config
	Sources   map[string]Source
	Logger    *slog.Logger
	Telemetry *telemetry.Telemetry
}
type contextKey string

const identityKey contextKey = "identity"
const requestKey contextKey = "request_id"

type APIError struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id"`
	Retryable bool   `json:"retryable"`
}

func (s *Server) Handler() http.Handler {
	r := chi.NewRouter()
	if s.Telemetry != nil {
		r.Use(s.Telemetry.Middleware)
	}
	r.Use(s.requestContext)
	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) { respond(w, 200, map[string]string{"status": "ok"}) })
	r.Get("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		if err := s.Store.Pool.Ping(ctx); err != nil {
			s.fail(w, r, err)
			return
		}
		respond(w, 200, map[string]string{"status": "ready"})
	})
	if s.Telemetry != nil {
		r.Handle("/metrics", s.Telemetry.Handler())
	} else {
		r.Handle("/metrics", promhttp.Handler())
	}
	r.Group(func(r chi.Router) {
		r.Use(s.authenticate)
		r.Post("/v1/projects", s.createProject)
		r.Post("/v1/projects/{id}/runs", s.submit)
		r.Get("/v1/runs/{id}", s.getRun)
		r.Get("/v1/runs/{id}/snapshot", s.getRun)
		r.Get("/v1/runs/{id}/events", s.events)
		r.Post("/v1/runs/{id}/cancel", s.cancel)
		r.Post("/v1/runs/{id}/resume", s.resume)
		r.Post("/v1/runs/{id}/messages", s.message)
		r.Get("/v1/approvals/{id}", s.getApproval)
		r.Post("/v1/approvals/{id}/decision", s.decide)
		r.Get("/v1/runs/{id}/artifacts", s.listArtifacts)
		r.Get("/v1/artifacts/{id}", s.download)
	})
	return r
}
func (s *Server) requestContext(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := string(persistence.NewID("request"))
		w.Header().Set("X-Request-ID", id)
		defer func() {
			if v := recover(); v != nil {
				if s.Logger != nil {
					s.Logger.Error("handler panic", "request_id", id)
				}
				s.fail(w, r, errors.New("handler panic"))
			}
		}()
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), requestKey, id)))
	})
}
func (s *Server) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		value := r.Header.Get("Authorization")
		if !strings.HasPrefix(value, "Bearer ") {
			s.fail(w, r, domain.ErrForbidden)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		i, err := s.Store.Authenticate(ctx, strings.TrimPrefix(value, "Bearer "), domain.ID(r.Header.Get("X-Forge-Tenant")))
		if err != nil {
			s.fail(w, r, err)
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), identityKey, i)))
	})
}
func identity(r *http.Request) persistence.Identity {
	return r.Context().Value(identityKey).(persistence.Identity)
}
func id(r *http.Request) domain.ID     { return domain.ID(chi.URLParam(r, "id")) }
func requestID(r *http.Request) string { v, _ := r.Context().Value(requestKey).(string); return v }
func decode(w http.ResponseWriter, r *http.Request, target any) error {
	if r.Header.Get("Content-Type") != "application/json" {
		return fmt.Errorf("%w: Content-Type must be application/json", domain.ErrInvalid)
	}
	r.Body = http.MaxBytesReader(w, r.Body, 128<<10)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(target); err != nil {
		return fmt.Errorf("%w: invalid JSON body", domain.ErrInvalid)
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return domain.ErrInvalid
	}
	return nil
}
func respond(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func (s *Server) fail(w http.ResponseWriter, r *http.Request, err error) {
	status, code, message, retry := 500, "internal", "request could not be completed", false
	switch {
	case errors.Is(err, domain.ErrInvalid):
		status, code, message = 400, "invalid_request", err.Error()
	case errors.Is(err, domain.ErrForbidden):
		status, code, message = 403, "forbidden", "authentication or permission denied"
	case errors.Is(err, domain.ErrNotFound):
		status, code, message = 404, "not_found", "resource not found"
	case errors.Is(err, eventstream.ErrReset):
		status, code, message = 410, "reset_required", "fetch snapshot and reconnect from covered_seq"
	case errors.Is(err, domain.ErrConflict), errors.Is(err, domain.ErrFenced), errors.Is(err, domain.ErrTerminal), errors.Is(err, domain.ErrTransition), errors.Is(err, domain.ErrReconciliation):
		status, code, message = 409, "conflict", err.Error()
	case errors.Is(err, domain.ErrCapacity):
		status, code, message, retry = 429, "capacity", "resource limit reached", true
	case errors.Is(err, context.DeadlineExceeded):
		status, code, message, retry = 503, "dependency_timeout", "dependency deadline exceeded", true
	}
	if status == 500 && s.Logger != nil {
		s.Logger.Error("API dependency failure", "request_id", requestID(r), "error", err)
	}
	respond(w, status, APIError{Code: code, Message: message, RequestID: requestID(r), Retryable: retry})
}
func (s *Server) createProject(w http.ResponseWriter, r *http.Request) {
	i := identity(r)
	if !i.IsAdmin() {
		s.fail(w, r, domain.ErrForbidden)
		return
	}
	var body struct {
		Name      string `json:"name"`
		SourceID  string `json:"source_id"`
		ProfileID string `json:"profile_id"`
	}
	if err := decode(w, r, &body); err != nil {
		s.fail(w, r, err)
		return
	}
	source, ok := s.Sources[body.SourceID]
	if !ok || source.ProfileID != body.ProfileID {
		s.fail(w, r, domain.ErrInvalid)
		return
	}
	p, err := s.Store.CreateProject(r.Context(), i.TenantID, body.Name, body.SourceID, body.ProfileID)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	respond(w, 201, p)
}

type Budget struct {
	MaxModelRounds    *uint64       `json:"max_model_rounds,omitempty"`
	MaxToolCalls      *uint64       `json:"max_tool_calls,omitempty"`
	MaxCost           *domain.Money `json:"max_cost_microusd,omitempty"`
	MaxRuntimeSeconds *int64        `json:"max_runtime_seconds,omitempty"`
}
type SubmitBody struct {
	Task       string `json:"task"`
	BaseCommit string `json:"base_commit"`
	ConfigID   string `json:"config_id"`
	Budget     Budget `json:"budget"`
}

func (b Budget) apply(c persistence.Config) (persistence.Config, error) {
	if b.MaxModelRounds != nil {
		if *b.MaxModelRounds > c.MaxModelRounds {
			return c, domain.ErrInvalid
		}
		c.MaxModelRounds = *b.MaxModelRounds
	}
	if b.MaxToolCalls != nil {
		if *b.MaxToolCalls > c.MaxToolCalls {
			return c, domain.ErrInvalid
		}
		c.MaxToolCalls = *b.MaxToolCalls
	}
	if b.MaxCost != nil {
		if *b.MaxCost > c.MaxCost || *b.MaxCost <= 0 {
			return c, domain.ErrInvalid
		}
		c.MaxCost = *b.MaxCost
	}
	if b.MaxRuntimeSeconds != nil {
		if *b.MaxRuntimeSeconds > c.MaxRuntimeSeconds {
			return c, domain.ErrInvalid
		}
		c.MaxRuntimeSeconds = *b.MaxRuntimeSeconds
	}
	return c, c.Validate()
}
func (s *Server) submit(w http.ResponseWriter, r *http.Request) {
	i := identity(r)
	if !i.CanWrite() {
		s.fail(w, r, domain.ErrForbidden)
		return
	}
	var body SubmitBody
	if err := decode(w, r, &body); err != nil {
		s.fail(w, r, err)
		return
	}
	project, err := s.Store.GetProject(r.Context(), i.TenantID, id(r))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	source, ok := s.Sources[project.SourceID]
	if !ok || source.BaseCommit != body.BaseCommit {
		s.fail(w, r, fmt.Errorf("%w: base_commit must identify the registered immutable source", domain.ErrInvalid))
		return
	}
	cfg, ok := s.Configs[body.ConfigID]
	if !ok {
		s.fail(w, r, domain.ErrInvalid)
		return
	}
	cfg, err = body.Budget.apply(cfg)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	result, reused, err := s.Store.Submit(r.Context(), persistence.SubmitRequest{TenantID: i.TenantID, PrincipalID: i.PrincipalID, ProjectID: project.ID, Task: body.Task, BaseCommit: body.BaseCommit, Config: cfg}, r.Header.Get("Idempotency-Key"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	w.Header().Set("Location", "/v1/runs/"+string(result.ID))
	respond(w, 202, map[string]any{"run_id": result.ID, "reused": reused})
}
func (s *Server) getRun(w http.ResponseWriter, r *http.Request) {
	v, err := s.Store.GetRun(r.Context(), identity(r).TenantID, id(r))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	v.Commands = nil
	respond(w, 200, v)
}
func (s *Server) cancel(w http.ResponseWriter, r *http.Request) {
	v, err := s.Store.Cancel(r.Context(), identity(r), id(r))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	v.Commands = nil
	respond(w, 202, v)
}
func (s *Server) resume(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ExpectedVersion uint64 `json:"expected_version"`
	}
	if err := decode(w, r, &body); err != nil {
		s.fail(w, r, err)
		return
	}
	v, err := s.Store.Resume(r.Context(), identity(r), id(r), body.ExpectedVersion)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	v.Commands = nil
	respond(w, 202, v)
}
func (s *Server) message(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Text string `json:"text"`
	}
	if err := decode(w, r, &body); err != nil {
		s.fail(w, r, err)
		return
	}
	v, reused, err := s.Store.AddMessage(r.Context(), identity(r), id(r), body.Text, r.Header.Get("Idempotency-Key"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	respond(w, 202, map[string]any{"message": v, "reused": reused})
}
func (s *Server) getApproval(w http.ResponseWriter, r *http.Request) {
	v, err := s.Store.GetApproval(r.Context(), identity(r).TenantID, id(r))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	respond(w, 200, v)
}
func (s *Server) decide(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Binding flow.ApprovalBinding `json:"binding"`
		Approve bool                 `json:"approve"`
	}
	if err := decode(w, r, &body); err != nil {
		s.fail(w, r, err)
		return
	}
	v, err := s.Store.Decide(r.Context(), identity(r), id(r), body.Binding, body.Approve)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	v.Commands = nil
	respond(w, 200, v)
}
func (s *Server) listArtifacts(w http.ResponseWriter, r *http.Request) {
	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			s.fail(w, r, domain.ErrInvalid)
			return
		}
		limit = n
	}
	v, err := s.Store.ListArtifacts(r.Context(), identity(r).TenantID, id(r), r.URL.Query().Get("after"), limit)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	respond(w, 200, v)
}
func (s *Server) download(w http.ResponseWriter, r *http.Request) {
	a, err := s.Store.GetArtifact(r.Context(), identity(r).TenantID, id(r))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	reader, err := s.Artifacts.Open(r.Context(), a.TenantID, a.RunID, artifact.Ref{TenantID: a.TenantID, RunID: a.RunID, Kind: a.Kind, ObjectKey: a.ObjectKey, SHA256: a.SHA256, Size: a.ByteSize})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	defer reader.Close()
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(a.ByteSize, 10))
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, a.ID))
	w.Header().Set("ETag", `"`+a.SHA256+`"`)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Cache-Control", "private, no-store")
	controller := http.NewResponseController(w)
	_ = controller.SetWriteDeadline(time.Now().Add(15 * time.Second))
	_, _ = io.CopyBuffer(w, reader, make([]byte, 32<<10))
}
