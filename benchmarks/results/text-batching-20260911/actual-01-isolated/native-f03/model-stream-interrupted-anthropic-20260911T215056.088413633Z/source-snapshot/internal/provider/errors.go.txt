package provider

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type ErrorKind string

const (
	ErrAuthentication ErrorKind = "authentication"
	ErrInvalidRequest ErrorKind = "invalid_request"
	ErrUnsupported    ErrorKind = "unsupported_capability"
	ErrRateLimited    ErrorKind = "rate_limited"
	ErrUnavailable    ErrorKind = "unavailable"
	ErrDeadline       ErrorKind = "deadline"
	ErrCancelled      ErrorKind = "cancelled"
	ErrProtocol       ErrorKind = "protocol"
	ErrLimit          ErrorKind = "stream_limit"
	ErrInterrupted    ErrorKind = "stream_interrupted"
	ErrConsumer       ErrorKind = "event_consumer"
)

// Error is safe to log: Detail never includes a remote error body or credentials.
// Retryable is advisory; the durable application owns budgets and scheduling.
type Error struct {
	Kind       ErrorKind
	StatusCode int
	RetryAfter time.Duration
	RequestID  string
	Detail     string
	Cause      error
}

func (e *Error) Error() string { return fmt.Sprintf("provider %s: %s", e.Kind, e.Detail) }
func (e *Error) Unwrap() error { return e.Cause }
func (e *Error) Retryable() bool {
	return e.Kind == ErrRateLimited || e.Kind == ErrUnavailable || e.Kind == ErrInterrupted
}

func classify(err error) *Error {
	var p *Error
	if errors.As(err, &p) {
		return p
	}
	if errors.Is(err, context.Canceled) {
		return &Error{Kind: ErrCancelled, Detail: "request cancelled", Cause: err}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return &Error{Kind: ErrDeadline, Detail: "request deadline reached", Cause: err}
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return &Error{Kind: ErrUnavailable, Detail: "transport failed", Cause: err}
	}
	return &Error{Kind: ErrInterrupted, Detail: "stream did not complete", Cause: err}
}

// HTTPError maps provider status codes without leaking response bodies. Retry-
// After supports delta-seconds and HTTP-date using the application's clock.
func HTTPError(status int, h http.Header, now time.Time) *Error {
	kind := ErrInvalidRequest
	switch {
	case status == 401 || status == 403:
		kind = ErrAuthentication
	case status == 429:
		kind = ErrRateLimited
	case status == 408 || status == 409 || status >= 500:
		kind = ErrUnavailable
	}
	id := h.Get("x-request-id")
	if id == "" {
		id = h.Get("request-id")
	}
	return &Error{Kind: kind, StatusCode: status, RetryAfter: ParseRetryAfter(h.Get("Retry-After"), now), RequestID: id, Detail: http.StatusText(status)}
}

func ParseRetryAfter(value string, now time.Time) time.Duration {
	value = strings.TrimSpace(value)
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil {
		if seconds <= 0 {
			return 0
		}
		// Saturate before multiplication; an untrusted header cannot wrap negative.
		if seconds > int64((time.Duration(1<<63-1))/time.Second) {
			return time.Duration(1<<63 - 1)
		}
		return time.Duration(seconds) * time.Second
	}
	if at, err := http.ParseTime(value); err == nil && at.After(now) {
		return at.Sub(now)
	}
	return 0
}
