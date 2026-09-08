package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	api "github.com/JDinSeattle/forge-runtime/internal/httpcontract"
)

const maxJSONBytes = 2 << 20

type settings struct {
	endpoint, tenant, token, stateDir string
	timeout                           time.Duration
}

func settingsFromEnv() settings {
	dir := os.Getenv("FORGE_STATE_DIR")
	if dir == "" {
		dir = os.Getenv("XDG_STATE_HOME")
		if dir == "" {
			home, _ := os.UserHomeDir()
			dir = filepath.Join(home, ".local", "state")
		}
		dir = filepath.Join(dir, "forge")
	}
	endpoint := os.Getenv("FORGE_API_URL")
	if endpoint == "" {
		endpoint = "http://127.0.0.1:8080"
	}
	return settings{endpoint: endpoint, tenant: os.Getenv("FORGE_TENANT"), token: os.Getenv("FORGE_TOKEN"), stateDir: dir, timeout: 20 * time.Second}
}
func (s settings) client() (*api.Client, error) {
	u, err := url.Parse(s.endpoint)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("FORGE_API_URL must be an HTTP(S) base URL without credentials, query, or fragment")
	}
	if u.Scheme == "http" {
		host := u.Hostname()
		ip := net.ParseIP(host)
		if host != "localhost" && (ip == nil || !ip.IsLoopback()) {
			return nil, errors.New("remote FORGE_API_URL requires HTTPS")
		}
	}
	if s.token == "" || strings.ContainsAny(s.token, "\r\n") {
		return nil, errors.New("set FORGE_TOKEN to a valid bearer token")
	}
	if !validID(s.tenant) {
		return nil, errors.New("set FORGE_TENANT to a valid tenant ID")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ResponseHeaderTimeout = 10 * time.Second
	transport.MaxResponseHeaderBytes = 32 << 10
	client := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return api.NewClient(strings.TrimRight(s.endpoint, "/"), api.WithHTTPClient(client), api.WithRequestEditorFn(func(ctx context.Context, r *http.Request) error {
		r.Header.Set("Authorization", "Bearer "+s.token)
		r.Header.Set("X-Forge-Tenant", s.tenant)
		r.Header.Set("Accept", "application/json")
		return nil
	}))
}

func validID(id string) bool {
	if len(id) < 1 || len(id) > 128 {
		return false
	}
	for _, c := range id {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '-') {
			return false
		}
	}
	return true
}
func readBounded(r io.Reader, limit int64) ([]byte, error) {
	raw, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > limit {
		return nil, fmt.Errorf("response exceeds %d-byte limit", limit)
	}
	return raw, nil
}
func decodeResponse(response *http.Response, target any, expected int) error {
	if response == nil {
		return errors.New("missing HTTP response")
	}
	defer response.Body.Close()
	raw, err := readBounded(response.Body, maxJSONBytes)
	if err != nil {
		return err
	}
	if response.StatusCode != expected {
		var failure api.APIError
		if json.Unmarshal(raw, &failure) == nil && failure.Code != "" {
			return fmt.Errorf("HTTP %d: %s (request %s): %s", response.StatusCode, failure.Code, failure.RequestId, failure.Message)
		}
		return fmt.Errorf("HTTP %d: response was not an expected API result", response.StatusCode)
	}
	media, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || media != "application/json" {
		return errors.New("expected application/json response")
	}
	if err = checkJSONDepth(raw, 64); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err = decoder.Decode(target); err != nil {
		return fmt.Errorf("invalid API JSON: %w", err)
	}
	return nil
}
func printJSON(w io.Writer, value any) error { return json.NewEncoder(w).Encode(value) }
func checkJSONDepth(raw []byte, maxDepth int) error {
	depth := 0
	quoted, escape := false, false
	for _, b := range raw {
		if quoted {
			if escape {
				escape = false
			} else if b == '\\' {
				escape = true
			} else if b == '"' {
				quoted = false
			}
			continue
		}
		switch b {
		case '"':
			quoted = true
		case '{', '[':
			depth++
			if depth > maxDepth {
				return errors.New("JSON nesting limit exceeded")
			}
		case '}', ']':
			depth--
		}
	}
	if !json.Valid(raw) {
		return errors.New("invalid JSON")
	}
	return nil
}

func (s settings) context(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, s.timeout)
}
