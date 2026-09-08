package runner

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
)

func TestCleanupGrantIsPurposeRestrictedAndLeaseBound(t *testing.T) {
	s, _ := NewSigner([]byte(strings.Repeat("c", 32)))
	now := time.Now()
	lease := now.Add(time.Minute)
	c := Claims{Purpose: "cleanup", CleanupID: "cleanup-1", TenantID: "tenant", RunID: "run", WorkspaceID: "workspace", Epoch: 2, IssuedAt: now, ExpiresAt: now.Add(20 * time.Second), Permissions: []string{"cancel", "snapshot", "inspect", "release"}}
	token, err := s.SignCleanup(c, lease)
	if err != nil {
		t.Fatal(err)
	}
	r := WorkspaceRequest{TenantID: c.TenantID, RunID: c.RunID, WorkspaceID: c.WorkspaceID, Epoch: c.Epoch}
	for _, p := range c.Permissions {
		if err = s.Verify(token, r, p, now); err != nil {
			t.Fatalf("cleanup %s: %v", p, err)
		}
	}
	for _, p := range []string{"prepare", "adopt", "execute", "verify"} {
		if err = s.Verify(token, r, p, now); !errors.Is(err, domain.ErrForbidden) {
			t.Fatalf("cleanup allowed %s: %v", p, err)
		}
		unsafe := c
		unsafe.Permissions = []string{p}
		if _, err = s.SignCleanup(unsafe, lease); !errors.Is(err, domain.ErrForbidden) {
			t.Fatalf("signed execution cleanup: %v", err)
		}
		// Defense at Verify is independent of the trusted caller's signing path.
		raw, _ := json.Marshal(unsafe)
		body := base64.RawURLEncoding.EncodeToString(raw)
		mac := hmac.New(sha256.New, s.key)
		mac.Write([]byte(body))
		forged := body + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
		if err = s.Verify(forged, r, p, now); !errors.Is(err, domain.ErrForbidden) {
			t.Fatalf("verified malformed cleanup capability: %v", err)
		}
	}
	if _, err = s.Sign(c, lease); !errors.Is(err, domain.ErrForbidden) {
		t.Fatal("execution signer accepted maintenance purpose")
	}
	c.ExpiresAt = lease
	if _, err = s.SignCleanup(c, lease); !errors.Is(err, domain.ErrFenced) {
		t.Fatal("cleanup grant exceeded independent lease margin")
	}
}
