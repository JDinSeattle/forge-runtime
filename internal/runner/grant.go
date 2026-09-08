package runner

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
)

type Claims struct {
	Purpose     string    `json:"purpose,omitempty"`
	CleanupID   domain.ID `json:"cleanup_id,omitempty"`
	TenantID    domain.ID `json:"tenant_id"`
	RunID       domain.ID `json:"run_id"`
	WorkspaceID domain.ID `json:"workspace_id"`
	Epoch       uint64    `json:"epoch"`
	Permissions []string  `json:"permissions"`
	IssuedAt    time.Time `json:"issued_at"`
	ExpiresAt   time.Time `json:"expires_at"`
}
type Signer struct {
	key    []byte
	Skew   time.Duration
	MaxTTL time.Duration
}

func NewSigner(key []byte) (*Signer, error) {
	if len(key) < 32 {
		return nil, fmt.Errorf("%w: signing key needs at least 256 bits", domain.ErrInvalid)
	}
	return &Signer{key: append([]byte(nil), key...), Skew: time.Second, MaxTTL: 2 * time.Minute}, nil
}

// Sign requires the database lease expiry; a service identity alone cannot mint
// an arbitrarily long execution capability. Caller authenticates DB lease owner.
func (s *Signer) Sign(c Claims, leaseUntil time.Time) (string, error) {
	if c.Purpose != "" || c.CleanupID != "" {
		return "", domain.ErrForbidden
	}
	return s.sign(c, leaseUntil)
}

// SignCleanup uses an independent, database-backed maintenance lease. Its
// capability can only stop, inspect, snapshot or release the bound workspace;
// it can never prepare, adopt, verify or execute repository operations.
func (s *Signer) SignCleanup(c Claims, cleanupLeaseUntil time.Time) (string, error) {
	if c.Purpose != "cleanup" || c.CleanupID.Validate() != nil {
		return "", domain.ErrForbidden
	}
	return s.sign(c, cleanupLeaseUntil)
}
func (s *Signer) sign(c Claims, leaseUntil time.Time) (string, error) {
	if err := s.validate(c, c.IssuedAt); err != nil {
		return "", err
	}
	if c.ExpiresAt.After(leaseUntil.Add(-s.Skew)) {
		return "", fmt.Errorf("%w: grant outlives database lease safety margin", domain.ErrFenced)
	}
	body, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	encoded := base64.RawURLEncoding.EncodeToString(body)
	mac := hmac.New(sha256.New, s.key)
	_, _ = mac.Write([]byte(encoded))
	return encoded + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}
func (s *Signer) Verify(token string, request WorkspaceRequest, permission string, now time.Time) error {
	if len(token) > 8192 {
		return domain.ErrForbidden
	}
	parts := strings.Split(token, ".")
	if len(parts) != 2 {
		return domain.ErrForbidden
	}
	mac := hmac.New(sha256.New, s.key)
	_, _ = mac.Write([]byte(parts[0]))
	signature, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil || !hmac.Equal(signature, mac.Sum(nil)) {
		return domain.ErrForbidden
	}
	body, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return domain.ErrForbidden
	}
	var c Claims
	if json.Unmarshal(body, &c) != nil {
		return domain.ErrForbidden
	}
	if err = s.validate(c, now); err != nil {
		return err
	}
	if c.TenantID != request.TenantID || c.RunID != request.RunID || c.WorkspaceID != request.WorkspaceID || c.Epoch != request.Epoch {
		return domain.ErrForbidden
	}
	for _, p := range c.Permissions {
		if p == permission {
			return nil
		}
	}
	return domain.ErrForbidden
}
func (s *Signer) validate(c Claims, now time.Time) error {
	switch c.Purpose {
	case "":
		if c.CleanupID != "" {
			return domain.ErrForbidden
		}
	case "cleanup":
		if c.CleanupID.Validate() != nil {
			return domain.ErrForbidden
		}
	default:
		return domain.ErrForbidden
	}
	if len(c.Permissions) == 0 {
		return domain.ErrForbidden
	}
	for _, p := range c.Permissions {
		switch p {
		case "cancel", "snapshot", "inspect", "release":
		case "prepare", "adopt", "execute", "verify":
			if c.Purpose == "cleanup" {
				return domain.ErrForbidden
			}
		default:
			return domain.ErrForbidden
		}
	}

	if c.TenantID.Validate() != nil || c.RunID.Validate() != nil || c.WorkspaceID.Validate() != nil || c.Epoch == 0 || c.Epoch > uint64(^uint64(0)>>1) || c.IssuedAt.IsZero() || now.IsZero() || !c.ExpiresAt.After(now) || c.IssuedAt.After(now.Add(s.Skew)) || !c.ExpiresAt.After(c.IssuedAt) || c.ExpiresAt.Sub(c.IssuedAt) > s.MaxTTL {
		return domain.ErrFenced
	}
	return nil
}
