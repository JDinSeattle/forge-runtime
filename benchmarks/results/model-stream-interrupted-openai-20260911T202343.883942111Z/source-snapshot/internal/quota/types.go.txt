// Package quota coordinates credential-group capacity across all workers using
// PostgreSQL row locks. This is separate from runner slots and tenant run slots.
package quota

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
)

var (
	ErrInvalid           = errors.New("invalid quota request")
	ErrConflict          = errors.New("quota idempotency conflict")
	ErrNotFound          = errors.New("quota reservation not found")
	ErrAlreadyDispatched = errors.New("request may already have been dispatched; reconcile instead of resending")
)

type CapacityError struct {
	Reason  string
	RetryAt time.Time
}

func (e *CapacityError) Error() string { return "provider quota unavailable: " + e.Reason }
func (e *CapacityError) Unwrap() error { return domain.ErrCapacity }

type Config struct {
	CredentialGroup  string
	MaxConcurrent    int
	MaxTokens        int64
	MaxCost          domain.Money
	WindowDuration   time.Duration
	FailureThreshold int
	BreakerCooldown  time.Duration
}

type Request struct {
	TenantID        string
	RunID           string
	AttemptID       string
	CredentialGroup string
	InputTokens     int64
	MaxOutputTokens int64
	MaxCost         domain.Money
	PriceVersion    string
	RequestDeadline time.Time
}

func (r Request) normalized() (Request, int64, string, error) {
	if domain.ID(r.TenantID).Validate() != nil || domain.ID(r.RunID).Validate() != nil || domain.ID(r.AttemptID).Validate() != nil || r.CredentialGroup == "" || len(r.CredentialGroup) > 128 || r.PriceVersion == "" || r.RequestDeadline.IsZero() {
		return r, 0, "", fmt.Errorf("%w: missing request identity, price version, or deadline", ErrInvalid)
	}
	if r.InputTokens < 0 || r.MaxOutputTokens <= 0 || r.InputTokens > math.MaxInt64-r.MaxOutputTokens || r.MaxCost < 0 {
		return r, 0, "", fmt.Errorf("%w: invalid token or cost reservation", ErrInvalid)
	}
	r.RequestDeadline = r.RequestDeadline.UTC().Truncate(time.Microsecond)
	raw, _ := json.Marshal(r)
	sum := sha256.Sum256(raw)
	return r, r.InputTokens + r.MaxOutputTokens, hex.EncodeToString(sum[:]), nil
}

type Reservation struct {
	TenantID         string        `json:"tenant_id"`
	RunID            string        `json:"run_id"`
	AttemptID        string        `json:"attempt_id"`
	CredentialGroup  string        `json:"credential_group"`
	Tokens           int64         `json:"tokens"`
	Cost             domain.Money  `json:"microusd"`
	Status           string        `json:"status"`
	SlotReleased     bool          `json:"slot_released"`
	RequestDeadline  time.Time     `json:"request_deadline"`
	DispatchedAt     *time.Time    `json:"dispatched_at,omitempty"`
	ActualTokens     *int64        `json:"actual_tokens,omitempty"`
	ActualCost       *domain.Money `json:"actual_microusd,omitempty"`
	SettlementKind   *string       `json:"settlement_kind,omitempty"`
	Outcome          *string       `json:"outcome,omitempty"`
	WindowGeneration int64         `json:"window_generation"`
	requestHash      string
}

// Settlement requires definitive, complete usage and priced cost. When any
// usage/cost is missing, call MarkUnknown and retain the conservative reserve.
// All actual consumption, including overages, is recorded even above limits.
type Settlement struct {
	Tokens int64
	Cost   domain.Money
}

type Snapshot struct {
	CredentialGroup     string
	MaxConcurrent       int
	MaxTokens           int64
	MaxCost             domain.Money
	ActiveRequests      int
	ReservedTokens      int64
	CommittedTokens     int64
	ReservedCost        domain.Money
	CommittedCost       domain.Money
	WindowEndsAt        time.Time
	WindowGeneration    int64
	BreakerUntil        *time.Time
	ConsecutiveFailures int
	ProbeTenantID       *string
	ProbeAttemptID      *string
	windowDurationMS    int64
	failureThreshold    int
	breakerCooldownMS   int64
}
