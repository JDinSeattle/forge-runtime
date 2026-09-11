package persistence

import (
	"context"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/dependency"
)

func (s *Store) DatabaseTime(ctx context.Context) (time.Time, error) {
	ctx, cancel := dependency.Database(ctx)
	defer cancel()
	var now time.Time
	err := s.Pool.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now)
	return now, err
}
