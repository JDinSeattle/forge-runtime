package httpapi

import (
	"errors"
	"io"
	"net"

	"github.com/jackc/pgx/v5/pgconn"
)

// Availability is distinct from malformed SQL, bad credentials or other
// permanent configuration failures. A retryable response does not establish
// whether a mutation committed: callers retain their key/version semantics.
func dependencyUnavailable(err error) bool {
	var postgres *pgconn.PgError
	if errors.As(err, &postgres) {
		switch postgres.Code {
		case "08000", "08001", "08003", "08004", "08006", "08007", "53300", "57P01", "57P02", "57P03":
			return true
		default:
			return false
		}
	}
	var transport *net.OpError
	return errors.As(err, &transport) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed)
}
