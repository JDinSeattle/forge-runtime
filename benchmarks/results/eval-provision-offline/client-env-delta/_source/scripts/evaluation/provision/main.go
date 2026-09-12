// Command eval-provision creates one operator-selected private evaluation schema.
// It never launches services, reads a model/signing key, or executes a model.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		// Database errors can contain SQL/password text. Only the fixed stage and
		// SQLSTATE are public; no error wrapping, DSN, token, or credential digest.
		var staged *stageError
		if errors.As(err, &staged) {
			fmt.Fprintf(os.Stderr, "provision stopped at %s (SQLSTATE %s); retain private descriptors, do not retry into another batch\n", staged.stage, sqlState(staged.err))
		} else {
			fmt.Fprintln(os.Stderr, "provision preflight rejected; use the documented candidate and fresh private output contract")
		}
		os.Exit(1)
	}
	fmt.Println("Private exact-schema provisioning ready. No service or provider was started.")
}
func run(args []string) error {
	f := flag.NewFlagSet("eval-provision", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	candidate := f.String("candidate", "", "absolute prepared candidate directory")
	output := f.String("output", "", "new sibling private directory")
	if e := f.Parse(args); e != nil {
		return e
	}
	if f.NArg() != 0 {
		return errors.New("unexpected arguments")
	}
	b, e := loadBundle(*candidate, *output)
	if e != nil {
		return e
	}
	admin, e := adminURL(os.Getenv("FORGE_EVAL_ADMIN_DATABASE_URL"))
	if e != nil {
		return e
	}
	// libpq service/options could supply authority outside an explicit URL. We do
	// not read their contents or any other credential source.
	for _, k := range []string{"PGSERVICE", "PGSERVICEFILE", "PGOPTIONS"} {
		if os.Getenv(k) != "" {
			return errors.New("ambient database routing is forbidden")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	return provision(ctx, b, admin)
}

type stageError struct {
	stage string
	err   error
}

func (e *stageError) Error() string { return "provision failed at " + e.stage }
func (e *stageError) Unwrap() error { return e.err }
func sqlState(e error) string {
	var p *pgconn.PgError
	if errors.As(e, &p) {
		return p.Code
	}
	return "unavailable"
}
func randomSuffix() (string, error) {
	var b [16]byte
	_, e := rand.Read(b[:])
	return hex.EncodeToString(b[:]), e
}
func writePrivate(path string, b []byte) error {
	f, e := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		return e
	}
	_, e = f.Write(b)
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	if e != nil {
		return e
	}
	return syncDirectory(filepath.Dir(path))
}
func syncDirectory(path string) error {
	d, e := os.Open(path)
	if e != nil {
		return e
	}
	defer d.Close()
	return d.Sync()
}
func writeJSON(path string, v any) error {
	b, e := json.MarshalIndent(v, "", "  ")
	if e != nil {
		return e
	}
	return writePrivate(path, append(b, '\n'))
}
func quote(value string) string {
	// The environment files are literal shell assignments, not command sources.
	out := "'"
	for _, r := range value {
		if r == '\'' {
			out += "'\"'\"'"
		} else {
			out += string(r)
		}
	}
	return out + "'"
}

type stages struct {
	out    string
	number int
	last   string
}

func (s *stages) record(phase string, body any) error {
	s.last = phase
	s.number++
	return writeJSON(filepath.Join(s.out, fmt.Sprintf("stage-%02d-%s.json", s.number, phase)), body)
}
