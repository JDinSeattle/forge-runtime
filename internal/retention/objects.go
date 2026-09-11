// Package retention coordinates local object collection with both durable
// reference authorities. It is operator-only and requires an all-tenant role.
package retention

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"path"
	"path/filepath"
	"strings"

	"github.com/JDinSeattle/forge-runtime/internal/artifact"
	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	_ "modernc.org/sqlite"
)

// Collect implements the single-runner local deployment's retention boundary.
// The supplied journal must be the authoritative configured runner journal,
// and every writer must have been upgraded to the publication-lock protocol.
// It never retires READY metadata or journal pins; that requires a separate
// retention policy with stronger evidence than the age of an object.
func Collect(ctx context.Context, objects *artifact.LocalStore, db *persistence.Store, journalPath, runnerID string, options artifact.CollectionOptions) (artifact.CollectionResult, error) {
	if !filepath.IsAbs(journalPath) || runnerID == "" {
		return artifact.CollectionResult{}, domain.ErrInvalid
	}
	return objects.Collect(ctx, options, func(ctx context.Context) (map[string]bool, error) {
		var allTenants, otherRunner bool
		err := db.Pool.QueryRow(ctx, `SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname=current_user`).Scan(&allTenants)
		if err != nil {
			return nil, err
		}
		if !allTenants {
			return nil, fmt.Errorf("%w: collection needs all-tenant visibility", domain.ErrForbidden)
		}
		err = db.Pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM runner_allocations WHERE runner_id<>$1)`, runnerID).Scan(&otherRunner)
		if err != nil {
			return nil, err
		}
		if otherRunner {
			return nil, fmt.Errorf("%w: collection requires all runner journals; this command supports one local runner", domain.ErrInvalid)
		}
		refs := map[string]bool{}
		rows, err := db.Pool.Query(ctx, `SELECT object_key FROM artifacts`)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var key string
			if err = rows.Scan(&key); err != nil {
				rows.Close()
				return nil, err
			}
			if err = add(refs, key); err != nil {
				rows.Close()
				return nil, err
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
		if err = journalReferences(ctx, journalPath, refs, func(identity string) error { return objects.CheckJournal(ctx, journalPath, identity) }); err != nil {
			return nil, err
		}
		return refs, nil
	})
}

func add(refs map[string]bool, key string) error {
	parts := strings.Split(key, "/")
	if len(parts) != 3 || domain.ID(parts[0]).Validate() != nil || domain.ID(parts[1]).Validate() != nil {
		return fmt.Errorf("%w: malformed authoritative artifact key", domain.ErrInvalid)
	}
	digest, err := hex.DecodeString(parts[2])
	if err != nil || len(digest) != 32 || hex.EncodeToString(digest) != parts[2] {
		return fmt.Errorf("%w: malformed authoritative artifact digest", domain.ErrInvalid)
	}
	refs[key] = true
	if len(refs) > 1000000 {
		return domain.ErrCapacity
	}
	return nil
}

func journalReferences(ctx context.Context, filename string, refs map[string]bool, verify ...func(string) error) error {
	// mode=ro fails if missing, reads committed WAL state, and never silently
	// creates a new empty journal or migrates an unknown schema during GC.
	u := url.URL{Scheme: "file", Path: filename, RawQuery: url.Values{"mode": {"ro"}, "_pragma": {"busy_timeout(5000)"}}.Encode()}
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var version int
	if err = tx.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return err
	}
	if version != 4 {
		return fmt.Errorf("%w: artifact GC requires runner journal schema 4, got %d", domain.ErrInvalid, version)
	}
	var identity string
	if err = tx.QueryRowContext(ctx, `SELECT id FROM journal_identity WHERE singleton=1`).Scan(&identity); err != nil {
		return err
	}
	rawID, err := hex.DecodeString(identity)
	if err != nil || len(rawID) != 32 || hex.EncodeToString(rawID) != identity {
		return fmt.Errorf("%w: invalid journal identity", domain.ErrInvalid)
	}
	for _, check := range verify {
		if err = check(identity); err != nil {
			return err
		}
	}
	rows, err := tx.QueryContext(ctx, `SELECT receipt_json,0 FROM operations UNION ALL SELECT ref_json,1 FROM runner_artifacts`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var raw string
		var pin bool
		if err = rows.Scan(&raw, &pin); err != nil {
			return err
		}
		var ref artifact.Ref
		if err = json.Unmarshal([]byte(raw), &ref); err != nil {
			return err
		}
		if ref.ObjectKey == "" && !pin && strings.TrimSpace(raw) == "{}" {
			continue
		}
		if ref.Size < 0 || ref.Kind == "" || ref.ObjectKey != path.Join(string(ref.TenantID), string(ref.RunID), ref.SHA256) {
			return fmt.Errorf("%w: malformed runner artifact reference", domain.ErrInvalid)
		}
		if err = add(refs, ref.ObjectKey); err != nil {
			return err
		}
	}
	if err = rows.Err(); err != nil {
		return err
	}
	return tx.Commit()
}
