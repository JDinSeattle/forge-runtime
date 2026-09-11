package persistence

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/dependency"
	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/jackc/pgx/v5"
)

type Artifact struct {
	TenantID  domain.ID `json:"tenant_id"`
	ID        domain.ID `json:"id"`
	RunID     domain.ID `json:"run_id"`
	Kind      string    `json:"kind"`
	ObjectKey string    `json:"object_key"`
	SHA256    string    `json:"sha256"`
	ByteSize  int64     `json:"byte_size"`
	State     string    `json:"state"`
	CreatedAt time.Time `json:"created_at"`
}

// PublishArtifact is service-only. The caller must first durably write and
// verify bytes in ArtifactStore; clients never supply ready-object metadata.
func (s *Store) PublishArtifact(ctx context.Context, a Artifact) error {
	ctx, cancel := dependency.Database(ctx)
	defer cancel()
	if a.TenantID.Validate() != nil || a.RunID.Validate() != nil || a.ID.Validate() != nil || a.Kind == "" || a.ByteSize < 0 || len(a.SHA256) != 64 || !strings.HasPrefix(a.ObjectKey, string(a.TenantID)+"/"+string(a.RunID)+"/") {
		return domain.ErrInvalid
	}
	if _, err := hex.DecodeString(a.SHA256); err != nil {
		return domain.ErrInvalid
	}
	tx, err := s.Tx(ctx, a.TenantID, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	a.State = "ready"
	tag, err := tx.Exec(ctx, `INSERT INTO artifacts(tenant_id,id,run_id,kind,object_key,sha256,byte_size,state) VALUES($1,$2,$3,$4,$5,$6,$7,'ready') ON CONFLICT(tenant_id,id) DO NOTHING`, a.TenantID, a.ID, a.RunID, a.Kind, a.ObjectKey, a.SHA256, a.ByteSize)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		var same bool
		err = tx.QueryRow(ctx, `SELECT run_id=$3 AND object_key=$4 AND sha256=$5 AND byte_size=$6 AND state='ready' FROM artifacts WHERE tenant_id=$1 AND id=$2`, a.TenantID, a.ID, a.RunID, a.ObjectKey, a.SHA256, a.ByteSize).Scan(&same)
		if err != nil {
			return err
		}
		if !same {
			return domain.ErrConflict
		}
	} else {
		body, _ := json.Marshal(a)
		if _, err = appendEvent(ctx, tx, a.TenantID, a.RunID, "artifact.ready", body); err != nil {
			return err
		}
	}
	if tag.RowsAffected() == 1 {
		if observed, ok := tx.(*observedTx); ok {
			observed.after = append(observed.after, func() { observed.telemetry.ObserveArtifact(a.Kind, a.ByteSize) })
		}
	}
	return tx.Commit(ctx)
}
func (s *Store) GetArtifact(ctx context.Context, tenant, id domain.ID) (Artifact, error) {
	ctx, cancel := dependency.Database(ctx)
	defer cancel()
	tx, err := s.Tx(ctx, tenant, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return Artifact{}, err
	}
	defer tx.Rollback(ctx)
	var a Artifact
	err = tx.QueryRow(ctx, `SELECT tenant_id,id,run_id,kind,object_key,sha256,byte_size,state,created_at FROM artifacts WHERE tenant_id=$1 AND id=$2 AND state='ready'`, tenant, id).Scan(&a.TenantID, &a.ID, &a.RunID, &a.Kind, &a.ObjectKey, &a.SHA256, &a.ByteSize, &a.State, &a.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return a, domain.ErrNotFound
	}
	if err != nil {
		return a, err
	}
	return a, tx.Commit(ctx)
}
func (s *Store) ListArtifacts(ctx context.Context, tenant, id domain.ID, after string, limit int) ([]Artifact, error) {
	ctx, cancel := dependency.Database(ctx)
	defer cancel()
	if limit < 1 || limit > 1000 {
		return nil, domain.ErrInvalid
	}
	tx, err := s.Tx(ctx, tenant, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	// Listing immutable evidence does not interpret an incompatible run state.
	var exists bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM runs WHERE tenant_id=$1 AND id=$2)`, tenant, id).Scan(&exists); err != nil {
		return nil, err
	}
	if !exists {
		return nil, domain.ErrNotFound
	}
	rows, err := tx.Query(ctx, `SELECT tenant_id,id,run_id,kind,object_key,sha256,byte_size,state,created_at FROM artifacts WHERE tenant_id=$1 AND run_id=$2 AND state='ready' AND id>$3 ORDER BY id LIMIT $4`, tenant, id, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []Artifact{}
	for rows.Next() {
		var a Artifact
		if err = rows.Scan(&a.TenantID, &a.ID, &a.RunID, &a.Kind, &a.ObjectKey, &a.SHA256, &a.ByteSize, &a.State, &a.CreatedAt); err != nil {
			return nil, err
		}
		result = append(result, a)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return result, tx.Commit(ctx)
}
