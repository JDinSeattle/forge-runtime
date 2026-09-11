package persistence

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/jackc/pgx/v5"
)

// acceptedInput is the versioned admission record stored in runs.input_snapshot.
// Source bytes are identified by the registered immutable base; this is not a
// mutable workspace snapshot or a newly published artifact object.
type acceptedInput struct {
	SchemaVersion int       `json:"schema_version"`
	ProjectID     domain.ID `json:"project_id"`
	Task          string    `json:"task"`
	BaseCommit    string    `json:"base_commit"`
	SourceID      string    `json:"source_id"`
	ProfileID     string    `json:"profile_id"`
}

func snapshotInput(ctx context.Context, tx pgx.Tx, req SubmitRequest) (string, error) {
	input := acceptedInput{SchemaVersion: 1, ProjectID: req.ProjectID, Task: req.Task, BaseCommit: req.BaseCommit}
	err := tx.QueryRow(ctx, `SELECT repo_source,repo_profile->>'id' FROM projects
	 WHERE tenant_id=$1 AND id=$2 FOR SHARE`, req.TenantID, req.ProjectID).Scan(&input.SourceID, &input.ProfileID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", domain.ErrNotFound
	}
	if err != nil {
		return "", err
	}
	body, err := json.Marshal(input)
	return string(body), err
}
