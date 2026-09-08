-- name: PageRunEvents :many
-- Authorization, repeatable-read snapshot and future-cursor checks live in
-- Store.Events. This query must run on that tenant-scoped transaction.
SELECT run_id, seq, type, schema_version, payload, created_at
FROM run_events
WHERE tenant_id = sqlc.arg(tenant_id)
  AND run_id = sqlc.arg(run_id)
  AND seq > sqlc.arg(after_seq)
ORDER BY seq
LIMIT sqlc.arg(page_limit);
