BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL forge.tenant_id = :'tenant';
SELECT json_build_object(
 'scope', (__AUDIT_SCOPE__),
 'as_of', clock_timestamp(),
 'run', (SELECT row_to_json(r) FROM (SELECT tenant_id,id,project_id,state,snapshot,config_snapshot,created_at FROM :"schema".runs WHERE tenant_id=:'tenant' AND id=:'run') r),
 'finished_at', (SELECT max(created_at) FROM :"schema".run_events WHERE tenant_id=:'tenant' AND run_id=:'run' AND type='run.finished'),
 'attempts', COALESCE((SELECT json_agg(row_to_json(a) ORDER BY a.step_seq,a.attempt) FROM (
   SELECT m.attempt_id,m.step_seq,m.attempt,m.provider,m.model_id,m.status,m.raw_ref,m.usage,m.request_id,m.price_version,m.started_at,m.deadline,m.error_code,m.pricing,m.failure_policy,o.created_at AS response_persisted_at
   FROM :"schema".model_attempts m LEFT JOIN :"schema".artifacts o ON o.tenant_id=m.tenant_id AND o.id=m.raw_ref
   WHERE m.tenant_id=:'tenant' AND m.run_id=:'run'
 ) a),'[]'::json),
 'reservations', COALESCE((SELECT json_agg(row_to_json(q) ORDER BY q.id) FROM (
   SELECT id,credential_group,tokens,microusd,status,request_slot_released,request_deadline,dispatched_at,actual_tokens,actual_microusd,settlement_kind,outcome,settled_at
   FROM :"schema".quota_reservations WHERE tenant_id=:'tenant' AND run_id=:'run'
 ) q),'[]'::json)
);
ROLLBACK;
