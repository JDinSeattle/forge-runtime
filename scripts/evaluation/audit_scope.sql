SELECT pg_catalog.json_build_object(
 'database', pg_catalog.current_database(),
 'schema', :'schema',
 'schema_oid', (SELECT oid::bigint FROM pg_catalog.pg_namespace WHERE nspname=:'schema'),
 'current_user', current_user, 'session_user', session_user,
 'read_only', pg_catalog.current_setting('transaction_read_only')='on',
 'select_allowed', (SELECT bool_and(pg_catalog.has_table_privilege(current_user,pg_catalog.format('%I.%I',:'schema',name),'SELECT')) FROM (VALUES ('runs'),('model_attempts'),('quota_reservations'),('artifacts'),('run_events')) AS required(name)),
 'dml_allowed', (SELECT bool_or(pg_catalog.has_table_privilege(current_user,pg_catalog.format('%I.%I',:'schema',name),'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')) FROM (VALUES ('runs'),('model_attempts'),('quota_reservations'),('artifacts'),('run_events')) AS required(name)),
 'unsafe_role', (SELECT rolsuper OR rolbypassrls OR rolcreaterole OR rolcreatedb OR pg_catalog.pg_has_role(current_user,(SELECT relowner FROM pg_catalog.pg_class WHERE oid=pg_catalog.to_regclass(pg_catalog.format('%I.runs',:'schema'))),'MEMBER') FROM pg_catalog.pg_roles WHERE rolname=current_user)
)
