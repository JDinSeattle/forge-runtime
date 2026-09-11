-- +goose Up
-- FOR SHARE needs UPDATE privilege even though it does not change data. Keep
-- runtime roles read-only on memberships and expose only this fixed row-lock
-- operation. Resolve the table's trusted schema during the owner migration;
-- neither caller search_path nor a temporary table can redirect the read.
-- +goose StatementBegin
DO $migration$
DECLARE
    target_schema name := current_schema();
BEGIN
    EXECUTE format($definition$
        CREATE FUNCTION %1$I.lock_current_membership(p_tenant text, p_principal text)
        RETURNS text
        LANGUAGE plpgsql VOLATILE SECURITY DEFINER
        SET search_path = pg_catalog, pg_temp
        AS $function$
        DECLARE
            member_role text;
        BEGIN
            IF p_tenant IS NULL OR p_principal IS NULL OR
               p_tenant IS DISTINCT FROM pg_catalog.current_setting('forge.tenant_id', true) THEN
                RAISE EXCEPTION 'membership authority scope mismatch' USING ERRCODE = '42501';
            END IF;
            SELECT m.role INTO member_role FROM %1$I.memberships AS m
                WHERE m.tenant_id = p_tenant AND m.principal_id = p_principal
                FOR SHARE;
            RETURN member_role;
        END
        $function$
    $definition$, target_schema);
    EXECUTE format('REVOKE ALL ON FUNCTION %I.lock_current_membership(text,text) FROM PUBLIC', target_schema);
END
$migration$;
-- +goose StatementEnd

-- +goose Down
DROP FUNCTION lock_current_membership(text,text);
