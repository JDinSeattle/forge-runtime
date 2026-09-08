-- +goose Up
ALTER TABLE model_attempts ADD COLUMN pricing jsonb;
ALTER TABLE model_attempts ADD CONSTRAINT attempt_pricing_object CHECK (pricing IS NULL OR jsonb_typeof(pricing)='object');
ALTER TABLE model_attempts ADD COLUMN failure_policy jsonb;
ALTER TABLE model_attempts ADD CONSTRAINT attempt_failure_policy_object CHECK (failure_policy IS NULL OR jsonb_typeof(failure_policy)='object');

-- An established pricing snapshot cannot be rewritten, including by an older
-- application version. Legacy NULL records require explicit reconciliation;
-- their historical rates must not be inferred from current configuration.
-- +goose StatementBegin
CREATE FUNCTION preserve_attempt_pricing() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF OLD.pricing IS NOT NULL AND NEW.pricing IS DISTINCT FROM OLD.pricing THEN
    RAISE EXCEPTION 'model attempt pricing is immutable';
  END IF;
  IF OLD.failure_policy IS NOT NULL AND NEW.failure_policy IS DISTINCT FROM OLD.failure_policy THEN
    RAISE EXCEPTION 'model attempt failure policy is immutable';
  END IF;
  RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER attempt_pricing_immutable BEFORE UPDATE ON model_attempts
    FOR EACH ROW EXECUTE FUNCTION preserve_attempt_pricing();

-- +goose Down
DROP TRIGGER attempt_pricing_immutable ON model_attempts;
DROP FUNCTION preserve_attempt_pricing();
ALTER TABLE model_attempts DROP CONSTRAINT attempt_failure_policy_object, DROP COLUMN failure_policy,
    DROP CONSTRAINT attempt_pricing_object, DROP COLUMN pricing;
