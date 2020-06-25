CREATE FUNCTION timeseries.prevent_delete_default_dataset()
RETURNS TRIGGER AS
$$
BEGIN
  IF OLD.set_id = 'default' THEN
    RETURN NULL;
  END IF;

  RETURN OLD;
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;

CREATE TRIGGER no_delete_default_dataset
BEFORE DELETE
ON timeseries.datasets
FOR EACH ROW
EXECUTE PROCEDURE timeseries.prevent_delete_default_dataset();

CREATE FUNCTION timeseries.ensure_access_level_is_role()
RETURNS TRIGGER AS
$$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_catalog.pg_roles
    WHERE rolname = NEW.role
  ) THEN
    RAISE EXCEPTION 'Role % does not exist so it can not be an access level.', NEW.role;
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;

CREATE TRIGGER access_level_role_check
BEFORE INSERT OR UPDATE
ON timeseries.access_levels
FOR EACH ROW
EXECUTE PROCEDURE timeseries.ensure_access_level_is_role();
