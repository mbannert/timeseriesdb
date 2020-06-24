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

CREATE FUNCTION timeseries.manage_dataset()
RETURNS TRIGGER
AS $$
BEGIN
  IF TG_OP = 'DELETE' THEN
    IF (SELECT count(*)
        FROM timeseries.catalog
        WHERE set_id = OLD.set_id) = 0 THEN
      DELETE FROM timeseries.datasets
      WHERE set_id = OLD.set_id;
    END IF;
  END IF;
  RETURN NULL;
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;

CREATE TRIGGER manage_dataset
AFTER DELETE
ON timeseries.catalog
FOR EACH ROW EXECUTE PROCEDURE timeseries.manage_dataset();
