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
