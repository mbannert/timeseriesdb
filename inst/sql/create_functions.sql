BEGIN;
-- delete function if exists

-- don't think the search path trick works here
-- Well, it probably would, schema should/could be a parameter
-- CC will know a trick

-- figure out if there is a standard way of documenting psql functions
CREATE FUNCTION timeseries.dataset_exists(dataset_name TEXT)
RETURNS BOOL
AS $$
BEGIN
  IF EXISTS(SELECT 1 FROM timeseries.datasets WHERE set_id = dataset_name) THEN
    RETURN true;
  ELSE
    RETURN false;
  END IF;
END;
$$ LANGUAGE PLPGSQL;


CREATE FUNCTION timeseries.create_dataset(dataset_name TEXT,
                                          dataset_md JSON DEFAULT NULL)
RETURNS TEXT
AS $$
DECLARE
  v_id TEXT;
BEGIN
  INSERT INTO timeseries.datasets(set_id, set_md) VALUES(dataset_name, dataset_md)
  RETURNING set_id
  INTO v_id;
  RETURN v_id;
END;
$$ LANGUAGE PLPGSQL;


CREATE FUNCTION timeseries.perform_insert(/*name of temp table?*/)
RETURNS ???
AS $$
BEGIN
  -- 1) close the necessary ranges
  UPDATE timeseries.timeseries_main
  SET validity = daterange(lower(timeseries_main.validity), lower(ts_updates.validity)),
  FROM ts_updates
  WHERE ts_updates.ts_key = timeseries_main.ts_key
  AND upper_inf(timeseries_main.validity);

  -- 2) inser the new data
  INSERT INTO timeseries.timeseries_main
  SELECT * FROM ts_updates;
END;
$$ LANGUAGE PLPGSQL;
COMMIT;