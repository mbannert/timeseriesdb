-- Check whether a given dataset exists
--
-- returns: true if so, false if not
CREATE FUNCTION timeseries.dataset_exists(dataset_name TEXT)
RETURNS BOOL
AS $$
BEGIN
  RETURN EXISTS(SELECT 1 FROM timeseries.datasets WHERE set_id = dataset_name);
END;
$$ LANGUAGE PLPGSQL;






-- Create a new dataset
--
-- Each dataset must explicitly be registered with the database
-- before any time series can be assigned to it.
--
-- returns: name of the set
-- TODO: Check for conflict and change return type to json
CREATE FUNCTION timeseries.create_dataset(dataset_name TEXT,
                                          dataset_description TEXT DEFAULT NULL,
                                          dataset_md JSON DEFAULT NULL)
RETURNS TEXT
AS $$
  INSERT INTO timeseries.datasets(set_id, set_description, set_md)
  VALUES(dataset_name, dataset_description, dataset_md)
  RETURNING set_id
$$ LANGUAGE SQL;






-- List all keys in a given dataset
--
-- param: id the name of the set
--
-- returns: table(ts_key TEXT)
CREATE FUNCTION timeseries.keys_in_dataset(id TEXT)
RETURNS TABLE(ts_key TEXT)
AS $$
BEGIN
  RETURN QUERY SELECT timeseries.catalog.ts_key
  FROM timeseries.catalog
  WHERE id = set_id;
END;
$$ LANGUAGE PLPGSQL;





-- Get the dataset ts keys belong to
--
-- tmp_get_set has columns (ts_key TEXT)
--
-- returns: table(ts_key TEXT, set_id TEXT)
CREATE FUNCTION timeseries.get_set_of_keys()
RETURNS TABLE(ts_key TEXT, set_id TEXT)
AS $$
BEGIN
  RETURN QUERY SELECT tmp.ts_key, cat.set_id
  FROM tmp_get_set AS tmp
  LEFT JOIN timeseries.catalog AS cat
  ON cat.ts_key = tmp.ts_key;
END;
$$ LANGUAGE PLPGSQL;






-- Assign ts keys to a dataset
--
-- When time series are added for the first time they are initially member of
-- the 'default' dataset.
-- Use this function to change that or to move keys to a different set at a later
-- point.
--
-- tmp_set_assign has columns (ts_key TEXT)
--
-- returns: json {"status": "", "message": "", "offending_keys": [""]}
CREATE FUNCTION timeseries.assign_dataset(id TEXT)
RETURNS JSON
AS $$
DECLARE
  v_keys_not_in_catalog TEXT[];
BEGIN
  IF NOT EXISTS(SELECT 1 FROM timeseries.datasets WHERE set_id = id) THEN
    -- TODO: this should be status "error" and throw in the corresponding R function
    RETURN ('{"status": "failure", "reason": "Dataset ' || id || ' does not exist!"}')::JSON;
  END IF; -- Welcome to the bronze age of programming

  UPDATE timeseries.catalog AS cat
  SET set_id = id
  FROM tmp_set_assign AS tmp -- "FROM" ;P
  WHERE cat.ts_key = tmp.ts_key;

  SELECT array_agg(tmp.ts_key)
  FROM tmp_set_assign AS tmp
  LEFT JOIN
    timeseries.catalog AS cat
  ON tmp.ts_key = cat.ts_key
  WHERE cat.ts_key IS NULL
  INTO v_keys_not_in_catalog;

  IF array_length(v_keys_not_in_catalog, 1) != 0 THEN
  -- TODO: use json_build_object for consistency
    RETURN ('{"status": "warning",'
    '"reason": "Some keys are not in catalog!",'
    '"offending_keys": ["' || array_to_string(v_keys_not_in_catalog, '", "') || '"]}')::JSON;
  ELSE
    RETURN '{"status": "ok"}'::JSON;
  END IF;
END;
$$ LANGUAGE PLPGSQL;