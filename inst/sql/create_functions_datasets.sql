CREATE FUNCTION timeseries.dataset_exists(dataset_name TEXT)
RETURNS BOOL
AS $$
BEGIN
  RETURN EXISTS(SELECT 1 FROM timeseries.datasets WHERE set_id = dataset_name);
END;
$$ LANGUAGE PLPGSQL;

CREATE FUNCTION timeseries.create_dataset(dataset_name TEXT,
                                          dataset_description TEXT DEFAULT NULL,
                                          dataset_md JSON DEFAULT NULL)
RETURNS TEXT
AS $$
  INSERT INTO timeseries.datasets(set_id, set_description, set_md)
  VALUES(dataset_name, dataset_description, dataset_md)
  RETURNING set_id
$$ LANGUAGE SQL;

CREATE FUNCTION timeseries.keys_in_dataset(id TEXT)
RETURNS TABLE(ts_key TEXT)
AS $$
BEGIN
  RETURN QUERY SELECT timeseries.catalog.ts_key
  FROM timeseries.catalog
  WHERE id = set_id;
END;
$$ LANGUAGE PLPGSQL;

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

CREATE FUNCTION timeseries.assign_dataset(id TEXT)
RETURNS JSON
AS $$
DECLARE
  v_keys_not_in_catalog TEXT[];
BEGIN
  IF NOT EXISTS(SELECT 1 FROM timeseries.datasets WHERE set_id = id) THEN
    RETURN ('{"status": "failure", "reason": "Dataset ' || id || ' does not exist!"}')::JSON;
  END IF; -- Welcome to the stone age of programming

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
    RETURN ('{"status": "warning",'
    '"reason": "Some keys are not in catalog!",'
    '"offending_keys": ["' || array_to_string(v_keys_not_in_catalog, '", "') || '"]}')::JSON;
  ELSE
    RETURN '{"status": "ok"}'::JSON;
  END IF;
END;
$$ LANGUAGE PLPGSQL;
