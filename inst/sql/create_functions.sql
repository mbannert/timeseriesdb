BEGIN;
-- delete function if exists

-- don't think the search path trick works here
-- Well, it probably would, schema could be a parameter
-- CC will know a trick

-- figure out if there is a standard way of documenting psql functions
CREATE FUNCTION timeseries.dataset_exists(dataset_name TEXT)
RETURNS BOOL
AS $$
BEGIN
  RETURN EXISTS(SELECT 1 FROM timeseries.datasets WHERE set_id = dataset_name);
END;
$$ LANGUAGE PLPGSQL;


CREATE FUNCTION timeseries.create_dataset(dataset_name TEXT,
                                          dataset_md JSON DEFAULT NULL)
RETURNS TEXT
AS $$
DECLARE
  v_id TEXT;
BEGIN
  -- TODO: is this necessary?
  INSERT INTO timeseries.datasets(set_id, set_md) VALUES(dataset_name, dataset_md)
  RETURNING set_id
  INTO v_id;
  RETURN v_id;
END;
$$ LANGUAGE PLPGSQL;

-- Ask charles for schemas as params
CREATE FUNCTION timeseries.insert_from_tmp()
RETURNS JSON
AS $$
DECLARE
  v_invalid_keys JSON;
BEGIN
  SELECT json_agg(DISTINCT tmp_ts_updates.ts_key)
  INTO v_invalid_keys
  FROM tmp_ts_updates
  INNER JOIN timeseries.timeseries_main
  ON tmp_ts_updates.ts_key = timeseries.timeseries_main.ts_key
  AND tmp_ts_updates.validity <= timeseries.timeseries_main.validity;
  
  IF json_array_length(v_invalid_keys) > 0 THEN
    RETURN json_build_object('status', 'failure',
                             'reason', 'keys with invalid vintages',
                             'offending_keys', v_invalid_keys);
  END IF;
  
  -- after this insert the set_id is 'default' because we don't want a set parameter in our
  -- store functions
  INSERT INTO timeseries.catalog
  SELECT tmp_ts_updates.ts_key
  FROM tmp_ts_updates
  LEFT OUTER JOIN timeseries.catalog ON (timeseries.catalog.ts_key = tmp_ts_updates.ts_key)
  WHERE timeseries.catalog.ts_key IS NULL;

  -- Generate computed property "coverage"  
  ALTER TABLE tmp_ts_updates
  ADD COLUMN coverage DATERANGE;
  UPDATE tmp_ts_updates
  SET coverage = concat('[', ts_data->'time'->0, ',', ts_data->'time'->-1, ')')::daterange;

  -- Main insert  
  INSERT INTO timeseries.timeseries_main(ts_key, validity, coverage, release_date, ts_data, access)
  SELECT ts_key, COALESCE(validity, CURRENT_DATE), coverage, COALESCE(release_date, CURRENT_TIMESTAMP), ts_data, access
  FROM tmp_ts_updates;
  
  -- All went well
  RETURN '{"status": "ok", "reason": "the world is full of rainbows"}'::JSON;
END;
$$ LANGUAGE PLPGSQL;
COMMIT;
