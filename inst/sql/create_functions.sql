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
  INSERT INTO timeseries.datasets(set_id, set_md) VALUES(dataset_name, dataset_md)
  RETURNING set_id
  INTO v_id;
  RETURN v_id;
END;
$$ LANGUAGE PLPGSQL;

-- Ask charles for schemas as params
CREATE FUNCTION timeseries.insert_from_tmp()
RETURNS VOID -- for now... json with status message?
AS $$
BEGIN
  -- after this insert the set_id is 'default' because we don't want a set parameter in our
  -- store functions
  INSERT INTO timeseries.catalog
  SELECT tmp_ts_updates.ts_key
  FROM tmp_ts_updates
  LEFT OUTER JOIN timeseries.catalog ON (timeseries.catalog.ts_key = tmp_ts_updates.ts_key)
  WHERE timeseries.catalog.ts_key IS NULL;
  
  ALTER TABLE tmp_ts_updates
  ADD COLUMN coverage DATERANGE;
  
  UPDATE tmp_ts_updates
  SET coverage = concat('[', ts_data->'time'->0, ',', ts_data->'time'->-1, ')')::daterange;
  
  -- pro: always use DB time
  -- con: two additional updates
  -- alternative: ??? 
  -- UPDATE tmp_ts_updates
  -- SET validity = CURRENT_DATE
  -- WHERE validity IS NULL;
  -- 
  -- UPDATE tmp_ts_updates
  -- SET release_date = CURRENT_TIMESTAMP
  -- WHERE release_date IS NULL;
  -- 
  INSERT INTO timeseries.timeseries_main(ts_key, validity, coverage, release_date, ts_data, access)
  SELECT ts_key, COALESCE(validity, CURRENT_DATE), coverage, COALESCE(release_date, CURRENT_TIMESTAMP), ts_data, access
  FROM tmp_ts_updates;
  
END;
$$ LANGUAGE PLPGSQL;
COMMIT;
