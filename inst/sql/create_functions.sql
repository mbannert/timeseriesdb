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

-- Ask charles for schemas as params
CREATE FUNCTION timeseries.insert_from_tmp()
RETURNS ???
AS $$
BEGIN
  -- after this insert the set_id is 'default' because we don't want a set parameter in our
  -- store functions
  INSERT INTO timeseries.catalog
  SELECT tmp_ts_updates.ts_key,
  FROM tmp_ts_updates
  LEFT OUTER JOIN timeseries.catalog ON (timeseries.catalog.ts_key = tmp_ts_updates.ts_key)
  WHERE timeseries.catalog.ts_key IS NULL;
  
  -- TODO: Coverage needs to computed
  -- either on DB level or in R, in R this would be a matter of 
  /* R
  convert_index_datestr(range(index(series)))
  SQL 
  could run an update on the temp table and
  add a coverage column.. 
  then do some json operation and populate it. 
  
  */
  INSERT INTO timeseries.timeseries_main
  SELECT ts_key, validity, coverage, release_date, created_by, created_at, ts_data
  FROM tmp_ts_updates;
  
  
  
  
  
END;
$$ LANGUAGE PLPGSQL;
COMMIT;
