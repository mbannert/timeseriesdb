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
                                          dataset_description TEXT DEFAULT NULL,
                                          dataset_md JSON DEFAULT NULL)
RETURNS TEXT
AS $$
  INSERT INTO timeseries.datasets(set_id, set_description, set_md)
  VALUES(dataset_name, dataset_description, dataset_md)
  RETURNING set_id
$$ LANGUAGE SQL;

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

CREATE FUNCTION timeseries.create_read_tmp_regex(pattern TEXT)
RETURNS VOID
AS $$
  DROP TABLE IF EXISTS tmp_ts_read_keys;
  CREATE TEMPORARY TABLE tmp_ts_read_keys AS(
  SELECT ts_key FROM timeseries.catalog
  WHERE ts_key ~ 'ts');
$$ LANGUAGE SQL;

CREATE FUNCTION timeseries.read_ts_raw(valid_on DATE DEFAULT CURRENT_DATE, respect_release_date BOOLEAN DEFAULT false)
RETURNS TABLE(ts_key TEXT, ts_data JSON)
AS $$
BEGIN
  RETURN QUERY SELECT distinct on (rd.ts_key) rd.ts_key, mn.ts_data
    FROM tmp_ts_read_keys as rd
    JOIN timeseries.timeseries_main as mn
    ON rd.ts_key = mn.ts_key
    AND ((NOT respect_release_date) OR mn.release_date <= CURRENT_TIMESTAMP)
    AND mn.validity <= valid_on
    ORDER BY rd.ts_key, mn.validity DESC;
END;
$$ LANGUAGE PLPGSQL; -- plpgsql because plain sql would (somewhat rightly) complain that the tmp table does not exist

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

COMMIT;
