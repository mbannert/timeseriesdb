-- Add time series to the database
--
-- Transfer time series data from a temporary table to the main table.
-- Updating the latest vintage is allowed while attempts to update an outdated
-- one is an error.
-- TODO: make it a warning
--
-- tmp_ts_updates has columns (ts_key TEXT,
--                             ts_data JSON,
--                             validity DATE,
--                             release_date TIMESTAMPTZ,
--                             access TEXT)
--
-- returns: json {"status": "", "message": "", ["offending_keys": ""]}
-- TODO: validity, release_date, access could be params for this function
--       -> saves storing 10s of 1000s of copies into tmp table
CREATE FUNCTION timeseries.insert_from_tmp()
RETURNS JSON
AS $$
DECLARE
  v_invalid_keys TEXT[];
BEGIN
  SELECT array_agg(DISTINCT tmp.ts_key)
  INTO v_invalid_keys
  FROM tmp_ts_updates AS tmp
  INNER JOIN timeseries.timeseries_main AS main
  ON tmp.ts_key = main.ts_key
  AND tmp.validity < main.validity;

  -- IMPORTANT!!!
  -- When converting this to a warning, make sure to delete
  -- invalid keys from update table, elsewise updating the past is possible!
  IF array_length(v_invalid_keys, 1) > 0 THEN
    RETURN json_build_object('status', 'failure',
                             'reason', 'keys with invalid vintages',
                             'offending_keys', to_json(v_invalid_keys));
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
  SELECT tmp.ts_key, COALESCE(tmp.validity, CURRENT_DATE), tmp.coverage, COALESCE(tmp.release_date, CURRENT_TIMESTAMP), tmp.ts_data, tmp.access
  FROM tmp_ts_updates AS tmp
  LEFT JOIN timeseries.timeseries_main AS main
  ON tmp.ts_key = main.ts_key
  AND tmp.validity = main.validity
  ON CONFLICT (ts_key, validity) DO UPDATE
  SET
    coverage = EXCLUDED.coverage,
    release_date = EXCLUDED.release_date,
    created_by = EXCLUDED.created_by,
    created_at = EXCLUDED.created_at,
    ts_data = EXCLUDED.ts_data;

  -- All went well
  RETURN '{"status": "ok", "reason": "the world is full of rainbows"}'::JSON;
END;
$$ LANGUAGE PLPGSQL
-- Read this tho: https://www.cybertec-postgresql.com/en/abusing-security-definer-functions/
SECURITY DEFINER;






-- Populate a temporary table with ts_keys via regex
--
-- This is a helper function to simplify populating tmp_ts_read_keys
-- via regex.
-- Without it, calling code would need to first read matching keys in one query
-- and then create and populate the table in another.
--
-- param: pattern regular expression to find keys
--
-- returns: json {"status": "", "message": "", ["removed_collection"]: ""}
CREATE FUNCTION timeseries.create_read_tmp_regex(pattern TEXT)
RETURNS VOID
AS $$
  DROP TABLE IF EXISTS tmp_ts_read_keys;
  CREATE TEMPORARY TABLE tmp_ts_read_keys AS(
  SELECT ts_key FROM timeseries.catalog
  WHERE ts_key ~ pattern);
$$ LANGUAGE SQL;






-- Read time series data in raw (i.e. JSON) form
--
--
-- tmp_ts_read_keys has columns (ts_key TEXT)
--
-- param: valid_on Date of the desired vintage
-- param: respect_release_date Should time series not yet released at the current
--        time be held back?
--
-- returns: TABLE(ts_key TEXT, ts_data JSON)
CREATE FUNCTION timeseries.read_ts_raw(valid_on DATE DEFAULT CURRENT_DATE,
                                       respect_release_date BOOLEAN DEFAULT false)
RETURNS TABLE(ts_key TEXT, ts_data JSON)
AS $$
BEGIN
  -- The default only works if no value is passed.
  -- Some calling code uses explicit nulls so we need to cover that.
  IF valid_on IS NULL THEN
    valid_on := CURRENT_DATE;
  END IF;

  IF respect_release_date IS NULL THEN
    respect_release_date := false;
  END IF;

  RETURN QUERY SELECT distinct on (rd.ts_key) rd.ts_key, mn.ts_data
    FROM tmp_ts_read_keys as rd
    JOIN timeseries.timeseries_main as mn
    ON rd.ts_key = mn.ts_key
    AND ((NOT respect_release_date) OR mn.release_date <= CURRENT_TIMESTAMP)
    AND mn.validity <= valid_on
    ORDER BY rd.ts_key, mn.validity DESC;
END;
$$ LANGUAGE PLPGSQL;
