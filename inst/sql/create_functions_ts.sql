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
CREATE OR REPLACE FUNCTION timeseries.insert_from_tmp(p_validity DATE,
                                           p_release_date TIMESTAMPTZ,
                                           p_access TEXT)
RETURNS JSON
AS $$
DECLARE
  v_invalid_keys TEXT[];
BEGIN
  IF p_access IS NULL
  AND NOT EXISTS (
    SELECT 1
    FROM timeseries.access_levels
    WHERE is_default
  ) THEN
    RETURN json_build_object('status', 'failure',
                             'message', 'No access level supplied and no default configured on the database.');
  END IF;

  WITH inv_keys AS (
    SELECT DISTINCT tmp.ts_key
    FROM tmp_ts_updates AS tmp
    INNER JOIN timeseries.timeseries_main AS main
    USING (ts_key)
    WHERE p_validity < main.validity
  ), del_keys AS(
    DELETE FROM tmp_ts_updates
    USING inv_keys
    WHERE tmp_ts_updates.ts_key = inv_keys.ts_key
  )
  SELECT array_agg(ts_key) FROM inv_keys
  INTO v_invalid_keys;

  -- after this insert the set_id is 'default' because we don't want a set parameter in our
  -- store functions
  INSERT INTO timeseries.catalog
  SELECT tmp_ts_updates.ts_key
  FROM tmp_ts_updates
  LEFT OUTER JOIN timeseries.catalog
  USING (ts_key)
  WHERE timeseries.catalog.ts_key IS NULL;

  -- Generate computed property "coverage"
  UPDATE tmp_ts_updates
  SET coverage = concat('[', ts_data->'time'->0, ',', ts_data->'time'->-1, ')')::daterange;

  -- Main insert
  INSERT INTO timeseries.timeseries_main(ts_key, validity, coverage, release_date, ts_data, access)
  SELECT tmp.ts_key, COALESCE(p_validity, CURRENT_DATE), tmp.coverage,
            COALESCE(p_release_date, CURRENT_TIMESTAMP), tmp.ts_data,
            COALESCE(p_access, (SELECT role FROM timeseries.access_levels WHERE is_default))
  FROM tmp_ts_updates AS tmp
  LEFT JOIN timeseries.timeseries_main AS main
  ON tmp.ts_key = main.ts_key
  AND p_validity = main.validity
  ON CONFLICT (ts_key, validity) DO UPDATE
  SET
    coverage = EXCLUDED.coverage,
    release_date = EXCLUDED.release_date,
    created_by = EXCLUDED.created_by,
    created_at = EXCLUDED.created_at,
    ts_data = EXCLUDED.ts_data,
    access = COALESCE(p_access, timeseries.timeseries_main.access);

  IF array_length(v_invalid_keys, 1) > 0 THEN
    RETURN json_build_object('status', 'warning',
                             'message', 'Some keys already have a newer vintage.',
                             'offending_keys', to_json(v_invalid_keys));
  ELSE
    -- All went well
    RETURN '{"status": "ok", "reason": "the world is full of rainbows"}'::JSON;
  END IF;
END;
$$ LANGUAGE PLPGSQL
-- Read this tho: https://www.cybertec-postgresql.com/en/abusing-security-definer-functions/
SECURITY DEFINER
SET search_path = timeseries, pg_temp;






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
CREATE OR REPLACE FUNCTION timeseries.fill_read_tmp_regex(pattern TEXT)
RETURNS VOID
AS $$
BEGIN
  INSERT INTO tmp_ts_read_keys
  SELECT ts_key FROM timeseries.catalog
  WHERE ts_key ~ pattern;
END;
$$
LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;


-- Change the access level for given time series
--
-- Optionally specify exact vintage to change
--
-- tmp_ts_access_keys (ts_key TEXT)
--
-- param:  p_level TEXT, the access level to set the series to
-- param:  p_validity DATE, the exact vintage for which to change the access level
--
-- By default all vintages are set to the specified level
CREATE OR REPLACE FUNCTION timeseries.change_access_level(p_level TEXT,
                                               p_validity DATE DEFAULT NULL)
RETURNS JSON
AS $$
BEGIN
-- TODO: check for missing keys?
--       is it time for a helper for that?? table names change tho
  IF NOT EXISTS (
    SELECT 1
    FROM timeseries.access_levels
    WHERE role = p_level
  ) THEN
    RETURN json_build_object('status', 'error', 'message', 'Role ' || p_level || ' is not a valid access level.');
  END IF;

  UPDATE timeseries.timeseries_main mn
  SET access = p_level
  FROM tmp_ts_access_keys tmp
  WHERE tmp.ts_key = mn.ts_key
  AND (p_validity IS NULL OR mn.validity = p_validity);

  RETURN json_build_object('status', 'ok');
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;



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
CREATE OR REPLACE FUNCTION timeseries.read_ts_raw(valid_on DATE DEFAULT CURRENT_DATE,
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
    USING (ts_key)
    WHERE ((NOT respect_release_date) OR release_date <= CURRENT_TIMESTAMP)
    AND validity <= valid_on
    -- Use SESSION_USER because function is executed under timeseries_admin
    AND (pg_has_role(SESSION_USER, 'timeseries_admin', 'usage') OR pg_has_role(SESSION_USER, access, 'usage'))
    ORDER BY rd.ts_key, mn.validity DESC;
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;

-- Completely Purge a Time Series from the database
--
-- tmp_ts_delete_keys (ts_key TEXT)
--
-- Removes all vintages, metadata, catalog entries, collection entries and dataset entries
-- (also the set if it ends up empty).
-- Use VERY SPARINGLY!
CREATE OR REPLACE FUNCTION timeseries.delete_ts()
RETURNS JSON
AS $$
BEGIN
  DELETE
  FROM timeseries.catalog cat
  USING tmp_ts_delete_keys del
  WHERE del.ts_key = cat.ts_key;

  RETURN json_build_object('status', 'ok');
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;

-- Delete the latest vintage from given time series
--
-- tmp_ts_delete_keys (ts_key TEXT)
CREATE OR REPLACE FUNCTION timeseries.delete_ts_edge()
RETURNS JSON
AS $$
BEGIN
  WITH ids_to_delete AS (
    SELECT DISTINCT ON (ts_key) id
    FROM tsdb_test.timeseries_main mn
    JOIN tmp_ts_delete_keys del
    USING(ts_key)
    ORDER BY ts_key, validity DESC
  )
  DELETE
  FROM timeseries.timeseries_main mn
  USING ids_to_delete ids
  WHERE mn.id = ids.id;

  RETURN json_build_object('status', 'ok');
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;


-- Delete vintages older than some date
--
-- param p_older_than The cut off point. All vintages older than that date are removed.
--
-- tmp_ts_delete_keys (ts_key TEXT)
CREATE OR REPLACE FUNCTION timeseries.delete_ts_old_vintages(p_older_than DATE)
RETURNS JSON
AS $$
BEGIN
  DELETE
  FROM timeseries.timeseries_main mn
  USING tmp_ts_delete_keys tmp
  WHERE mn.ts_key = tmp.ts_key
  AND mn.validity <= p_older_than;

  RETURN json_build_object('status', 'ok');
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;
