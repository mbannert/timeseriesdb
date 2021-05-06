-- Add time series to the database
--
-- Transfer time series data from a temporary table to the main table.
-- Updating the latest vintage is allowed while attempts to update an outdated
-- one is an error.
--
-- tmp_ts_updates has columns (ts_key TEXT,
--                             ts_data JSON,
--                             validity DATE,
--                             release_date TIMESTAMPTZ,
--                             access TEXT)
--
-- returns: json {"status": "", "message": "", ["offending_keys": ""]}
CREATE OR REPLACE FUNCTION timeseries.ts_insert(p_validity DATE,
                                           p_release_date TIMESTAMPTZ,
                                           p_access TEXT,
                                           p_pre_release_access TEXT)
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
    RETURN json_build_object('status', 'error',
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
  SET coverage = concat('[', ts_data->'time'->0, ',', ts_data->'time'->-1, ']')::daterange;

  -- Main insert
  INSERT INTO timeseries.timeseries_main(ts_key, validity, coverage, release_date, ts_data, access, pre_release_access)
  SELECT tmp.ts_key, COALESCE(p_validity, CURRENT_DATE), tmp.coverage,
            COALESCE(p_release_date, CURRENT_TIMESTAMP), tmp.ts_data,
            COALESCE(p_access, (SELECT role FROM timeseries.access_levels WHERE is_default)),
            p_pre_release_access
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
    access = COALESCE(p_access, timeseries.timeseries_main.access),
    pre_release_access = p_pre_release_access;

  IF array_length(v_invalid_keys, 1) > 0 THEN
    RETURN json_build_object('status', 'warning',
                             'message', 'Some keys already have a newer vintage.',
                             'offending_keys', to_json(v_invalid_keys));
  ELSE
    -- All went well
    RETURN '{"status": "ok"}'::JSON;
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
CREATE OR REPLACE FUNCTION timeseries.helper_keys_fill_read_regex(pattern TEXT)
RETURNS VOID
AS $$
BEGIN
  DELETE FROM tmp_ts_read_keys;

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
CREATE OR REPLACE FUNCTION timeseries.ts_change_access_level(p_level TEXT,
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
CREATE OR REPLACE FUNCTION timeseries.ts_read_raw(valid_on DATE DEFAULT CURRENT_DATE,
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

  RETURN QUERY
    WITH result AS (
      SELECT distinct on (rd.ts_key) rd.ts_key, mn.ts_data
      FROM tmp_ts_read_keys as rd
      JOIN timeseries.timeseries_main as mn
      USING (ts_key)
      WHERE (((NOT respect_release_date) AND
             ( pre_release_access IS NULL OR
				       pg_has_role(SESSION_USER, pre_release_access, 'usage') OR
				       pg_has_role(SESSION_USER, 'timeseries_admin', 'usage')))
		         OR
		         release_date <= CURRENT_TIMESTAMP)
      AND validity <= valid_on
      -- Use SESSION_USER because function is executed under timeseries_admin
      AND (pg_has_role(SESSION_USER, 'timeseries_admin', 'usage') OR pg_has_role(SESSION_USER, access, 'usage'))
      ORDER BY rd.ts_key, mn.validity DESC
    )
    -- JOIN against temp table again to restore original order of keys
    SELECT *
    FROM result AS res
    JOIN tmp_ts_read_keys AS tmp USING(ts_key);
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;


-- Read time series data in raw (i.e. JSON) form
--
-- see timeseries.ts_read_raw(date, boolean)
--
-- This version handles all the temp tableing on the databas by accepting an array of keys to read
-- Wherever possible, prefer this.
CREATE OR REPLACE FUNCTION timeseries.ts_read_raw(p_keys TEXT[],
                                                  p_valid_on DATE DEFAULT CURRENT_DATE,
                                                  p_respect_release_date BOOLEAN DEFAULT false)
RETURNS TABLE(ts_key TEXT, ts_data JSON)
AS $$
BEGIN
  CREATE TEMPORARY TABLE tmp_ts_read_keys
  ON COMMIT DROP
  AS (
    SELECT * FROM unnest(p_keys) AS ts_key
  );

  RETURN QUERY
  SELECT * FROM timeseries.ts_read_raw(p_valid_on::DATE, p_respect_release_date::BOOLEAN);
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;


-- Return Long Format Time Series Instead of JSON
CREATE OR REPLACE FUNCTION timeseries.ts_read_long(p_keys TEXT[],
                      p_valid_on DATE DEFAULT CURRENT_DATE,
                                          p_respect_release_date BOOLEAN DEFAULT false)
RETURNS TABLE(ts_key TEXT, date TEXT, value NUMERIC)
AS $$
BEGIN
  RETURN QUERY
  WITH json AS (
    SELECT j.ts_key, json_array_elements(ts_data->'time')::TEXT AS date,
           json_array_elements(ts_data->'value')::TEXT AS value
           FROM timeseries.ts_read_raw(p_keys, p_valid_on, p_respect_release_date) AS j
  )
  SELECT json.ts_key, json.date, CASE WHEN json.value = 'null' THEN NULL ELSE json.value::NUMERIC END
  FROM json;
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;






-- Read all vintages of a given time series
--
-- param p_key: the key to read
-- param p_respect_release_date: should the release date be respected?
--
-- This function follows the same constraints (release date, access) as ts_read_raw
CREATE OR REPLACE FUNCTION timeseries.ts_read_history_raw(p_key TEXT,
                                                          p_respect_release_date BOOLEAN DEFAULT false)
RETURNS TABLE(validity TEXT, ts_data JSON)
AS $$
BEGIN
  RETURN QUERY
  SELECT to_char(mn.validity, 'YYYYmmdd') AS validity, mn.ts_data
  FROM timeseries.timeseries_main AS mn
  WHERE mn.ts_key = p_key
  AND ((NOT p_respect_release_date) OR mn.release_date <= CURRENT_TIMESTAMP)
  AND (pg_has_role(SESSION_USER, 'timeseries_admin', 'usage') OR pg_has_role(SESSION_USER, mn.access, 'usage'))
  ORDER BY mn.validity;
END;
$$LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;


-- Completely Purge a Time Series from the database
--
-- tmp_ts_delete_keys (ts_key TEXT)
--
-- Removes all vintages, metadata, catalog entries, collection entries and dataset entries
-- (also the set if it ends up empty).
-- Use VERY SPARINGLY!
CREATE OR REPLACE FUNCTION timeseries.ts_delete()
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
CREATE OR REPLACE FUNCTION timeseries.ts_delete_edge()
RETURNS JSON
AS $$
BEGIN
  WITH ids_to_delete AS (
    SELECT DISTINCT ON (ts_key) id
    FROM timeseries.timeseries_main mn
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
CREATE OR REPLACE FUNCTION timeseries.ts_trim_history(p_older_than DATE)
RETURNS JSON
AS $$
BEGIN
  DELETE
  FROM timeseries.timeseries_main mn
  USING tmp_ts_delete_keys tmp
  WHERE mn.ts_key = tmp.ts_key
  AND mn.validity < p_older_than;

  RETURN json_build_object('status', 'ok');
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;

-- Get the last time time series were updated
--
-- Returns the created_at of the given time series
CREATE OR REPLACE FUNCTION timeseries.ts_get_last_update()
RETURNS TABLE(ts_key TEXT, updated TIMESTAMPTZ)
AS $$
BEGIN
  RETURN QUERY
  SELECT
  DISTINCT ON(mn.ts_key)
  mn.ts_key, mn.created_at AS updated
  FROM timeseries.timeseries_main AS mn
  JOIN tmp_ts_read_keys AS rd
  USING(ts_key)
  ORDER BY mn.ts_key, mn.created_at DESC;
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;

-- Vectorized version for frontends that can provide arrays
--
CREATE OR REPLACE FUNCTION timeseries.ts_get_last_update(p_keys TEXT[])
RETURNS TABLE(ts_key TEXT, updated TIMESTAMPTZ)
AS $$
BEGIN
  CREATE TEMPORARY TABLE tmp_ts_read_keys
  ON COMMIT DROP
  AS (
    SELECT * FROM unnest(p_keys) AS ts_key
  );

  RETURN QUERY
  SELECT * FROM timeseries.ts_get_last_update();
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;

CREATE OR REPLACE FUNCTION timeseries.ts_rename()
RETURNS JSON
AS $$
DECLARE
  v_not_found TEXT[];
BEGIN
  SELECT array_agg(rn.ts_key)
  FROM tmp_ts_rename AS rn
  LEFT JOIN timeseries.catalog AS cat
  USING(ts_key)
  WHERE cat.ts_key IS NULL
  INTO v_not_found;

  UPDATE timeseries.catalog AS cat
  SET ts_key = rn.ts_key_new
  FROM tmp_ts_rename AS rn
  WHERE cat.ts_key = rn.ts_key
  -- Better safe than sorry
  AND NOT rn.ts_key_new IS NULL;

  IF array_length(v_not_found, 1) > 0 THEN
    RETURN json_build_object('status', 'warning',
                             'message', 'Some keys not found in the catalog.',
                             'offending_keys', to_json(v_not_found));
  ELSE
    RETURN json_build_object('status', 'ok');
  END IF;
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;



-- Find keys given a pattern
--
-- param: pattern regular expression to find keys
--
-- returns: table(ts_key TEXT)
CREATE OR REPLACE FUNCTION timeseries.ts_find_keys(pattern TEXT)
RETURNS TABLE(ts_key TEXT)
AS $$
BEGIN
  RETURN QUERY
  SELECT cat.ts_key FROM timeseries.catalog AS cat
  WHERE cat.ts_key ~ pattern
  ORDER BY cat.ts_key;
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;

