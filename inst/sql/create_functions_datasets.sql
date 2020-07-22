-- Check whether a given dataset exists
--
-- returns: true if so, false if not
CREATE OR REPLACE FUNCTION timeseries.dataset_exists(dataset_name TEXT)
RETURNS BOOL
AS $$
BEGIN
  RETURN EXISTS(SELECT 1 FROM timeseries.datasets WHERE set_id = dataset_name);
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;






-- Create a new dataset
--
-- Each dataset must explicitly be registered with the database
-- before any time series can be assigned to it.
--
-- returns: name of the set
CREATE OR REPLACE FUNCTION timeseries.create_dataset(dataset_name TEXT,
                                          dataset_description TEXT DEFAULT NULL,
                                          dataset_md JSON DEFAULT NULL)
RETURNS JSON
AS $$
DECLARE
  v_id TEXT;
BEGIN
  INSERT INTO timeseries.datasets(set_id, set_description, set_md)
  VALUES(dataset_name, dataset_description, dataset_md)
  RETURNING set_id
  INTO v_id;

  RETURN json_build_object('status', 'ok', 'id', v_id);
EXCEPTION
  WHEN unique_violation THEN
    RETURN json_build_object('status', 'error', 'message', 'A dataset with that name already exists.');
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;






-- List all keys in a given dataset
--
-- param: id the name of the set
--
-- returns: table(ts_key TEXT)
CREATE OR REPLACE FUNCTION timeseries.keys_in_dataset(id TEXT)
RETURNS TABLE(ts_key TEXT)
AS $$
BEGIN
  RETURN QUERY SELECT cat.ts_key
  FROM timeseries.catalog cat
  WHERE id = set_id
  ORDER BY cat.ts_key;
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;





-- Get the dataset ts keys belong to
--
-- tmp_get_set has columns (ts_key TEXT)
--
-- returns: table(ts_key TEXT, set_id TEXT)
CREATE OR REPLACE FUNCTION timeseries.get_set_of_keys()
RETURNS TABLE(ts_key TEXT, set_id TEXT)
AS $$
BEGIN
  RETURN QUERY SELECT tmp.ts_key, cat.set_id
  FROM tmp_get_set AS tmp
  LEFT JOIN timeseries.catalog AS cat
  USING (ts_key)
  ORDER BY cat.set_id;
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;






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
CREATE OR REPLACE FUNCTION timeseries.assign_dataset(id TEXT)
RETURNS JSON
AS $$
DECLARE
  v_keys_not_in_catalog TEXT[];
BEGIN
  IF NOT EXISTS(SELECT 1 FROM timeseries.datasets WHERE set_id = id) THEN
    RETURN ('{"status": "error", "reason": "Dataset ' || id || ' does not exist!"}')::JSON;
  END IF; -- Welcome to the bronze age of programming

  UPDATE timeseries.catalog AS cat
  SET set_id = id
  FROM tmp_set_assign AS tmp -- "FROM" ;P
  WHERE cat.ts_key = tmp.ts_key;

  SELECT array_agg(tmp.ts_key)
  FROM tmp_set_assign AS tmp
  LEFT JOIN
    timeseries.catalog AS cat
  USING (ts_key)
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
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;






-- List all datasets and their description
--
-- returns: table(set_id TEXT, set_description TEXT)
CREATE OR REPLACE FUNCTION timeseries.list_datasets()
RETURNS TABLE(set_id TEXT, set_description TEXT)
AS $$
BEGIN
  RETURN QUERY SELECT ds.set_id, ds.set_description
  FROM timeseries.datasets ds
  ORDER BY ds.set_id;
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;

-- The reason this is its own function rather than ts's upsert approach is that we
-- want to make sure mistyped set ids lead to errors and not new sets with data that
-- should have been assigned to the actual (existing) set.
CREATE OR REPLACE FUNCTION timeseries.dataset_update(p_dataset_id TEXT,
                                                     p_description TEXT,
                                                     p_md JSONB,
                                                     p_update_mode TEXT)
RETURNS JSON
AS $$
BEGIN
  IF NOT (p_update_mode = 'update' OR p_update_mode = 'overwrite') THEN
    RETURN json_build_object('status', 'error', 'message', 'Update mode must be one of "update" or "overwrite".');
  END IF;

  IF NOT (SELECT * FROM timeseries.dataset_exists(p_dataset_id)) THEN
    RETURN json_build_object('status', 'error', 'message', 'Dataset ' || p_dataset_id || ' does not exist.');
  END IF;

  UPDATE timeseries.datasets
  SET
    set_description = COALESCE(p_description, set_description),
    set_md = CASE
              WHEN p_update_mode = 'update' THEN COALESCE(set_md || p_md, set_md)
              WHEN p_update_mode = 'overwrite' THEN p_md
             END
  WHERE set_id = p_dataset_id;

  RETURN json_build_object('status', 'ok');
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;


CREATE OR REPLACE FUNCTION timeseries.dataset_delete(p_dataset_name TEXT,
                                          p_confirm_dataset_name TEXT)
RETURNS JSON
AS $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM timeseries.datasets
    WHERE set_id = p_dataset_name
  ) THEN
    RETURN json_build_object('status', 'warning', 'reason', 'Dataset ' || p_dataset_name || ' does not exist.');
  ELSIF (p_dataset_name != p_confirm_dataset_name) THEN
    RETURN json_build_object('status', 'error', 'reason', 'Dataset name and confirmation do not match.');
  END IF;

  DELETE
  FROM timeseries.datasets
  WHERE set_id = p_dataset_name
  AND p_dataset_name = p_confirm_dataset_name;

  RETURN json_build_object('status', 'ok');
EXCEPTION
  WHEN triggered_action_exception THEN
    RETURN json_build_object('status', 'error', 'message', p_dataset_name || ' is the default dataset and may not be deleted.');
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;


-- Delete vintages older than some date for whole dataset
--
-- param: p_dataset The dataset to trim
-- param p_older_than The cut off point. All vintages older than that date are removed.
--
-- tmp_ts_delete_keys (ts_key TEXT)
CREATE OR REPLACE FUNCTION timeseries.dataset_trim(p_dataset TEXT,
                                        p_older_than DATE)
RETURNS JSON
AS $$
DECLARE v_out JSON;
BEGIN
  CREATE TEMPORARY TABLE tmp_ts_delete_keys
  ON COMMIT DROP
  AS (
    SELECT ts_key
    FROM timeseries.catalog
    WHERE set_id = p_dataset
  );

  SELECT * FROM timeseries.delete_ts_old_vintages(p_older_than)
  INTO v_out;

  RETURN v_out;
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;

-- Read all (accessible) series in a dataset
--
-- This function wraps read_ts_raw, filling the tmp_ts_read_keys table with
-- keys in the desired dataset.
--
-- tmp_datasets_read (set_id TEXT)
CREATE OR REPLACE FUNCTION timeseries.read_ts_dataset_raw(p_valid_on DATE DEFAULT CURRENT_DATE,
                                                          p_respect_release_date BOOLEAN DEFAULT FALSE)
RETURNS TABLE(ts_key TEXT, ts_data JSON)
AS $$
BEGIN
  -- TODO: check for existence of set here? If so: need to RAISE an error as returning
  --       JSON is not an option
  CREATE TEMPORARY TABLE tmp_ts_read_keys
  ON COMMIT DROP
  AS (
    SELECT cat.ts_key
    FROM timeseries.catalog AS cat
    JOIN tmp_datasets_read
    USING(set_id)
  );

  RETURN QUERY
  SELECT * FROM timeseries.read_ts_raw(p_valid_on, p_respect_release_date);
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;


CREATE OR REPLACE FUNCTION timeseries.read_ts_dataset_raw(p_datasets TEXT[],
                                                          p_valid_on DATE DEFAULT CURRENT_DATE,
                                                          p_respect_release_date BOOLEAN DEFAULT FALSE)
RETURNS TABLE(ts_key TEXT, ts_data JSON)
AS $$
BEGIN
  -- TODO: check for existence of set here? If so: need to RAISE an error as returning
  --       JSON is not an option
  CREATE TEMPORARY TABLE tmp_ts_read_keys
  ON COMMIT DROP
  AS (
    SELECT cat.ts_key
    FROM timeseries.catalog AS cat
    WHERE set_id = ANY(p_datasets)
  );

  RETURN QUERY
  SELECT * FROM timeseries.read_ts_raw(p_valid_on, p_respect_release_date);
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;
