-- List all registered levels
--
-- returns: table(set_id TEXT, set_description TEXT)
CREATE OR REPLACE FUNCTION timeseries.list_access_levels()
RETURNS TABLE(role TEXT,
              description TEXT,
              is_default BOOLEAN)
AS $$
  BEGIN
  RETURN QUERY SELECT levels.role,
                      levels.description,
                      levels.is_default
  FROM timeseries.access_levels as levels
  ORDER BY levels.role;

END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;



-- Remove access_levels
--
-- role: role to remove
--
-- returns: json {"status": "", "message": "", ["id"]: ""}
CREATE OR REPLACE FUNCTION timeseries.access_levels_delete(access_level TEXT)
RETURNS JSON
AS $$
DECLARE
  error_no_delete_default TEXT;
BEGIN
  IF NOT EXISTS (SELECT 1 FROM timeseries.access_levels
    WHERE role = access_level) THEN
    RETURN json_build_object('status', 'warning',
                         'message', 'access level does not exist.');

  -- TODO: is this faster with limit 1?
  ELSIF EXISTS (SELECT 1 FROM timeseries.timeseries_main
    WHERE access = access_level) THEN
    RETURN json_build_object('status', 'error',
                         'message', 'access level '||access_level||' is still in use in timeseries_main');

  ELSE
    DELETE FROM timeseries.access_levels
    WHERE role = access_level;
    RETURN json_build_object('status', 'ok');
  END IF;

EXCEPTION
  WHEN triggered_action_exception THEN
    GET STACKED DIAGNOSTICS error_no_delete_default = MESSAGE_TEXT;
    RETURN json_build_object('status', 'error',
                         'message', error_no_delete_default);

END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;


-- Add access_levels
--
-- role: role to add
-- role: role to add
-- returns: json {"status": "", "message": "", ["id"]: ""}
CREATE OR REPLACE FUNCTION timeseries.access_levels_insert(access_level_name TEXT,
                                                access_level_description TEXT DEFAULT NULL,
                                                access_level_default BOOLEAN DEFAULT NULL)
RETURNS JSON
AS $$
DECLARE
  error_no_role TEXT;
BEGIN
  IF NOT EXISTS (SELECT 1 FROM timeseries.access_levels
    WHERE role = access_level_name) THEN
    INSERT INTO timeseries.access_levels(role, description)
      VALUES(access_level_name, access_level_description);
    IF access_level_default THEN
      PERFORM timeseries.set_access_level_default(access_level_name);
    END IF;
    RETURN json_build_object('status', 'ok');


  ELSE
    RETURN json_build_object('status', 'warning',
                         'message', 'access level '||access_level_name||' already exists');

  END IF;

EXCEPTION
  WHEN triggered_action_exception THEN
    GET STACKED DIAGNOSTICS error_no_role = MESSAGE_TEXT;
    RETURN json_build_object('status', 'error',
                         'message', error_no_role);

END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;





-- Changing default access level
--
-- role: role to add
--
-- returns: json {"status": "", "message": "", ["id"]: ""}
CREATE OR REPLACE FUNCTION timeseries.set_access_level_default(access_level_name TEXT)
RETURNS JSON
AS $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM timeseries.access_levels
    WHERE role = access_level_name) THEN
    RETURN json_build_object('status', 'error',
                         'message', 'access level '||access_level_name||' does not exists ');
  END IF;

  UPDATE timeseries.access_levels
    SET is_default = NULL WHERE is_default;

  UPDATE timeseries.access_levels
    SET is_default = TRUE WHERE role = access_level_name;

  RETURN json_build_object('status', 'ok');

END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;

-- Change the access level for given time series
--
-- Optionally specify exact vintage to change
--
-- This is just a wrapper for change_access_level that applies to a whole dataset
-- at a time.
--
-- param:  p_dataset TEXT, the dataset to set the access level for
-- param:  p_level TEXT, the access level to set the series to
-- param:  p_validity DATE, the exact vintage for which to change the access level
--
-- By default all vintages are set to the specified level
CREATE FUNCTION timeseries.change_access_level_dataset(p_dataset TEXT,
                                                       p_level TEXT,
                                                       p_validity DATE DEFAULT NULL)
RETURNS JSON
AS $$
DECLARE
  v_out JSON;
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM timeseries.datasets
    WHERE set_id = p_dataset
  ) THEN
    RETURN json_build_object('status', 'warning', 'message', 'Dataset ' || p_dataset || ' does not exist.');
  END IF;

  CREATE TEMPORARY TABLE tmp_ts_access_keys
  ON COMMIT DROP
  AS (
    SELECT ts_key
    FROM timeseries.catalog
    WHERE set_id = p_dataset
  );

  SELECT *
  FROM timeseries.change_access_level(p_level, p_validity)
  INTO v_out;

  RETURN v_out;
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;

