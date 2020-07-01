-- List all registered levels
--
-- returns: table(set_id TEXT, set_description TEXT)
CREATE FUNCTION timeseries.list_access_levels()
RETURNS TABLE(role TEXT,
              description TEXT,
              is_default BOOLEAN)
AS $$
  BEGIN
  RETURN QUERY SELECT timeseries.access_levels.role,
                      timeseries.access_levels.description,
                      timeseries.access_levels.is_default
FROM timeseries.access_levels;
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;



-- Remove access_levels
--
-- role: role to remove
--
-- returns: json {"status": "", "message": "", ["id"]: ""}
CREATE FUNCTION timeseries.access_levels_delete(role_name TEXT)
RETURNS JSON
AS $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM timeseries.access_levels
    WHERE role = role_name) THEN
    RETURN json_build_object('status', 'warning',
                         'message', 'role does not exist.');

  ELSIF EXISTS (SELECT 1 FROM timeseries.timeseries_main
    WHERE access = role_name) THEN
    RETURN json_build_object('status', 'error',
                         'reason', 'role is still in use in timeseries_main');

  ELSE
    DELETE FROM timeseries.access_levels
    WHERE role = role_name;
    RETURN json_build_object('status', 'ok',
                         'message', 'role successfully deleted',
                         'role', role_name);
  END IF;
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;


-- Add access_levels
--
-- role: role to add
--
-- returns: json {"status": "", "message": "", ["id"]: ""}
CREATE FUNCTION timeseries.access_levels_insert(role_name TEXT,
                                                role_description TEXT,
                                                role_default BOOLEAN)
RETURNS JSON
AS $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM timeseries.access_levels
      WHERE role = role_name) THEN
    INSERT INTO timeseries.access_levels(role, description)
      VALUES(role_name, role_description, role_default);

  ELSIF EXISTS (SELECT 1 FROM timeseries.timeseries_main
    WHERE access = role_name) THEN
    RETURN json_build_object('status', 'error',
                         'reason', 'role already exists');

  END IF;

EXCEPTION
  WHEN triggered_action_exception THEN
    RETURN json_build_object('status', 'error',
                         'message', 'it can not be an access level',
                         'role', role_name);

END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;
