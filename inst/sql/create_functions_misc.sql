CREATE OR REPLACE FUNCTION timeseries.get_version()
RETURNS TABLE (version TEXT)
AS $$
BEGIN
  RETURN QUERY
  SELECT version.version FROM timeseries.version;
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;
