CREATE OR REPLACE FUNCTION timeseries.get_version()
RETURNS TABLE (version TEXT)
AS $$
BEGIN
  RETURN QUERY
  SELECT timeseriesdb_info.version FROM timeseries.timeseriesdb_info;
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;
