CREATE FUNCTION ts_data(tkey varchar) RETURNS TABLE(date date, value varchar)
AS $$
  SELECT ((each(ts_data)).key)::date,
         ((each(ts_data)).value)::varchar
         FROM timeseries_main
         WHERE ts_key = tkey;
$$ LANGUAGE sql;