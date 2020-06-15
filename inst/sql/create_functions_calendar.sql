CREATE FUNCTION timeseries.create_release(p_id TEXT, p_title TEXT, p_note TEXT,
                                          p_date TIMESTAMPTZ, p_year INTEGER,
                                          p_period INTEGER, p_frequency INTEGER)
RETURNS JSON
AS $$
DECLARE
  v_nonexistent_datasets TEXT[];
BEGIN
  SELECT array_agg(tmp.set_id)
  FROM tmp_release_insert AS tmp
  LEFT JOIN
    timeseries.datasets AS dat
  USING (set_id)
  WHERE dat.set_id IS NULL
  INTO v_nonexistent_datasets;

  IF array_length(v_nonexistent_datasets, 1) != 0 THEN
  -- TODO: use json_build_object for consistency
    RETURN json_build_object('status', 'failure',
                             'reason', 'Some datasets do not exist.',
                             'missing_datasets', v_nonexistent_datasets);
  END IF;

  INSERT INTO timeseries.release_calendar(id, title, note,
                                          release_date, reference_year,
                                          reference_period, reference_frequency)
  VALUES (p_id, p_title, p_note, p_date, p_year, p_period, p_frequency);

  INSERT INTO timeseries.release_dataset(release_id, set_id)
  SELECT p_id, set_id FROM tmp_release_insert;

  RETURN '{"status": "ok"}'::JSON;
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;

CREATE FUNCTION timeseries.update_release(p_id TEXT, p_title TEXT, p_note TEXT,
                                          p_date TIMESTAMPTZ, p_year INTEGER,
                                          p_period INTEGER, p_frequency INTEGER)
RETURNS TEXT
AS $$
BEGIN
  UPDATE timeseries.release_calendar
  AS cal
  SET
    title = COALESCE(p_title, cal.title),
    note  = COALESCE(p_note, cal.note),
    release_date = COALESCE(p_date, cal.release_date),
    reference_year = COALESCE(p_year, cal.reference_year),
    reference_period = COALESCE(p_period, cal.reference_period),
    reference_frequency = COALESCE(p_frequency, cal.reference_frequency)
  RETURNING id;
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;
