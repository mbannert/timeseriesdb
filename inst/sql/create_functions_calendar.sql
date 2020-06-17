CREATE FUNCTION timeseries.create_release(p_id TEXT, p_title TEXT, p_note TEXT,
                                          p_date TIMESTAMPTZ, p_year INTEGER,
                                          p_period INTEGER, p_frequency INTEGER)
RETURNS JSON
AS $$
DECLARE
  v_nonexistent_datasets TEXT[];
BEGIN
  -- TODO: As we're using JSON after all, set up a "set already exists" case
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
                                          p_period INTEGER, p_frequency INTEGER,
                                          p_sets_change BOOLEAN)
RETURNS TEXT
AS $$
DECLARE
  v_nonexistent_datasets TEXT[];
BEGIN
  IF p_sets_change THEN
    SELECT array_agg(tmp.set_id)
    FROM tmp_release_update AS tmp
    LEFT JOIN
      timeseries.datasets AS dat
    USING (set_id)
    WHERE dat.set_id IS NULL
    INTO v_nonexistent_datasets;

    IF array_length(v_nonexistent_datasets, 1) != 0 THEN
      RETURN json_build_object('status', 'failure',
                               'reason', 'Some datasets do not exist.',
                               'missing_datasets', v_nonexistent_datasets);
    END IF;

    DELETE FROM timeseries.release_dataset
    WHERE release_id = p_id;

    INSERT INTO timeseries.release_dataset
    SELECT p_id, set_id FROM tmp_release_update;
  END IF;

  UPDATE timeseries.release_calendar
  AS cal
  SET
    title = COALESCE(p_title, cal.title),
    note = COALESCE(p_note, cal.note),
    release_date = COALESCE(p_date, cal.release_date),
    reference_year = COALESCE(p_year, cal.reference_year),
    reference_period = COALESCE(p_period, cal.reference_period),
    reference_frequency = COALESCE(p_frequency, cal.reference_frequency)
  WHERE id = p_id;

  RETURN json_build_object('status', 'ok');
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;


CREATE FUNCTION timeseries.list_releases(p_include_past BOOLEAN DEFAULT FALSE)
RETURNS TABLE(id TEXT,
              title TEXT,
              note TEXT,
              release_date TIMESTAMPTZ,
              reference_year INTEGER,
              reference_period INTEGER,
              reference_frequency INTEGER)
AS $$
BEGIN
  RETURN QUERY
  SELECT rls.id, rls.title, rls.note, rls.release_date, rls.reference_year,
         rls.reference_period, rls.reference_frequency
  FROM timeseries.release_calendar
  AS rls
  WHERE (p_include_past OR rls.release_date >= CURRENT_TIMESTAMP)
  ORDER BY release_date, id;
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;

CREATE FUNCTION timeseries.get_next_release_for_sets()
RETURNS TABLE(set_id TEXT,
              release_id TEXT,
              release_date TIMESTAMPTZ)
AS $$
BEGIN
  RETURN QUERY
  WITH releases_with_set AS (
    SELECT rls.release_id, tmp.set_id
    FROM timeseries.release_dataset AS rls
    JOIN tmp_get_release AS tmp
    USING(set_id)
  )
  SELECT DISTINCT ON(releases_with_set.set_id) releases_with_set.set_id, rls.id AS release_id, rls.release_date
  FROM timeseries.release_calendar AS rls
  JOIN releases_with_set
  ON rls.id = releases_with_set.release_id
  WHERE rls.release_date > CURRENT_TIMESTAMP
  ORDER BY releases_with_set.set_id, rls.release_date;
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;
