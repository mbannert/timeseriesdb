-- Create a new release
--
-- A release marks the point in time where new data for a group of datasets are
-- to be made public.
-- The year and period fields identify what time the data themselves are about.
-- Numbers may be released with a certain lag such that the release for
-- e.g. March are only released in mit April so this release would have
-- 3 as its period and 4-15 as its release date.
--
-- param: p_id The identifier of the release e.g. 'gdp_march_2020'
-- param: p_title Display title for the release
-- param: p_note Additional remarks
-- param: p_date The timestamptz when the release is to occur
-- param: p_year The year the release pertains to
-- param: p_period The period (month, quarter etc) the release pertains to
-- param: p_frequency The frequency of the release e.g. 4 for quarterly
--
-- returns: json {"status": "", "reason": "", "missing_datasets", [""]}
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
                                          release_date, target_year,
                                          target_period, target_frequency)
  VALUES (p_id, p_title, p_note, p_date, p_year, p_period, p_frequency);

  INSERT INTO timeseries.release_dataset(release_id, set_id)
  SELECT p_id, set_id FROM tmp_release_insert;

  RETURN '{"status": "ok"}'::JSON;
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;



-- Update an existing release
--
-- Any fields provided to update_release will overwrite the existing data for the
-- given release. The other fields remain untouched.
--
-- param: p_id The identifier of the release e.g. 'gdp_march_2020'
-- param: p_title Display title for the release
-- param: p_note Additional remarks
-- param: p_date The timestamptz when the release is to occur
-- param: p_year The year the release pertains to
-- param: p_period The period (month, quarter etc) the release pertains to
-- param: p_frequency The frequency of the release e.g. 4 for quarterly
-- param: p_sets_change Do the sets of the release change? Guards against reading from a nonexistent temp table
--
-- returns: json {"status": "", "reason": "", "missing_datasets", [""]}
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
    target_year = COALESCE(p_year, cal.target_year),
    target_period = COALESCE(p_period, cal.target_period),
    target_frequency = COALESCE(p_frequency, cal.target_frequency)
  WHERE id = p_id;

  RETURN json_build_object('status', 'ok');
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;


-- List all content of the release table
--
-- param: p_include_past Should releases in the past be included? Default FALSE
--
CREATE FUNCTION timeseries.list_releases(p_include_past BOOLEAN DEFAULT FALSE)
RETURNS TABLE(id TEXT,
              title TEXT,
              note TEXT,
              release_date TIMESTAMPTZ,
              target_year INTEGER,
              target_period INTEGER,
              target_frequency INTEGER)
AS $$
BEGIN
  RETURN QUERY
  SELECT rls.id, rls.title, rls.note, rls.release_date, rls.target_year,
         rls.target_period, rls.target_frequency
  FROM timeseries.release_calendar
  AS rls
  WHERE (p_include_past OR rls.release_date >= CURRENT_TIMESTAMP)
  ORDER BY release_date, id;
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;


-- Get the next release date for a given set of datasets
--
-- tmp_get_release has column (set_id TEXT)
--
-- returns: a table with (set_id TEXT, release_id TEXT, release_date TIMESTAMPTZ)
CREATE FUNCTION timeseries.get_next_release_for_sets()
RETURNS TABLE(set_id TEXT,
              release_id TEXT,
              release_date TIMESTAMPTZ)
AS $$
BEGIN
  RETURN QUERY
  WITH rlscal AS (
    SELECT rls.set_id, rls.release_id, cal.release_date
    FROM timeseries.release_calendar AS cal
    JOIN timeseries.release_dataset AS rls
    ON rls.release_id = cal.id
    WHERE cal.release_date > CURRENT_TIMESTAMP
  )
  SELECT DISTINCT ON(tmp.set_id) tmp.set_id, rlscal.release_id, rlscal.release_date
  FROM tmp_get_release AS tmp
  LEFT JOIN rlscal
  USING(set_id)
  ORDER BY tmp.set_id, rlscal.release_date;
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;

-- Get the latest release date for a given set of datasets
--
-- tmp_get_release has column (set_id TEXT)
--
-- returns: a table with (set_id TEXT, release_id TEXT, release_date TIMESTAMPTZ)
CREATE FUNCTION timeseries.get_latest_release_for_sets()
RETURNS TABLE(set_id TEXT,
              release_id TEXT,
              release_date TIMESTAMPTZ)
AS $$
BEGIN
  RETURN QUERY
  WITH rlscal AS (
    SELECT rls.set_id, rls.release_id, cal.release_date
    FROM timeseries.release_calendar AS cal
    JOIN timeseries.release_dataset AS rls
    ON rls.release_id = cal.id
    WHERE cal.release_date <= CURRENT_TIMESTAMP
  )
  SELECT DISTINCT ON(tmp.set_id) tmp.set_id, rlscal.release_id, rlscal.release_date
  FROM tmp_get_release AS tmp
  LEFT JOIN rlscal
  USING(set_id)
  ORDER BY tmp.set_id, rlscal.release_date DESC;
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;
