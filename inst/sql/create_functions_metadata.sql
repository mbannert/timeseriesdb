-- Inserts new data from tmp_md_insert into metadata
--
-- tmp_md_insert has columns
-- (ts_key   TEXT,
--  metadata JSONB)
-- It is allowed to specify the current validity edge as validity_in in which case
-- the edge gets updated or overridden instead of creating a new vintage.
--
-- param: validity_in The start of the validity to be stored. Must be >= current latest validity
-- param: on_conflict What to do if validity_in coincides with a keys current edge.
--        Possible values are 'update', 'overwrite' and 'ignore'
--        'update' concatenates the existing record with the incoming one with jsonb's ||
--        'overwrite' overwrites the existing record with the incoming one
--        'ignore' keeps the current record untouched
--
-- returns: A json with either {"status": "ok"} or
--          {"status": "warning", "warnings": [{"message": "", "offending_keys": [...]}, ...]}
CREATE OR REPLACE FUNCTION timeseries.md_unlocal_upsert(validity_in DATE, on_conflict TEXT)
RETURNS JSONB
AS $$
DECLARE
  -- Holds keys whose latest validity is greater than validity_in
  v_invalid_keys TEXT[];

  -- Holds keys that do not appear in the catalog for reporting
  v_missing_keys TEXT[];
BEGIN
  -- Find keys to ignore and store them for later
  -- Any updates to outdated records (validity_in < md.validity) are not permitted
  SELECT array_agg(DISTINCT tmp.ts_key)
  INTO v_invalid_keys
  FROM tmp_md_insert AS tmp
  INNER JOIN timeseries.metadata AS md
  USING (ts_key)
  WHERE validity_in < md.validity;

  -- Main write
  INSERT INTO timeseries.metadata(ts_key, validity, metadata)
  SELECT tmp.ts_key, validity_in, tmp.metadata
  FROM tmp_md_insert AS tmp
  INNER JOIN timeseries.catalog AS cat
  USING (ts_key)
  -- If no keys are invalid, v_invalid_keys is NULL
  WHERE (v_invalid_keys IS NULL OR NOT tmp.ts_key = ANY(v_invalid_keys))
  ON CONFLICT (ts_key, validity) DO UPDATE
  SET
    metadata = CASE WHEN on_conflict = 'update' THEN timeseries.metadata.metadata || EXCLUDED.metadata
                    WHEN on_conflict = 'overwrite' THEN EXCLUDED.metadata
                    ELSE timeseries.metadata.metadata END,
    created_by = EXCLUDED.created_by,
    created_at  = EXCLUDED.created_at;

  -- Select keys not in catalog for reporting
  SELECT array_agg(DISTINCT tmp.ts_key)
  FROM tmp_md_insert AS tmp
  LEFT JOIN timeseries.catalog AS cat
  USING (ts_key)
  WHERE cat.ts_key IS NULL
  INTO v_missing_keys;

  RETURN timeseries.build_meta_status(v_missing_keys, v_invalid_keys);
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;





-- Inserts new data from tmp_md_insert into metadata_localized
--
-- tmp_md_insert has columns
-- (ts_key   TEXT,
--  locale   TEXT,
--  metadata JSONB)
-- It is allowed to specify the current validity edge as validity_in in which case
-- the edge gets updated or overridden instead of creating a new vintage.
--
-- param: validity_in The start of the validity to be stored. Must be >= current latest validity
-- param: on_conflict What to do if validity_in coincides with a keys current edge.
--        Possible values are 'update', 'overwrite' and 'ignore'
--        'update' concatenates the existing record with the incoming one with jsonb's ||
--        'overwrite' overwrites the existing record with the incoming one
--        'ignore' keeps the current record untouched
--
-- returns: A json with either {"status": "ok"} or
--          {"status": "warning", "warnings": [{"message": "", "offending_keys": [...]}, ...]}
CREATE OR REPLACE FUNCTION timeseries.md_local_upsert(validity_in DATE, on_conflict TEXT)
RETURNS JSON
AS $$
DECLARE
-- Holds keys whose latest validity is greater than validity_in
  v_invalid_keys TEXT[];

  -- Holds keys that do not appear in the catalog for reporting
  v_missing_keys TEXT[];
BEGIN
  -- Find keys to ignore and store them for later
  -- Any updates to outdated records (validity_in < md.validity) are not permitted
  SELECT array_agg(DISTINCT tmp.ts_key)
  INTO v_invalid_keys
  FROM tmp_md_insert AS tmp
  INNER JOIN timeseries.metadata_localized AS md
  USING (ts_key)
  WHERE validity_in < md.validity;

  -- Main write
  INSERT INTO timeseries.metadata_localized(ts_key, locale, validity, metadata)
  SELECT tmp.ts_key, tmp.locale, validity_in, tmp.metadata
  FROM tmp_md_insert AS tmp
  INNER JOIN timeseries.catalog AS cat
  USING (ts_key)
  -- If no keys are invalid, v_invalid_keys is NULL
  WHERE (v_invalid_keys IS NULL OR NOT ts_key = ANY(v_invalid_keys))
  ON CONFLICT (ts_key, locale, validity) DO UPDATE
  SET
    metadata = CASE WHEN on_conflict = 'update' THEN timeseries.metadata_localized.metadata || EXCLUDED.metadata
                    WHEN on_conflict = 'overwrite' THEN EXCLUDED.metadata
                    ELSE timeseries.metadata_localized.metadata END,
    created_by = EXCLUDED.created_by,
    created_at  = EXCLUDED.created_at;

  -- Select keys not in catalog for reporting
  SELECT array_agg(DISTINCT tmp.ts_key)
  FROM tmp_md_insert AS tmp
  LEFT JOIN timeseries.catalog AS cat
  USING (ts_key)
  WHERE cat.ts_key IS NULL
  INTO v_missing_keys;

  RETURN timeseries.build_meta_status(v_missing_keys, v_invalid_keys);
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;






-- Helper to build response object for metadata insert functions
--
-- param: v_missing_keys keys in tmp_md_insert that are not in the catalog
-- param: v_invalid_keys keys for which an update of metadata was not permitted
--
-- returns: A json with either {"status": "ok"} or {"status": "warning", "warnings": [...warning messages...]}
CREATE OR REPLACE FUNCTION timeseries.build_meta_status(v_missing_keys TEXT[], v_invalid_keys TEXT[])
RETURNS JSON
AS $$
DECLARE
  -- keep lengths of vectors to avoid recalculation
  v_n_invalid INTEGER;
  v_n_missing INTEGER;

  -- Final object to be returned, defaults to "OK" status
  v_status JSONB := jsonb_build_object('status', 'ok');
BEGIN
  v_n_invalid := array_length(v_invalid_keys, 1);
  v_n_missing := array_length(v_missing_keys, 1);

  -- If anythings needs to be done
  IF v_n_invalid > 0 OR v_n_missing > 0 THEN
    -- "OK" is no longer the case
    v_status := jsonb_build_object('status', 'warning', 'warnings', array[]::jsonb[]);

    -- Either some keys are invalid or some are missing or both
    -- Build up the warnings array accordingly
    IF v_n_invalid > 0 THEN
      v_status := jsonb_set(
        v_status,
        array['warnings'],
        v_status->'warnings' || jsonb_build_array(jsonb_build_object('message', 'Some keys already have a later vintage',
                                                                     'offending_keys', to_jsonb(v_invalid_keys)))
      );
    END IF;

    IF v_n_missing > 0 THEN
      v_status := jsonb_set(
        v_status,
        array['warnings'],
        v_status->'warnings' || jsonb_build_array(jsonb_build_object('message', 'Some keys were not found in the catalog',
                                                                     'offending_keys', to_jsonb(v_missing_keys)))
      );
    END IF;
  END IF;

  RETURN v_status;
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;






-- Read unlocalized metadata in raw (i.e. json) form
--
-- tmp_ts_read_keys has columns (ts_key TEXT)
--
-- param: valid_on the date for which to get the metadata
--
-- returns: table(ts_key TEXT, metadata JSONB)
CREATE OR REPLACE FUNCTION timeseries.read_metadata_raw(valid_on DATE DEFAULT CURRENT_DATE)
RETURNS TABLE(ts_key TEXT, metadata JSONB)
AS $$
BEGIN
  -- If an explicit NULL is passed, the default is not used -> take care of that here
  IF valid_on IS NULL THEN
    valid_on := CURRENT_DATE;
  END IF;

  -- DISCINCT ON ts_key with ORDER BY ts_key, validity DESC
  -- results in the row with validity = max(validities <= valid_on) for each key
  RETURN QUERY SELECT DISTINCT ON (rd.ts_key) rd.ts_key, md.metadata
    FROM tmp_ts_read_keys AS rd
    JOIN timeseries.metadata AS md
    USING (ts_key)
    WHERE validity <= valid_on
    ORDER BY rd.ts_key, md.validity DESC;
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;

CREATE OR REPLACE FUNCTION timeseries.read_metadata_raw(p_keys TEXT[],
                                                        p_valid_on DATE DEFAULT CURRENT_DATE)
RETURNS TABLE(ts_key TEXT, metadata JSONB)
AS $$
BEGIN
  CREATE TEMPORARY TABLE tmp_ts_read_keys
  ON COMMIT DROP
  AS (
    SELECT * FROM unnest(p_keys) AS ts_key
  );

  RETURN QUERY
  SELECT * FROM timeseries.read_metadata_raw(p_valid_on::DATE);
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;

-- Read localized metadata in raw (i.e. json) form
--
-- tmp_ts_read_keys has columns (ts_key TEXT)
--
-- param: valid_on the date for which to get the metadata
--
-- returns: table(ts_key TEXT, metadata JSONB)
CREATE OR REPLACE FUNCTION timeseries.read_metadata_localized_raw(valid_on DATE DEFAULT CURRENT_DATE, loc TEXT DEFAULT 'en')
RETURNS TABLE(ts_key TEXT, metadata JSONB)
AS $$
BEGIN
  -- If an explicit NULL is passed, the default is not used -> take care of that here
  IF valid_on IS NULL THEN
    valid_on := CURRENT_DATE;
  END IF;

  -- DISCINCT ON ts_key with ORDER BY ts_key, validity DESC
  -- results in the row with validity = max(validities <= valid_on) for each key
  RETURN QUERY SELECT DISTINCT ON (rd.ts_key) rd.ts_key, md.metadata
    FROM tmp_ts_read_keys AS rd
    JOIN timeseries.metadata_localized AS md
    USING (ts_key)
    WHERE validity <= valid_on
    AND locale = loc
    ORDER BY rd.ts_key, md.validity DESC;
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;


CREATE OR REPLACE FUNCTION timeseries.read_metadata_localized_raw(p_keys TEXT[],
                                                        p_valid_on DATE DEFAULT CURRENT_DATE,
                                                        loc TEXT DEFAULT 'en')
RETURNS TABLE(ts_key TEXT, metadata JSONB)
AS $$
BEGIN
  CREATE TEMPORARY TABLE tmp_ts_read_keys
  ON COMMIT DROP
  AS (
    SELECT * FROM unnest(p_keys) AS ts_key
  );

  RETURN QUERY
  SELECT * FROM timeseries.read_metadata_localized_raw(p_valid_on::DATE, loc::TEXT);
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;



-- Get the latest unlocalized metadata validities for keys
--
-- This is useful e.g. when trying to update the edge of metadata when it is not
-- immediately known
--
-- tmp_ts_read_keys has columns (ts_key TEXT)
--
-- returns: table(ts_key TEXT, metadata JSONB)
CREATE OR REPLACE FUNCTION timeseries.get_latest_vintages_metadata()
RETURNS TABLE(ts_key TEXT, validity DATE)
AS $$
BEGIN
  RETURN QUERY SELECT DISTINCT ON (rd.ts_key) rd.ts_key, md.validity
    FROM tmp_ts_read_keys AS rd
    LEFT JOIN timeseries.metadata AS md
    USING (ts_key)
    ORDER BY ts_key, validity DESC;
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;





-- Get the latest localized metadata validities for keys
--
-- This is useful e.g. when trying to update the edge of metadata when it is not
-- immediately known
--
-- param: locale_in the locale for which to get validities
--
-- tmp_ts_read_keys has columns (ts_key TEXT)
--
-- returns: table(ts_key TEXT, metadata JSONB)
CREATE OR REPLACE FUNCTION timeseries.get_latest_vintages_metadata_localized(locale_in TEXT)
RETURNS TABLE(ts_key TEXT, validity DATE)
AS $$
BEGIN
  RETURN QUERY SELECT DISTINCT ON (rd.ts_key) rd.ts_key, md.validity
    FROM tmp_ts_read_keys AS rd
    LEFT JOIN timeseries.metadata_localized AS md
    USING (ts_key)
    WHERE locale = locale_in
    OR locale IS NULL
    ORDER BY rd.ts_key, md.validity DESC;
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;
