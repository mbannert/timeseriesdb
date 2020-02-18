CREATE FUNCTION timeseries.md_unlocal_upsert(validity_in DATE, on_conflict TEXT)
RETURNS JSONB
AS $$
DECLARE
  v_invalid_keys TEXT[];
  v_missing_keys TEXT[];
BEGIN
  SELECT array_agg(DISTINCT tmp.ts_key)
  INTO v_invalid_keys
  FROM tmp_md_insert AS tmp
  INNER JOIN timeseries.metadata AS md
  ON tmp.ts_key = md.ts_key
  AND validity_in < md.validity;

  INSERT INTO timeseries.metadata(ts_key, validity, metadata)
  SELECT tmp.ts_key, validity_in, tmp.metadata
  FROM tmp_md_insert AS tmp
  INNER JOIN timeseries.catalog AS cat
  ON tmp.ts_key = cat.ts_key
  AND (v_invalid_keys IS NULL OR NOT tmp.ts_key = ANY(v_invalid_keys))
  ON CONFLICT (ts_key, validity) DO UPDATE
  SET
    metadata = CASE WHEN on_conflict = 'update' THEN timeseries.metadata.metadata || EXCLUDED.metadata
                    WHEN on_conflict = 'overwrite' THEN EXCLUDED.metadata
                    ELSE timeseries.metadata.metadata END,
    created_by = EXCLUDED.created_by,
    created_at  = EXCLUDED.created_at;

  SELECT array_agg(DISTINCT tmp.ts_key)
  FROM tmp_md_insert AS tmp
  LEFT JOIN timeseries.catalog AS cat
  ON tmp.ts_key = cat.ts_key
  WHERE cat.ts_key IS NULL
  INTO v_missing_keys;

  RETURN build_meta_status(v_missing_keys, v_invalid_keys);
END;
$$ LANGUAGE PLPGSQL;


CREATE FUNCTION timeseries.md_local_upsert(validity_in DATE, on_conflict TEXT)
RETURNS JSON
AS $$
DECLARE
  v_invalid_keys TEXT[];
  v_missing_keys TEXT[];
BEGIN
  SELECT array_agg(DISTINCT tmp.ts_key)
  INTO v_invalid_keys
  FROM tmp_md_insert AS tmp
  INNER JOIN timeseries.metadata_localized AS md
  ON tmp.ts_key = md.ts_key
  AND validity_in < md.validity;

  INSERT INTO timeseries.metadata_localized(ts_key, locale, validity, metadata)
  SELECT tmp.ts_key, tmp.locale, validity_in, tmp.metadata
  FROM tmp_md_insert AS tmp
  INNER JOIN timeseries.catalog AS cat
  ON tmp.ts_key = cat.ts_key
  AND (v_invalid_keys IS NULL OR NOT tmp.ts_key = ANY(v_invalid_keys))
  ON CONFLICT (ts_key, locale, validity) DO UPDATE
  SET
    metadata = CASE WHEN on_conflict = 'update' THEN timeseries.metadata_localized.metadata || EXCLUDED.metadata
                    WHEN on_conflict = 'overwrite' THEN EXCLUDED.metadata
                    ELSE timeseries.metadata_localized.metadata END,
    created_by = EXCLUDED.created_by,
    created_at  = EXCLUDED.created_at;

  SELECT array_agg(DISTINCT tmp.ts_key)
  FROM tmp_md_insert AS tmp
  LEFT JOIN timeseries.catalog AS cat
  ON tmp.ts_key = cat.ts_key
  WHERE cat.ts_key IS NULL
  INTO v_missing_keys;

  RETURN build_meta_status(v_missing_keys, v_invalid_keys);
END;
$$ LANGUAGE PLPGSQL;

CREATE FUNCTION build_meta_status(v_missing_keys TEXT[], v_invalid_keys TEXT[])
RETURNS JSON
AS $$
DECLARE
  v_n_invalid INTEGER;
  v_n_missing INTEGER;
  v_status JSONB := jsonb_build_object('status', 'ok');
BEGIN
  v_n_invalid := array_length(v_invalid_keys, 1);
  v_n_missing := array_length(v_missing_keys, 1);

  IF v_n_invalid > 0 OR v_n_missing > 0 THEN
    v_status := jsonb_build_object('status', 'warning', 'warnings', array[]::jsonb[]);

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
$$ LANGUAGE PLPGSQL;

CREATE FUNCTION timeseries.read_metadata_raw(valid_on DATE DEFAULT CURRENT_DATE)
RETURNS TABLE(ts_key TEXT, metadata JSONB)
AS $$
BEGIN
  IF valid_on IS NULL THEN
    valid_on := CURRENT_DATE;
  END IF;

  RETURN QUERY SELECT DISTINCT ON (rd.ts_key) rd.ts_key, md.metadata
    FROM tmp_ts_read_keys AS rd
    JOIN timeseries.metadata AS md
    ON rd.ts_key = md.ts_key
    AND md.validity <= valid_on
    ORDER BY rd.ts_key, md.validity DESC;
END;
$$ LANGUAGE PLPGSQL;

-- TODO: loc does not necessarily need a default but then it needs to move to the front of the list
CREATE FUNCTION timeseries.read_metadata_localized_raw(valid_on DATE DEFAULT CURRENT_DATE, loc TEXT DEFAULT 'en')
RETURNS TABLE(ts_key TEXT, metadata JSONB)
AS $$
BEGIN
  IF valid_on IS NULL THEN
    valid_on := CURRENT_DATE;
  END IF;

  RETURN QUERY SELECT DISTINCT ON (rd.ts_key) rd.ts_key, md.metadata
    FROM tmp_ts_read_keys AS rd
    JOIN timeseries.metadata_localized AS md
    ON rd.ts_key = md.ts_key
    AND md.validity <= valid_on
    AND md.locale = loc
    ORDER BY rd.ts_key, md.validity DESC;
END;
$$ LANGUAGE PLPGSQL;

CREATE FUNCTION timeseries.get_latest_vintages_metadata()
RETURNS TABLE(ts_key TEXT, validity DATE)
AS $$
BEGIN
  RETURN QUERY SELECT DISTINCT ON (rd.ts_key) rd.ts_key, md.validity
    FROM tmp_ts_read_keys AS rd
    JOIN timeseries.metadata AS md
    ON rd.ts_key = md.ts_key
    ORDER BY rd.ts_key, md.validity DESC;
END;
$$ LANGUAGE PLPGSQL;

CREATE FUNCTION timeseries.get_latest_vintages_metadata_localized(locale_in TEXT)
RETURNS TABLE(ts_key TEXT, validity DATE)
AS $$
BEGIN
  RETURN QUERY SELECT DISTINCT ON (rd.ts_key) rd.ts_key, md.validity
    FROM tmp_ts_read_keys AS rd
    JOIN timeseries.metadata_localized AS md
    ON rd.ts_key = md.ts_key
    AND md.locale = locale_in
    ORDER BY rd.ts_key, md.validity DESC;
END;
$$ LANGUAGE PLPGSQL;
