CREATE FUNCTION timeseries.dataset_exists(dataset_name TEXT)
RETURNS BOOL
AS $$
BEGIN
  RETURN EXISTS(SELECT 1 FROM timeseries.datasets WHERE set_id = dataset_name);
END;
$$ LANGUAGE PLPGSQL;



CREATE FUNCTION timeseries.collection_add(collection_name TEXT,
                                          col_owner TEXT,
                                          description TEXT)
RETURNS uuid
AS $$
DECLARE
  v_id uuid;
BEGIN
  SELECT id FROM timeseries.collections
  WHERE name = collection_name
  AND owner = col_owner
  INTO v_id;

  IF v_id IS NOT NULL THEN

    RETURN v_id;

  ELSE

    INSERT INTO timeseries.collections(name, owner, description)
    VALUES(collection_name, col_owner, description)
    ON CONFLICT DO NOTHING
    RETURNING id
    INTO v_id;

    RETURN v_id;
  END IF;

END;
$$ LANGUAGE PLPGSQL;



CREATE FUNCTION timeseries.collection_remove()
RETURNS JSON
AS $$
DECLARE
  result JSON;
BEGIN
  CREATE TEMP TABLE removed_keys (ts_key TEXT PRIMARY KEY) ON COMMIT DROP;
  CREATE TEMP TABLE removed_collect (id TEXT PRIMARY KEY) ON COMMIT DROP;

  WITH del_q AS (
    DELETE FROM timeseries.collect_catalog cc
    USING tmp_collection_remove rm
    WHERE cc.ts_key = rm.ts_key
    RETURNING rm.ts_key
  )
  INSERT INTO removed_keys
  SELECT ts_key FROM del_q;

  WITH del_collect AS (
  -- 'der letzte macht das licht aus'
  -- keeping an entirely empty set makes no sense,
  -- hence we delete a set that does not contain
  -- any series after removing keys.
    DELETE FROM timeseries.collections c
    USING tmp_collection_remove r
    WHERE c.id = r.c_id
    AND NOT EXISTS(SELECT 1 FROM timeseries.collect_catalog
    WHERE id IN (SELECT DISTINCT(r.c_id) FROM tmp_collection_remove r))
    RETURNING c.id
  )
  INSERT INTO removed_collect
  SELECT DISTINCT(id) FROM del_collect;


  SELECT json_build_object('number_of_removed_keys', count(k.ts_key),
                           'removed_keys', json_agg(k.ts_key),
                           'removed_collections', json_agg(DISTINCT(c.id)))
  INTO result
  FROM removed_keys k
  -- this is needed cause a comma separated FROM is basically
  -- an inner join which does not work with no key to join on.
  LEFT JOIN removed_collect c ON(TRUE);

  RETURN result;
END;
$$ LANGUAGE PLPGSQL;



CREATE FUNCTION timeseries.collection_delete(collection_name TEXT,
                                             col_owner TEXT)
RETURNS JSON
AS $$
DECLARE
  deleted_id UUID;
  result JSON;
BEGIN
  IF NOT EXISTS (SELECT 1 FROM timeseries.collections
  WHERE name = collection_name
  AND owner = col_owner) THEN
  RETURN json_build_object('status', 'warning',
                           'message', 'Collection cound not be found for this user.');
  ELSE
    DELETE FROM timeseries.collections CASCADE
    WHERE user = col_owner
    AND name = collection_name
    RETURNING id
    INTO deleted_id;

    RETURN json_build_object('status', 'ok',
                             'id', deleted_id);
  END IF;
END;
$$ LANGUAGE PLPGSQL;







CREATE FUNCTION timeseries.create_dataset(dataset_name TEXT,
                                          dataset_description TEXT DEFAULT NULL,
                                          dataset_md JSON DEFAULT NULL)
RETURNS TEXT
AS $$
  INSERT INTO timeseries.datasets(set_id, set_description, set_md)
  VALUES(dataset_name, dataset_description, dataset_md)
  RETURNING set_id
$$ LANGUAGE SQL;


CREATE FUNCTION timeseries.insert_collect_from_tmp()
RETURNS JSON
AS $$
DECLARE
  v_invalid_keys TEXT[];
BEGIN
  SELECT COALESCE(array_agg(DISTINCT tmp.ts_key), '{}'::TEXT[])
  FROM tmp_collect_updates AS tmp
  LEFT OUTER JOIN timeseries.catalog AS cat
  ON cat.ts_key = tmp.ts_key
  WHERE cat.ts_key IS NULL
  INTO v_invalid_keys;

  INSERT INTO timeseries.collect_catalog (id, ts_key)
  SELECT c_id, ts_key FROM tmp_collect_updates
  WHERE NOT ts_key = ANY(v_invalid_keys)
  ON CONFLICT DO NOTHING;

  IF array_length(v_invalid_keys, 1) > 0 THEN
    RETURN json_build_object('status', 'warning',
                             'message', 'Some series could not be added to the user specific collection because these series were not found in the database.',
                             'invalid_keys', to_jsonb(v_invalid_keys));
  END IF;

  -- All went well
  RETURN '{"status": "ok", "message": "All keys have been successfully added to the collection."}'::JSON;
END;
$$ LANGUAGE PLPGSQL;


CREATE FUNCTION timeseries.insert_from_tmp()
RETURNS JSON
AS $$
DECLARE
  v_invalid_keys TEXT[];
BEGIN
  SELECT array_agg(DISTINCT tmp.ts_key)
  INTO v_invalid_keys
  FROM tmp_ts_updates AS tmp
  INNER JOIN timeseries.timeseries_main AS main
  ON tmp.ts_key = main.ts_key
  AND tmp.validity < main.validity;

  -- IMPORTANT!!!
  -- When converting this to a warning, make sure to delete
  -- invalid keys from update table, elsewise updating the past is possible!
  IF array_length(v_invalid_keys, 1) > 0 THEN
    RETURN json_build_object('status', 'failure',
                             'reason', 'keys with invalid vintages',
                             'offending_keys', to_json(v_invalid_keys));
  END IF;

  -- after this insert the set_id is 'default' because we don't want a set parameter in our
  -- store functions
  INSERT INTO timeseries.catalog
  SELECT tmp_ts_updates.ts_key
  FROM tmp_ts_updates
  LEFT OUTER JOIN timeseries.catalog ON (timeseries.catalog.ts_key = tmp_ts_updates.ts_key)
  WHERE timeseries.catalog.ts_key IS NULL;

  -- Generate computed property "coverage"
  ALTER TABLE tmp_ts_updates
  ADD COLUMN coverage DATERANGE;
  UPDATE tmp_ts_updates
  SET coverage = concat('[', ts_data->'time'->0, ',', ts_data->'time'->-1, ')')::daterange;

  -- Main insert
  INSERT INTO timeseries.timeseries_main(ts_key, validity, coverage, release_date, ts_data, access)
  SELECT tmp.ts_key, COALESCE(tmp.validity, CURRENT_DATE), tmp.coverage, COALESCE(tmp.release_date, CURRENT_TIMESTAMP), tmp.ts_data, tmp.access
  FROM tmp_ts_updates AS tmp
  LEFT JOIN timeseries.timeseries_main AS main
  ON tmp.ts_key = main.ts_key
  AND tmp.validity = main.validity
  ON CONFLICT (ts_key, validity) DO UPDATE
  SET
    coverage = EXCLUDED.coverage,
    release_date = EXCLUDED.release_date,
    created_by = EXCLUDED.created_by,
    created_at = EXCLUDED.created_at,
    ts_data = EXCLUDED.ts_data;

  -- All went well
  RETURN '{"status": "ok", "reason": "the world is full of rainbows"}'::JSON;
END;
$$ LANGUAGE PLPGSQL
-- Read this tho: https://www.cybertec-postgresql.com/en/abusing-security-definer-functions/
SECURITY DEFINER;

CREATE FUNCTION timeseries.create_read_tmp_regex(pattern TEXT)
RETURNS VOID
AS $$
  DROP TABLE IF EXISTS tmp_ts_read_keys;
  CREATE TEMPORARY TABLE tmp_ts_read_keys AS(
  SELECT ts_key FROM timeseries.catalog
  WHERE ts_key ~ 'ts');
$$ LANGUAGE SQL;

CREATE FUNCTION timeseries.read_ts_raw(valid_on DATE DEFAULT CURRENT_DATE, respect_release_date BOOLEAN DEFAULT false)
RETURNS TABLE(ts_key TEXT, ts_data JSON)
AS $$
BEGIN
  IF valid_on IS NULL THEN
    valid_on := CURRENT_DATE;
  END IF;

  IF respect_release_date IS NULL THEN
    respect_release_date := false;
  END IF;

  RETURN QUERY SELECT distinct on (rd.ts_key) rd.ts_key, mn.ts_data
    FROM tmp_ts_read_keys as rd
    JOIN timeseries.timeseries_main as mn
    ON rd.ts_key = mn.ts_key
    AND ((NOT respect_release_date) OR mn.release_date <= CURRENT_TIMESTAMP)
    AND mn.validity <= valid_on
    ORDER BY rd.ts_key, mn.validity DESC;
END;
$$ LANGUAGE PLPGSQL; -- plpgsql because plain sql would (somewhat rightly) complain that the tmp table does not exist

CREATE FUNCTION timeseries.keys_in_dataset(id TEXT)
RETURNS TABLE(ts_key TEXT)
AS $$
BEGIN
  RETURN QUERY SELECT timeseries.catalog.ts_key
  FROM timeseries.catalog
  WHERE id = set_id;
END;
$$ LANGUAGE PLPGSQL;

CREATE FUNCTION timeseries.get_set_of_keys()
RETURNS TABLE(ts_key TEXT, set_id TEXT)
AS $$
BEGIN
  RETURN QUERY SELECT tmp.ts_key, cat.set_id
  FROM tmp_get_set AS tmp
  LEFT JOIN timeseries.catalog AS cat
  ON cat.ts_key = tmp.ts_key;
END;
$$ LANGUAGE PLPGSQL;

CREATE FUNCTION timeseries.assign_dataset(id TEXT)
RETURNS JSON
AS $$
DECLARE
  v_keys_not_in_catalog TEXT[];
BEGIN
  IF NOT EXISTS(SELECT 1 FROM timeseries.datasets WHERE set_id = id) THEN
    RETURN ('{"status": "failure", "reason": "Dataset ' || id || ' does not exist!"}')::JSON;
  END IF; -- Welcome to the stone age of programming

  UPDATE timeseries.catalog AS cat
  SET set_id = id
  FROM tmp_set_assign AS tmp -- "FROM" ;P
  WHERE cat.ts_key = tmp.ts_key;

  SELECT array_agg(tmp.ts_key)
  FROM tmp_set_assign AS tmp
  LEFT JOIN
    timeseries.catalog AS cat
  ON tmp.ts_key = cat.ts_key
  WHERE cat.ts_key IS NULL
  INTO v_keys_not_in_catalog;

  IF array_length(v_keys_not_in_catalog, 1) != 0 THEN
    RETURN ('{"status": "warning",'
    '"reason": "Some keys are not in catalog!",'
    '"offending_keys": ["' || array_to_string(v_keys_not_in_catalog, '", "') || '"]}')::JSON;
  ELSE
    RETURN '{"status": "ok"}'::JSON;
  END IF;
END;
$$ LANGUAGE PLPGSQL;

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
