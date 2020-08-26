-- Insert keys into a collection
--
-- If the collection does not already exist, it is automatically created.
--
-- tmp_collect_updates has columns (ts_key TEXT)
--
-- returns: JSON {"status": "", "message": "", ["invalid_keys": [""]]}
CREATE OR REPLACE FUNCTION timeseries.insert_collect_from_tmp(collection_name TEXT,
                                                   col_owner TEXT,
                                                   description TEXT)
RETURNS JSON
AS $$
DECLARE
  v_id UUID;
  v_invalid_keys TEXT[];
BEGIN
  -- Ensure collection exists
  SELECT id FROM timeseries.collections
  WHERE name = collection_name
  AND owner = col_owner
  INTO v_id;

  IF v_id IS NULL THEN
    INSERT INTO timeseries.collections(name, owner, description)
    VALUES(collection_name, col_owner, description)
    ON CONFLICT DO NOTHING
    RETURNING id
    INTO v_id;
  END IF;

  SELECT COALESCE(array_agg(DISTINCT tmp.ts_key), '{}'::TEXT[])
  FROM tmp_collect_updates AS tmp
  LEFT OUTER JOIN timeseries.catalog AS cat
  USING (ts_key)
  WHERE cat.ts_key IS NULL
  INTO v_invalid_keys;

  INSERT INTO timeseries.collect_catalog (id, ts_key)
  SELECT v_id, ts_key FROM tmp_collect_updates
  WHERE NOT ts_key = ANY(v_invalid_keys)
  ON CONFLICT DO NOTHING;

  IF array_length(v_invalid_keys, 1) > 0 THEN
    RETURN json_build_object('status', 'warning',
                             'message', 'Some series could not be added to the user specific collection because these series were not found in the database.',
                             'invalid_keys', to_jsonb(v_invalid_keys));
  END IF;

  -- All went well
  RETURN '{"status": "ok"}'::JSON;
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;


-- List all keys in a given user collection
--
-- param: p_name the name of the collection
-- param: p_owner: owner of the collection
--
-- returns: table(ts_key TEXT)
CREATE OR REPLACE FUNCTION timeseries.keys_in_collection(p_name TEXT,
                                                         p_owner TEXT)
RETURNS TABLE(ts_key TEXT)
AS $$
BEGIN
  RETURN QUERY
  SELECT cc.ts_key
  FROM timeseries.collect_catalog AS cc
  JOIN timeseries.collections col
  USING(id)
  WHERE col.name = p_name
  AND col.owner = p_owner
  ORDER BY cc.ts_key;
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;


-- Remove keys from a user collection
--
-- Collections are identified by (name, owner) hence both need to be specified.
--
-- If the last keys are removed from a collection it is also deleted
--
-- tmp_collection_remove has columns (ts_key TEXT)
--
-- param: col_name the name of the collection from which to remove the keys
-- param: col_owner the owner of the collection
--
-- returns: json {"status": "", "message": "", ["removed_collection"]: ""}
CREATE OR REPLACE FUNCTION timeseries.collection_remove(col_name TEXT, col_owner TEXT)
RETURNS JSON
AS $$
DECLARE
  v_id UUID;
  v_removed_keys TEXT[];
  v_collection_deleted BOOLEAN;
BEGIN
  SELECT id FROM timeseries.collections
  WHERE name = col_name
  AND owner = col_owner
  INTO v_id;

  IF v_id IS NULL THEN
    RETURN json_build_object('status', 'error',
                             'message', 'The set/user combination does not exist!');
  END IF;

  -- Leaving this as is for possible regex option in the future
  -- Otherwise the removed keys are already known
  -- TODO: Unless we want to also raise a warning, if some keys are not in the catalog at all
  WITH del_k AS (
    DELETE FROM timeseries.collect_catalog cc
    USING tmp_collection_remove rm
    WHERE cc.ts_key = rm.ts_key
    AND cc.id = v_id
    RETURNING rm.ts_key
  )
  SELECT array_agg(DISTINCT ts_key)
  FROM del_k
  INTO v_removed_keys;

  -- 'der letzte macht das licht aus'
  -- keeping an entirely empty set makes no sense,
  -- hence we delete a set that does not contain
  -- any series after removing keys.
  IF NOT EXISTS (SELECT 1 FROM timeseries.collect_catalog WHERE id = v_id) THEN
    DELETE FROM timeseries.collections
    WHERE id = v_id;

    v_collection_deleted := true;
  END IF;

  IF v_collection_deleted = true THEN
    RETURN json_build_object('status', 'notice',
                             'message', 'The collection was also removed because became empty.',
                             'removed_collection', to_json(v_id));
  ELSE
    RETURN json_build_object('status', 'ok');
  END IF;
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;






-- Remove keys from a user collection
--
-- Collections are identified by (name, owner) hence both need to be specified.
--
-- param: col_name the name of the collection from which to remove the keys
-- param: col_owner the owner of the collection
--
-- returns: json {"status": "", "message": "", ["id"]: ""}
CREATE OR REPLACE FUNCTION timeseries.collection_delete(col_name TEXT,
                                             col_owner TEXT)
RETURNS JSON
AS $$
DECLARE
  deleted_id UUID;
  result JSON;
BEGIN
  IF NOT EXISTS (SELECT 1 FROM timeseries.collections
  WHERE name = col_name
  AND owner = col_owner) THEN
    RETURN json_build_object('status', 'warning',
                             'message', 'Collection could not be found for this user.');
  ELSE
    DELETE FROM timeseries.collections
    WHERE owner = col_owner
    AND name = col_name
    RETURNING id
    INTO deleted_id;

    RETURN json_build_object('status', 'ok',
                             'id', deleted_id);
  END IF;
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;

-- Read all (accessible) series in a collection
--
-- This function wraps read_ts_raw, filling the tmp_ts_read_keys table with
-- keys in the desired collection.
CREATE OR REPLACE FUNCTION timeseries.read_ts_collection_raw(
                                                  p_name TEXT,
                                                  p_owner TEXT,
                                                  p_valid_on DATE DEFAULT CURRENT_DATE,
                                                  p_respect_release_date BOOLEAN DEFAULT FALSE)
RETURNS TABLE(ts_key TEXT, ts_data JSON)
AS $$
BEGIN
  CREATE TEMPORARY TABLE tmp_ts_read_keys
  ON COMMIT DROP
  AS (
    SELECT colcat.ts_key
    FROM timeseries.collections col
    JOIN timeseries.collect_catalog colcat
    ON col.id = colcat.id
    WHERE col.owner = p_owner
      AND col.name = p_name
  );

  RETURN QUERY
  SELECT * FROM timeseries.read_ts_raw(p_valid_on, p_respect_release_date);
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;


-- List all collections belonging to p_user
--
-- Mainly for use in the API
--
-- param p_user: The user for which to get collections
CREATE OR REPLACE FUNCTION timeseries.list_collections(p_user TEXT)
RETURNS TABLE(name TEXT, description TEXT)
AS $$
BEGIN
  RETURN QUERY
  SELECT coll.name, coll.description
  FROM timeseries.collections AS coll
  WHERE owner = p_user
  ORDER BY coll.name;
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;

-- Get the last time the collection was updated
--
-- Returns the created_at timestamp of the series in the given collection
-- that was most recently updated.
CREATE OR REPLACE FUNCTION timeseries.collection_get_last_update(p_collection TEXT,
                                                                 p_owner TEXT)
RETURNS TABLE(name TEXT, updated TIMESTAMPTZ)
AS $$
BEGIN
  CREATE TEMPORARY TABLE tmp_ts_read_keys
  ON COMMIT DROP
  AS (
    SELECT ts_key
    FROM timeseries.collect_catalog AS cat
    JOIN timeseries.collections AS coll
    USING(id)
    WHERE coll.name = p_collection
    AND coll.owner = p_owner
  );

  RETURN QUERY
  SELECT p_collection AS name, max(ud.updated) AS updated
  FROM timeseries.ts_get_last_update() AS ud;
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
SET search_path = timeseries, pg_temp;
