-- create schema, admin, reader, writer

-- Create access level role and insert it into newschema.access_levels

-- run R setup on new schema as schema-admin


-- Fill le catalog
-- These will all have the default dataset
INSERT INTO newschema.catalog ts_key (
  SELECT ts_key
  FROM oldschema.timeseries_main
);



-- Fill main table
CREATE TEMPORARY TABLE tmp_ts_updates
ON COMMIT DROP
AS (
  SELECT ts_key,
         json_build_object(
          'time', akeys(ts_data),
          'data', avals(ts_data)::NUMERIC[],
          'frequency', ts_frequency
         ) AS ts_data,
         NULL AS coverage
  FROM oldschema.timeseries_main
);

-- ...the sneaky way
SELECT * FROM newschema.insert_from_tmp(CURRENT_DATE, CURRENT_TIMESTAMP, 'access_level');

-- figure out and assign ALL THE DATASETS

-- Fill metadata_localized
CREATE TEMPORARY TABLE tmp_md_insert
ON COMMIT DROP
AS (
  SELECT ts_key, locale_info AS locale, meta_data AS metadata
  FROM oldschema.meta_data_localized
);

SELECT * FROM newschema.md_local_upsert(CURRENT_DATE, 'overwrite');

-- Fill metadata_unlocalized
CREATE TEMPORARY TABLE tmp_md_insert
ON COMMIT DROP
AS (
  SELECT ts_key, meta_data AS metadata
  FROM oldschema.meta_data_unlocalized
  WHERE meta_data IS NOT NULL
);

SELECT * FROM newschema.md_unlocal_upsert(CURRENT_DATE, 'overwrite');
