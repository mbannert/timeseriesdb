INSERT INTO lgcy_timeseries.catalog (
	SELECT ts_key
	FROM lgcy_timeseries_copy.meta_data_unlocalized
	WHERE NOT md_generated_by = 'dkun'
);

drop table tmp_ts_updates;
CREATE TEMPORARY TABLE tmp_ts_updates
AS (
-- The textual representation of NA (which is to be converted to NULL) complicates the query
WITH series AS (
  SELECT ts_key, ts_data, ts_frequency
  FROM lgcy_timeseries_copy.timeseries_main
  WHERE ts_key IN ( SELECT ts_key FROM lgcy_timeseries.catalog)
),
dt AS (
  SELECT ts_key, (each(ts_data)).key as time, (each(ts_data)).value, ts_frequency
  FROM series
  ORDER BY ts_key, time
)
SELECT ts_key, json_build_object(
    'time', json_agg(time),
    'value', array_agg(CASE WHEN value = 'NA' THEN NULL ELSE value END)::NUMERIC[],
    'frequency', ts_frequency
    ) as ts_data,
    NULL as coverage -- TODO: is text (or converted to in ts insert)
    FROM dt
    GROUP BY ts_key, ts_frequency
);

alter table tmp_ts_updates alter column coverage type DATERANGE USING NULL;
GRANT ALL ON TABLE tmp_ts_updates TO lgcy_timeseries_admin;
SELECT * FROM lgcy_timeseries.ts_insert(CURRENT_DATE, CURRENT_TIMESTAMP, 'lgcy_timeseries_access_main');

-- The textual representation of NA (which is to be converted to NULL) complicates the query
WITH series AS (
  SELECT ts_key, ts_data, ts_frequency, lower(ts_validity) as ts_validity
  FROM timeseries.timeseries_vintages
  WHERE ts_key ~ 'ch.fso.*gva'
),
dt AS (
  SELECT ts_key, (each(ts_data)).key as time, (each(ts_data)).value, ts_frequency, ts_validity
  FROM series
  ORDER BY ts_key, ts_validity, time
),
vintages AS (
	SELECT ts_key, json_build_object(
		'time', json_agg(time),
		'value', array_agg(CASE WHEN value = 'NA' THEN NULL ELSE value END)::NUMERIC[],
		'frequency', ts_frequency
		) as ts_data,
		ts_validity,
		concat('[', min(time), ',', max(time), ')')::daterange AS coverage,
		'' as access
		FROM dt
		GROUP BY ts_key, ts_validity, ts_frequency
)
INSERT INTO new_schema.timeseries_main (
	SELECT * FROM vintages
);

-- Fill metadata_unlocalized
-- CREATE TEMPORARY TABLE tmp_md_insert
-- AS (
--   SELECT ts_key, meta_data AS metadata
--   FROM lgcy_timeseries_copy.meta_data_unlocalized
--   WHERE meta_data IS NOT NULL
--   AND ts_key IN (
--     SELECT ts_key FROM lgcy_timeseries.catalog
--   )
-- );
--
-- GRANT ALL ON TABLE tmp_md_insert TO lgcy_timeseries_admin;
-- SELECT * FROM lgcy_timeseries.metadata_unlocalized_upsert(CURRENT_DATE, 'overwrite');
