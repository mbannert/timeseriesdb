-- DDL Script
-- strange: some of this doesn't run in data grip, just run it via psql.
ROLLBACK
BEGIN;
ALTER TABLE sbx_timeseries.meta_data_unlocalized
  DROP CONSTRAINT meta_data_unlocalized_ts_key_fkey;
ALTER TABLE sbx_timeseries.meta_data_unlocalized
  DROP CONSTRAINT meta_data_unlocalized_pkey;
ALTER TABLE sbx_timeseries.meta_data_localized
  DROP CONSTRAINT meta_data_localized_ts_key_fkey;
ALTER TABLE sbx_timeseries.meta_data_localized
  DROP CONSTRAINT meta_data_localized_pkey;

ALTER TABLE sbx_timeseries.timeseries_main
  DROP CONSTRAINT timeseries_main_pkey;

ALTER TABLE sbx_timeseries.timeseries_main
  ADD COLUMN validity DATERANGE;
UPDATE sbx_timeseries.timeseries_main
SET validity = '(,)' :: DATERANGE;
ALTER TABLE sbx_timeseries.timeseries_main
  ADD PRIMARY KEY (ts_key, validity);
ALTER TABLE sbx_timeseries.timeseries_main
  ADD EXCLUDE USING GIST (ts_key WITH =, validity WITH &&);

ALTER TABLE sbx_timeseries.meta_data_unlocalized
  ADD COLUMN validity DATERANGE;
UPDATE sbx_timeseries.meta_data_unlocalized
SET validity = '(,)' :: DATERANGE;
ALTER TABLE sbx_timeseries.meta_data_unlocalized
  ADD PRIMARY KEY (ts_key, validity);
COMMIT;
BEGIN

ALTER TABLE sbx_timeseries.meta_data_unlocalized
  ADD CONSTRAINT meta_data_unlocalized_timeseries_main_fkey
FOREIGN KEY (ts_key, validity)
REFERENCES sbx_timeseries.timeseries_main (ts_key, validity)
ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE sbx_timeseries.meta_data_localized
  ADD COLUMN validity DATERANGE;
UPDATE sbx_timeseries.meta_data_localized
SET validity = '(),)' :: DATERANGE;
ALTER TABLE sbx_timeseries.meta_data_localized
  ADD PRIMARY KEY (ts_key, validity);
ALTER TABLE sbx_timeseries.meta_data_localized
  ADD CONSTRAINT meta_data_localized_timeseries_main_fkey
FOREIGN KEY (ts_key, validity)
REFERENCES sbx_timeseries.timeseries_main (ts_key, validity)
ON DELETE CASCADE ON UPDATE CASCADE;


COMMIT;

VACUUM sbx_timeseries.timeseries_main;
VACUUM sbx_timeseries.meta_data_unlocalized;
VACUUM sbx_timeseries.meta_data_localized;
