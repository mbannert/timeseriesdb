BEGIN;
-- make it easy to change the schema name:
-- this is the only place it appears in the whole script
CREATE SCHEMA IF NOT EXISTS timeseries;
SET LOCAL search_path ='timeseries';

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE TABLE datasets(
    set_id TEXT PRIMARY KEY,
    set_description TEXT,
    set_md JSON
);

INSERT INTO datasets VALUES ('default', 'A set that is used if no other set is specified. Every time series needs to be part of a dataset', NULL);

CREATE TABLE catalog(
    ts_key TEXT PRIMARY KEY,
    set_id TEXT DEFAULT 'default',
    FOREIGN KEY (set_id) REFERENCES datasets(set_id)
);

CREATE TABLE timeseries_main(
  id UUID NOT NULL DEFAULT uuid_generate_v1() PRIMARY KEY,
  ts_key TEXT NOT NULL,
  validity DATE NOT NULL DEFAULT CURRENT_DATE,
  coverage DATERANGE,
  release_date TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, -- users are expected to give release time plus tz by its name
  created_by TEXT DEFAULT CURRENT_USER,
  created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  ts_data JSON,
  access TEXT,
  UNIQUE (ts_key, validity),
  FOREIGN KEY (ts_key) REFERENCES catalog(ts_key)
);

CREATE TABLE md_local_ts (
  ts_key TEXT,
  lang TEXT,
  data_desc JSON,
  PRIMARY KEY(ts_key, lang),
  FOREIGN KEY (ts_key) REFERENCES catalog(ts_key)
);


CREATE TABLE md_local_vintages (
  vintage_id UUID,
  lang TEXT,
  meta_data JSON,
  PRIMARY KEY(vintage_id, lang),
  FOREIGN KEY (vintage_id) REFERENCES timeseries_main(id)
);


CREATE TABLE md_vintages (
  vintage_id UUID,
  meta_data JSON,
  PRIMARY KEY(vintage_id),
  FOREIGN KEY (vintage_id) REFERENCES timeseries_main(id)
);

CREATE TABLE collections (
  id UUID NOT NULL DEFAULT uuid_generate_v1() PRIMARY KEY,
  name TEXT,
  owner TEXT, 
  description TEXT,
  UNIQUE (name, owner)
);

CREATE TABLE collect_catalog (
  id UUID, 
  ts_key TEXT,
  PRIMARY KEY (id,ts_key),
  FOREIGN KEY (id) REFERENCES collections(id),
  FOREIGN KEY (ts_key) REFERENCES catalog(ts_key)
);

COMMIT;
