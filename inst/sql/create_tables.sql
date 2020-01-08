BEGIN;
-- make it easy to change the schema name:
-- this is the only place it appears in the whole script
CREATE SCHEMA IF NOT EXISTS timeseries;
SET LOCAL search_path ='timeseries';

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE TABLE timeseries_main(
  id UUID NOT NULL DEFAULT uuid_generate_v1() PRIMARY KEY,
  ts_key TEXT,
  validity DATERANGE,
  coverage DATERANGE,
  release_date TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, -- users are expected to give release time plus tz by its name
  ts_data JSON,
  UNIQUE (ts_key, ts_validity),
  FOREIGN KEY (ts_key) REFERENCES timeseries_catalog(ts_key),
  EXCLUDE USING GIST (ts_key WITH =, validity WITH &&)
)


CREATE TABLE timeseries_catalog(
    ts_key TEXT PRIMARY KEY,
    set_id TEXT,
    created_by TEXT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (set_id) REFERENCES timeseries.datasets(set_id)
)

CREATE TABLE datasets(
    set_id TEXT PRIMARY KEY,
    set_md JSON
)

CREATE TABLE md_local_ts {
  ts_key TEXT,
  lang TEXT,
  data_desc JSON,
  PRIMARY KEY(ts_key, lang),
  FOREIGN KEY (ts_key) REFERENCES timeseries.catalog(ts_key)
}


CREATE TABLE md_local_vintages{
  vintage_key TEXT,
  lang TEXT,
  meta_data JSON,
  PRIMARY KEY(vintage_key, lang),
  FOREIGN KEY (vintage_key) REFERENCES timeseries.vintages(vintage_key)
}


CREATE TABLE md_vintages{
  vintage_key TEXT,
  meta_data JSON,
  PRIMARY KEY(vintage_key),
  FOREIGN KEY (vintage_key) REFERENCES timeseries.vintages(vintage_key)
}

CREATE TABLE collections {
  id UUID NOT NULL DEFAULT uuid_generate_v1() PRIMARY KEY,
  name TEXT,
  owner TEXT, 
  description TEXT,
  UNIQUE (name, owner)
}

CREATE TABLE collect_catalog {
  id UUID, 
  ts_key TEXT,
  PRIMARY KEY (id,ts_key),
  FOREIGN KEY (id) REFERENCES timeseries.collections(id),
  FOREIGN KEY (ts_key) REFERENCES timeseries.catalog(ts_key)
}

COMMIT;
