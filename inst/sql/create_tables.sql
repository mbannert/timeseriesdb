CREATE SCHEMA timeseries;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE TABLE timeseries.timeseries_main(
  id UUID NOT NULL DEFAULT uuid_generate_v1() PRIMARY KEY,
  ts_key text, [ref: > series.ts_key]
  validity daterange,
  coverage daterange,
  release_date timestamptz DEFAULT CURRENT_TIMESTAMP, -- users are expected to give release time plus tz by its name
  ts_data json,
  UNIQUE (ts_key, ts_validity),
  FOREIGN KEY (ts_key) REFERENCES timeseries.timeseries_catalog(ts_key),
  EXCLUDE USING GIST (ts_key WITH =, validity WITH &&)
)


CREATE TABLE timeseries.timeseries_catalog(
    ts_key text PRIMARY KEY,
    set_id text,
    created_by text,
    created_at timestamptz DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (set_id) REFERENCES timeseries.datasets(set_id)
)

CREATE TABLE timeseries.datasets(
    set_id text PRIMARY KEY,
    set_md json
)

CREATE TABLE timeseries.md_local_ts {
  ts_key text,
  lang text,
  data_desc json,
  PRIMARY KEY(ts_key, lang),
  FOREIGN KEY (ts_key) REFERENCES timeseries.catalog(ts_key)
}


CREATE TABLE timeseries.md_local_vintages{
  vintage_key text,
  lang text, 
  meta_data json,
  PRIMARY KEY(vintage_key, lang),
  FOREIGN KEY (vintage_key) REFERENCES timeseries.vintages(vintage_key)
}


CREATE TABLE timeseries.md_vintages{
  vintage_key text,
  meta_data json,
  PRIMARY KEY(vintage_key),
  FOREIGN KEY (vintage_key) REFERENCES timeseries.vintages(vintage_key)
}

CREATE TABLE timeseries.collections {
  id UUID NOT NULL DEFAULT uuid_generate_v1() PRIMARY KEY,
  name text,
  owner text, 
  description text,
  UNIQUE (name, owner)
}

CREATE TABLE timeseries.collect_catalog {
  id UUID, 
  ts_key text, 
  PRIMARY KEY (id,ts_key),
  FOREIGN KEY (id) REFERENCES timeseries.collections(id),
  FOREIGN KEY (ts_key) REFERENCES timeseries.catalog(ts_key)
}

















