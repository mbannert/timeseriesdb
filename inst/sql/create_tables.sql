CREATE TABLE timeseries.datasets(
    set_id TEXT PRIMARY KEY,
    set_description TEXT,
    set_md JSON
);

INSERT INTO timeseries.datasets VALUES ('default', 'A set that is used if no other set is specified. Every time series needs to be part of a dataset', NULL);

CREATE TABLE timeseries.catalog(
    ts_key TEXT PRIMARY KEY,
    set_id TEXT DEFAULT 'default',
    FOREIGN KEY (set_id) REFERENCES timeseries.datasets(set_id) ON DELETE CASCADE
);

CREATE TABLE timeseries.timeseries_main(
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
  FOREIGN KEY (ts_key) REFERENCES timeseries.catalog(ts_key) ON DELETE CASCADE
);

CREATE TABLE timeseries.md_local_ts (
  ts_key TEXT,
  lang TEXT,
  data_desc JSON,
  PRIMARY KEY(ts_key, lang),
  FOREIGN KEY (ts_key) REFERENCES timeseries.catalog(ts_key)
);


CREATE TABLE timeseries.md_local_vintages (
  vintage_id UUID,
  lang TEXT,
  meta_data JSON,
  PRIMARY KEY(vintage_id, lang),
  FOREIGN KEY (vintage_id) REFERENCES timeseries.timeseries_main(id)
);


CREATE TABLE timeseries.md_vintages (
  vintage_id UUID,
  meta_data JSON,
  PRIMARY KEY(vintage_id),
  FOREIGN KEY (vintage_id) REFERENCES timeseries.timeseries_main(id)
);

CREATE TABLE timeseries.collections (
  id UUID NOT NULL DEFAULT uuid_generate_v1() PRIMARY KEY,
  name TEXT,
  owner TEXT, 
  description TEXT,
  UNIQUE (name, owner)
);

CREATE TABLE timeseries.collect_catalog (
  id UUID, 
  ts_key TEXT,
  PRIMARY KEY (id,ts_key),
  FOREIGN KEY (id) REFERENCES timeseries.collections(id),
  FOREIGN KEY (ts_key) REFERENCES timeseries.catalog(ts_key)
);

