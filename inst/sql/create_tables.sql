CREATE TABLE timeseries.datasets(
    set_id TEXT PRIMARY KEY,
    set_description TEXT,
    set_md JSON
);

INSERT INTO timeseries.datasets VALUES ('default', 'A set that is used if no other set is specified. Every time series needs to be part of a dataset', NULL);

CREATE TABLE timeseries.access_levels (
  role TEXT PRIMARY KEY,
  description TEXT,
  is_default BOOLEAN DEFAULT NULL
);

CREATE UNIQUE INDEX ON timeseries.access_levels(is_default) WHERE is_default = true;

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
  FOREIGN KEY (ts_key) REFERENCES timeseries.catalog(ts_key) ON DELETE CASCADE,
  FOREIGN KEY (access) REFERENCES timeseries.access_levels(role)
);

CREATE TABLE timeseries.metadata(
  id UUID NOT NULL DEFAULT uuid_generate_v1() PRIMARY KEY,
  ts_key TEXT NOT NULL,
  validity DATE NOT NULL DEFAULT CURRENT_DATE,
  created_by TEXT NOT NULL DEFAULT CURRENT_USER,
  created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  metadata JSONB,
  UNIQUE (ts_key, validity),
  FOREIGN KEY (ts_key) REFERENCES timeseries.catalog(ts_key) ON DELETE CASCADE
);

CREATE TABLE timeseries.metadata_localized(
  id UUID NOT NULL DEFAULT uuid_generate_v1() PRIMARY KEY,
  ts_key TEXT NOT NULL,
  locale TEXT NOT NULL,
  validity DATE NOT NULL DEFAULT CURRENT_DATE,
  created_by TEXT NOT NULL DEFAULT CURRENT_USER,
  created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  metadata JSONB,
  UNIQUE (ts_key, locale, validity),
  FOREIGN KEY (ts_key) REFERENCES timeseries.catalog(ts_key) ON DELETE CASCADE
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
  FOREIGN KEY (id) REFERENCES timeseries.collections(id) ON DELETE CASCADE,
  FOREIGN KEY (ts_key) REFERENCES timeseries.catalog(ts_key) ON DELETE CASCADE
);
