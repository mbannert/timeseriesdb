CREATE TABLE timeseries.quarterly (
  ts_key text,
  time text,
  value text,
  primary key (ts_key, time)
);


---------


CREATE TABLE ts_data (
  time TIMESTAMPTZ NOT NULL,
  ts_key text NOT NULL,
  data DOUBLE PRECISION NULL
);


INSERT INTO ts_data VALUES ('1985-01-01','somekey',20)
