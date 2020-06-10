CREATE ROLE dev_reader_public LOGIN PASSWORD 'dev_reader_public';
GRANT timeseries_access_public TO dev_reader_public;
GRANT timeseries_reader TO dev_reader_public;
GRANT USAGE ON SCHEMA timeseries to dev_reader_public;


CREATE ROLE dev_reader_main LOGIN PASSWORD 'dev_reader_main';
GRANT timeseries_access_main TO dev_reader_main;
GRANT timeseries_reader TO dev_reader_main;
GRANT USAGE ON SCHEMA timeseries to dev_reader_main;

CREATE ROLE dev_writer LOGIN PASSWORD 'dev_writer';
GRANT timeseries_writer TO dev_writer;
GRANT USAGE ON SCHEMA timeseries to dev_writer;

INSERT INTO timeseries.access_levels VALUES ('timeseries_access_public', 'Publicly available time series');
INSERT INTO timeseries.access_levels VALUES ('timeseries_access_main', 'Non-public time series without license restrictions', true);
INSERT INTO timeseries.access_levels VALUES ('timeseries_access_restricted', 'License restricted time series');
