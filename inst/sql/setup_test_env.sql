CREATE ROLE dev_reader_public;
GRANT timeseries_access_public TO dev_reader_public;
GRANT timeseries_reader TO dev_reader_public;

CREATE ROLE dev_writer_public;
GRANT timeseries_access_public TO dev_writer_public;
GRANT timeseries_writer TO dev_writer_public;

CREATE ROLE dev_reader_main;
GRANT timeseries_access_main TO dev_reader_main;
GRANT timeseries_reader TO dev_reader_main;

CREATE ROLE dev_writer_main;
GRANT timeseries_access_main TO dev_writer_main;
GRANT timeseries_writer TO dev_writer_main;

CREATE ROLE dev_admin WITH LOGIN PASSWORD 'dev_admin'; -- public/private distinction does not really make sense for admins
GRANT timeseries_admin TO dev_admin;
GRANT CREATE, USAGE ON SCHEMA timeseries TO dev_admin;
