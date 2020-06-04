CREATE ROLE dev_reader_public;
GRANT tsdb_access_public TO dev_reader_public;
GRANT tsdb_reader TO dev_reader_public;

CREATE ROLE dev_writer_public;
GRANT tsdb_access_public TO dev_writer_public;
GRANT tsdb_writer TO dev_writer_public;

CREATE ROLE dev_reader_private;
GRANT tsdb_access_private TO dev_reader_private;
GRANT tsdb_reader TO dev_reader_private;

CREATE ROLE dev_writer_private;
GRANT tsdb_access_private TO dev_writer_private;
GRANT tsdb_writer TO dev_writer_private;

CREATE ROLE dev_admin WITH LOGIN; -- public/private distinction does not really make sense for admins
GRANT tsdb_admin TO dev_admin;
GRANT CREATE, USAGE ON SCHEMA timeseries TO dev_admin;
