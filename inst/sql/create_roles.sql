CREATE ROLE tsdb_admin NOLOGIN;
CREATE ROLE tsdb_reader NOLOGIN;
CREATE ROLE tsdb_writer NOLOGIN;

CREATE ROLE tsdb_access_public;
CREATE ROLE tsdb_access_private;
CREATE ROLE tsdb_access_restricted;

GRANT tsdb_reader TO tsdb_writer;
GRANT tsdb_writer TO tsdb_admin;



