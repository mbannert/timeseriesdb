CREATE ROLE tsdb_admin NOLOGIN;
CREATE ROLE tsdb_reader NOLOGIN;
CREATE ROLE tsdb_writer NOLOGIN;

CREATE ROLE tsdb_access_public;
CREATE ROLE tsdb_access_private;
CREATE ROLE tsdb_access_restricted;

GRANT tsdb_reader TO tsdb_writer;

GRANT tsdb_access_public TO tsdb_admin;      -- public/private distinction does
GRANT tsdb_access_private TO tsdb_admin;     -- not really make sense for admins?
GRANT tsdb_access_restricted TO tsdb_admin;
GRANT tsdb_writer TO tsdb_admin;



