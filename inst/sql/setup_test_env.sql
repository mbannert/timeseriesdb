CREATE ROLE dev_reader_public;
GRANT tsdb_access_public TO dev_reader_public;
GRANT tsdb_reader TO dev_reader_public;

CREATE ROLE dev_writer_public;
GRANT tsdb_access_public TO dev_reader_public;
GRANT tsdb_writer TO dev_reader_public;

CREATE ROLE dev_reader_private;
GRANT tsdb_access_private TO dev_reader_private;
GRANT tsdb_reader TO dev_reader_private;

CREATE ROLE dev_writer_private;
GRANT tsdb_access_private TO dev_writer_private;
GRANT tsdb_writer TO dev_writer_private;

CREATE ROLE dev_reader_restricted;
GRANT tsdb_access_restricted TO dev_reader_restricted;
GRANT tsdb_reader TO dev_reader_restricted;

CREATE ROLE dev_writer_restricted;
GRANT tsdb_access_public TO dev_writer_restricted;
GRANT tsdb_reader TO dev_reader_public
