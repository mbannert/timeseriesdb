CREATE ROLE timeseries_admin NOLOGIN;
CREATE ROLE timeseries_reader NOLOGIN;
CREATE ROLE timeseries_writer NOLOGIN;

CREATE ROLE timeseries_access_public;
CREATE ROLE timeseries_access_main;
CREATE ROLE timeseries_access_restricted;

GRANT timeseries_reader TO timeseries_writer;
GRANT timeseries_writer TO timeseries_admin;
