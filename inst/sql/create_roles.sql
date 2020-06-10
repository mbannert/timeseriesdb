CREATE ROLE timeseries_reader NOLOGIN;
CREATE ROLE timeseries_writer NOLOGIN;
GRANT timeseries_reader TO timeseries_writer;

CREATE ROLE timeseries_access_public;
CREATE ROLE timeseries_access_main;
CREATE ROLE timeseries_access_restricted;
GRANT timeseries_access_public TO timeseries_access_main;
GRANT timeseries_access_main to timeseries_access_restricted;

CREATE ROLE timeseries_admin NOLOGIN;
GRANT timeseries_writer TO timeseries_admin;
GRANT timeseries_access_restricted TO timeseries_admin;
