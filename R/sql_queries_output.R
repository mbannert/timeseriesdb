query_populate_ts_read_regex <- function(schema,
                                         pattern,
                                         valid_on,
                                         respect_release_date) {
  if(respect_release_date) {
    sprintf("
            CREATE TEMPORARY TABLE ts_read AS(
            SELECT ts_key, ts_validity FROM %s.releases
            JOIN %s.timeseries_main
            ON timeseries_main.ts_key ~ '%s'
            AND releases.release_validity @> '%s'::timestamptz
            AND timeseries_main.release = releases.release
            AND timeseries_main.ts_validity = releases.ts_validity)",
            schema,
            schema,
            pattern,
            valid_on)
  } else {
    sprintf("
            CREATE TEMPORARY TABLE ts_read AS(
            SELECT ts_key, ts_validity FROM %s.timeseries_main
            WHERE ts_key ~ '%s'
            AND ts_validity @> '%s'::date)",
            schema,
            pattern,
            valid_on)
  }
}

query_update_ts_read <- function(schema,
                                 valid_on,
                                 respect_release_date) {
  if(respect_release_date) {
    sprintf("
              UPDATE ts_read
              SET ts_validity = (
                SELECT releases.ts_validity FROM %s.timeseries_main
                JOIN %s.releases
                ON releases.release = timeseries_main.release
                AND releases.release_validity @> '%s'::timestamptz
                AND ts_read.ts_key = timeseries_main.ts_key
                AND releases.ts_validity = timeseries_main.ts_validity)",
            schema,
            schema,
            valid_on)
  } else {
    sprintf("
          UPDATE ts_read
          SET ts_validity = (
            SELECT ts_validity FROM %s.timeseries_main
            WHERE ts_read.ts_key = timeseries_main.ts_key
            AND timeseries_main.ts_validity @> '%s'::date)",
            schema,
            valid_on)
  }
}

query_select_time_series <- function(schema) {
  sprintf("
          SELECT timeseries_main.ts_key, ts_data FROM ts_read
          JOIN %s.timeseries_main
          ON ts_read.ts_key = timeseries_main.ts_key
          AND ts_read.ts_validity = timeseries_main.ts_validity",
          schema)
}