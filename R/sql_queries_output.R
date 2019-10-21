#' @importFrom RPostgres Id dbQuoteIdentifier
query_populate_ts_read_regex <- function(con,
                                         schema,
                                         pattern,
                                         valid_on,
                                         respect_release_date) {
  if(respect_release_date) {
    sprintf("
            CREATE TEMPORARY TABLE ts_read AS(
            SELECT ts_key, ts_validity FROM %s
            JOIN %s
            ON timeseries_main.ts_key ~ %s
            AND releases.release_validity @> '%s'::timestamptz
            AND timeseries_main.release = releases.release
            AND timeseries_main.ts_validity = releases.ts_validity)",
            dbQuoteIdentifier(con, Id(schema = schema, table = "releases")),
            dbQuoteIdentifier(con, Id(schema = schema, table = "timeseries_main")),
            dbQuoteLiteral(con, pattern),
            valid_on) # this is OK (for now) as dbQuoteLiteral makes a ::timestamp and valid_on is processed before this point
  } else {
    sprintf("
            CREATE TEMPORARY TABLE ts_read AS(
            SELECT ts_key, ts_validity FROM %s
            WHERE ts_key ~ %s
            AND ts_validity @> '%s'::date)",
            dbQuoteIdentifier(con, Id(schema = schema, table = "timeseries_main")),
            dbQuoteLiteral(con, pattern),
            valid_on)
  }
}

#' @importFrom RPostgres Id dbQuoteIdentifier
query_update_ts_read <- function(con,
                                 schema,
                                 valid_on,
                                 respect_release_date) {
  if(respect_release_date) {
    sprintf("
              UPDATE ts_read
              SET ts_validity = (
                SELECT releases.ts_validity FROM %s
                JOIN %s
                ON releases.release = timeseries_main.release
                AND releases.release_validity @> '%s'::timestamptz
                AND ts_read.ts_key = timeseries_main.ts_key
                AND releases.ts_validity = timeseries_main.ts_validity)",
            dbQuoteIdentifier(con, Id(schema = schema, table = "timeseries_main")),
            dbQuoteIdentifier(con, Id(schema = schema, table = "releases")),
            valid_on)
  } else {
    sprintf("
          UPDATE ts_read
          SET ts_validity = (
            SELECT ts_validity FROM %s
            WHERE ts_read.ts_key = timeseries_main.ts_key
            AND timeseries_main.ts_validity @> '%s'::date)",
            dbQuoteIdentifier(con, Id(schema = schema, table = "timeseries_main")),
            valid_on)
  }
}

#' @importFrom RPostgres Id dbQuoteIdentifier
query_select_time_series <- function(con,
                                     schema) {
  sprintf("
          SELECT timeseries_main.ts_key, ts_data FROM ts_read
          JOIN %s
          ON ts_read.ts_key = timeseries_main.ts_key
          AND ts_read.ts_validity = timeseries_main.ts_validity",
          dbQuoteIdentifier(con, Id(schema = "schema", table = "timeseries_main")))
}