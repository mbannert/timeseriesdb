# TODO: these are outdated. rewrite kthxbai.

#' Create a query to populate a temporary "ts_read" table with ts_keys matching pattern
#' and optionally excluding those which are not available yet due to a release date
#' 
#' This is the one to be used when reading ts based on regex pattern
#' 
#' @param con 
#'
#' @param schema 
#' @param pattern 
#' @param valid_on 
#' @param respect_release_date 
#'
#' @importFrom RPostgres Id dbQuoteIdentifier
query_populate_ts_read_regex <- function(con,
                                         schema,
                                         pattern,
                                         valid_on,
                                         respect_release_date) {
  if(respect_release_date) {
    sprintf("
            CREATE TEMPORARY TABLE ts_read AS(
            SELECT ts_key, ts_validity FROM %s                      -- schema.releases
            JOIN %s                                                 -- schema.timeseries_main
            ON timeseries_main.ts_key ~ %s                          -- pattern
            AND releases.release_validity @> '%s'::timestamptz      -- valid_on
            AND timeseries_main.release = releases.release
            AND timeseries_main.ts_validity = releases.ts_validity)",
            dbQuoteIdentifier(con, Id(schema = schema, table = "releases")),
            dbQuoteIdentifier(con, Id(schema = schema, table = "timeseries_main")),
            dbQuoteLiteral(con, pattern),
            valid_on) # this is OK (for now) as dbQuoteLiteral makes a ::timestamp and valid_on is processed before this point
  } else {
    sprintf("
            CREATE TEMPORARY TABLE ts_read AS(
            SELECT ts_key, ts_validity FROM %s                      -- schema.timeseries_main
            WHERE ts_key ~ %s                                       -- pattern
            AND ts_validity @> '%s'::date)                          -- valid_on",
            dbQuoteIdentifier(con, Id(schema = schema, table = "timeseries_main")),
            dbQuoteLiteral(con, pattern),
            valid_on)
  }
}

#' Create a query to set proper ts_validity on a ts_read that has already been populated with ts_keys
#' If release date does not matter, pick the one where ts_validity contains valid_on
#' If release date is to be respected, pick the one
#' 
#' 
#' @param con 
#'
#' @param schema 
#' @param valid_on 
#' @param respect_release_date 
#'
#' @importFrom RPostgres Id dbQuoteIdentifier
query_update_ts_read <- function(con,
                                 schema,
                                 valid_on,
                                 respect_release_date) {
  if(respect_release_date) {
    sprintf("
              UPDATE ts_read
              SET ts_validity = (
                SELECT ts_validity FROM %s
                WHERE release_validity @> '%s'::timestamptz
                AND ts_read.ts_key = timeseries_main.ts_key)",
            dbQuoteIdentifier(con, Id(schema = schema, table = "timeseries_main")),
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
          dbQuoteIdentifier(con, Id(schema = schema, table = "timeseries_main")))
}