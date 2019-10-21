#' Create a query to populate a temporary "ts_read" table with ts_keys matching pattern
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
                                         valid_on) {
  sprintf("
          CREATE TEMPORARY TABLE ts_read_keys AS(
            SELECT ts_key FROM %s
            WHERE ts_key ~ %s)",
          # Since all meta tables should be FKd to ts_main this should be ts_fine
          dbQuoteIdentifier(con, Id(schema = schema, table = "timeseries_main")), 
          dbQuoteLiteral(con, pattern))

  # if(respect_release_date) {
  #   sprintf("
  #           CREATE TEMPORARY TABLE ts_read AS(
  #           SELECT ts_key, ts_validity, release_validity FROM %s    -- schema.releases
  #           JOIN %s                                                 -- schema.timeseries_main
  #           ON timeseries_main.ts_key ~ %s                          -- pattern
  #           AND releases.release_validity @> '%s'::timestamptz      -- valid_on
  #           AND timeseries_main.release = releases.release
  #           AND timeseries_main.ts_validity = releases.ts_validity)",
  #           dbQuoteIdentifier(con, Id(schema = schema, table = "releases")),
  #           dbQuoteIdentifier(con, Id(schema = schema, table = "timeseries_main")),
  #           dbQuoteLiteral(con, pattern),
  #           dbQuoteLiteral(con, valid_on))
  # } else {
  #   sprintf("
  #           CREATE TEMPORARY TABLE ts_read AS(
  #           SELECT DISTINCT ON (ts_key)
  #             ts_key, ts_validity, release_validity FROM %s    -- schema.timeseries_main
  #           WHERE ts_key ~ %s                                  -- pattern
  #           AND ts_validity @> %s)                             -- valid_on
  #           ORDER BY ts_key, release_validity DESC",
  #           dbQuoteIdentifier(con, Id(schema = schema, table = "timeseries_main")),
  #           dbQuoteLiteral(con, pattern),
  #           dbQuoteLiteral(con, valid_on)
  # }
}

# https://stackoverflow.com/questions/24042359/how-to-join-only-one-row-in-joined-table-with-postgres
query_populate_ts_read <- function(con,
                                    schema,
                                    table,
                                    valid_on,
                                    respect_release_date) {
  if(respect_release_date) {
    sprintf("CREATE TEMPORARY TABLE ts_read AS(
              SELECT ts_read_keys.ts_key, ts_validity, release_validity FROM %s
              JOIN ts_read_keys
              ON %s.ts_key = ts_read_keys.ts_key
              AND %s.release_validity @> now()
              AND %s.ts_validity @> %s)",
            dbQuoteIdentifier(con, Id(schema = schema, table = table)),
            dbQuoteIdentifier(con, Id(table = table)),
            dbQuoteIdentifier(con, Id(table = table)),
            dbQuoteIdentifier(con, Id(table = table)),
            dbQuoteLiteral(con, valid_on))
  } else {
    sprintf("CREATE TEMPORARY TABLE ts_read AS(
              SELECT DISTINCT ON(ts_key)
                ts_read_keys.ts_key, ts_validity, release_validity FROM %s
              JOIN ts_read_keys
              ON %s.ts_key = ts_read_keys.ts_key
              AND %s.ts_validity @> %s)
              ORDER BY ts_key, release_validity DESC",
            dbQuoteIdentifier(con, Id(schema = schema, table = table)),
            dbQuoteIdentifier(con, Id(table = table)),
            dbQuoteIdentifier(con, Id(table = table)),
            dbQuoteLiteral(con, valid_on))
  }
}

#' #' Create a query to set proper ts_validity on a ts_read that has already been populated with ts_keys
#' #' If release date does not matter, pick the one where ts_validity contains valid_on
#' #' If release date is to be respected, pick the one
#' #' 
#' #' 
#' #' @param con 
#' #'
#' #' @param schema 
#' #' @param valid_on 
#' #' @param respect_release_date 
#' #'
#' #' @importFrom RPostgres Id dbQuoteIdentifier
#' query_update_ts_read <- function(con,
#'                                  schema,
#'                                  valid_on,
#'                                  respect_release_date) {
#'   
#'   tbl <- "timeseries_main"
#'   valid_on <- dbQuoteLiteral(con, valid_on)
#'   schema_table <- dbQuoteIdentifier(con, Id(schema = schema, table = tbl))
#'   
#'   if(respect_release_date) {
#'     sprintf("
#'               UPDATE ts_read AS tsr
#'               SET ts_validity = tsm.ts_validity,
#'                 release_validity = tsm.release_validity
#'               FROM %s as tsm
#'               WHERE tsm.release_validity @> now()
#'                 AND tsm.ts_validity @> %s
#'                 AND tsr.ts_key = tsm.ts_key",
#'             schema_table,
#'             valid_on)
#'   } else {
#'     sprintf("
#'           UPDATE ts_read as tsr
#'           SET ts_validity = tsm.ts_validity,
#'             release_validity = tsm.release_validity
#'           FROM %s as tsm
#'           WHERE tsm.ts_validity @> %s,
#'             AND ???, -- most recent one
#'             AND tsr.ts_key = tsm.ts_key",
#'           schema_table,
#'           valid_on)
#'   }
#' }

#' @importFrom RPostgres Id dbQuoteIdentifier
query_select_time_series <- function(con,
                                     schema) {
  sprintf("
          SELECT timeseries_main.ts_key, ts_data FROM ts_read
          JOIN %s
          ON ts_read.ts_key = timeseries_main.ts_key
          AND ts_read.ts_validity = timeseries_main.ts_validity
          AND ts_read.release_validity = timeseries_main.release_validity",
          dbQuoteIdentifier(con, Id(schema = schema, table = "timeseries_main")))
}