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
query_populate_ts_read_keys_regex <- function(con,
                                         schema,
                                         pattern) {
  sprintf("
          CREATE TEMPORARY TABLE ts_read_keys AS(
            SELECT ts_key FROM %s
            WHERE ts_key ~ %s)",
          # Since all meta tables should be FKd to ts_main this should be ts_fine
          dbQuoteIdentifier(con, Id(schema = schema, table = "timeseries_main")), 
          dbQuoteLiteral(con, pattern))
}

# https://stackoverflow.com/questions/24042359/how-to-join-only-one-row-in-joined-table-with-postgres
#' @importFrom RPostgres Id dbQuoteIdentifier
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
              AND %s.release_validity @> '%s'::timestamptz
              AND NOT lower(%s.release_validity) >= now())",
            dbQuoteIdentifier(con, Id(schema = schema, table = table)),
            dbQuoteIdentifier(con, Id(table = table)),
            dbQuoteIdentifier(con, Id(table = table)),
            valid_on,
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