#' Populate temporary ts_updates table with records
#' first properly formatting validity ranges
#'
#' @param con
#' @param records
#' @param valid_from
#' @param release_date
#' @param access
#' @param schema
#' @importFrom RPostgres dbWriteTable
db_tmp_store <- function(con,
                         records) {

  # TODO: add mechanism for setting column names (for e.g. metadata)
  dt <- data.table(
    ts_key = names(records),
    ts_data = unlist(records)
  )

  dbWriteTable(con,
               "tmp_ts_updates",
               dt,
               temporary = TRUE,
               overwrite = TRUE,
               field.types = c(
                 ts_key = "text",
                 ts_data = "json"
               )
  )
}


#' Create and populate a temporary table ts_read with desired (ts_key, ts_validity) pairs for
#' joining against timeseries_main and reading.
#'
#' if regex == TRUE the first entry of ts_keys will be used as the pattern
#'
#' @param con
#' @param ts_keys
#' @param regex
#' @param schema
#' @param table
#' @param valid_on
#' @param respect_release_date
#' @importFrom RPostgres dbExecute dbWriteTable
db_tmp_read <- function(con,
                        ts_keys,
                        regex,
                        schema) {
  if(regex) {
    if(length(ts_keys) > 1) {
      warning("regex = TRUE but length of ts_keys > 1, using only first element as pattern!")
    }
  }

  if(regex) {
    dbExecute(con,
              sprintf("SELECT 1 FROM %screate_read_tmp_regex(%s)",
                      dbQuoteIdentifier(con, Id(schema = schema)),
                      dbQuoteLiteral(con, ts_keys[1])))
  } else {
    dt <- data.table(
      ts_key = ts_keys
    )

    dbWriteTable(con,
                 "tmp_ts_read_keys",
                 dt,
                 temporary = TRUE, # Praise be for this parameter!
                 overwrite = TRUE,
                 field.types = c(
                   ts_key = "text"
                 )
    )
  }
}
