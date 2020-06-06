#' Title
#'
#' @param con
#' @param ts_keys
#' @param valid_on
#' @param regex
#' @param respect_release_date
#' @param schema
#' @param chunksize
#'
#' @return
#' @import data.table
#' @importFrom DBI dbHasCompleted dbQuoteIdentifier Id
#' @importFrom RPostgres dbSendQuery dbFetch dbClearResult dbQuoteLiteral
#' @export
#'
#' @examples
read_time_series <- function(con,
                             ts_keys,
                             valid_on = NA,
                             regex = FALSE,
                             respect_release_date = FALSE,
                             schema = "timeseries",
                             chunksize = 10000) {

  # timeseriesdb makes use of a temporary table that is joined against
  # to get the right data. This is much faster than WHERE clauses.
  # Populate said table with keys
  db_tmp_read(
    con,
    ts_keys,
    regex,
    schema
  )

  res <- dbSendQuery(con, sprintf("select * from %sread_ts_raw(%s, %s)",
                                  dbQuoteIdentifier(con, Id(schema = schema)),
                                  dbQuoteLiteral(con, valid_on),
                                  dbQuoteLiteral(con, respect_release_date)))

  tsl <- list()

  while(!dbHasCompleted(res)) {
    chunk <- data.table(dbFetch(res, n = chunksize))

    tsl[chunk[, ts_key]] <- chunk[, .(ts_obj = list(json_to_ts(ts_data))), by = ts_key]$ts_obj
  }
  dbClearResult(res)

  class(tsl) <- c("tslist", "list")

  tsl
}
