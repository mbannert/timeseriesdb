#' Read Time Series From PostgreSQL into R
#'
#' Read specific version of a time series given time series key (unique identifier) and validity. By default, this function returns the most recent version of a time series. 
#'
#'
#' @param con RPostgres connection object. 
#' @param ts_keys character vector containing time series keys which identify a time series uniquely. 
#' @param valid_on character representing a date of the form YYYY-MM-DD.
#' @param regex boolean should ts_keys be interpreted as regular expression? Defaults to FALSE.
#' @param respect_release_date boolean Should the release embargo of a time series be respected? Defaults to FALSE. This option makes sense when the function is used in an API in that sense that users do not have direct access to this function and therefore cannot simply switch barameters. 
#' @param schema character name of the schema 
#' @param chunksize
#'
#' @return list of time series. List elements vary depending on nature of time series, i.e., regular vs. irregular time series. 
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
