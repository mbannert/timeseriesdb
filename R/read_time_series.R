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
#' @importFrom RPostgres dbSendQuery dbFetch dbClearResult
#' @export
#'
#' @examples
read_time_series <- function(con,
                             ts_keys, 
                             valid_on = Sys.Date(),
                             regex = FALSE,
                             respect_release_date = FALSE,
                             schema = "timeseries",
                             chunksize = 10000) {
  if(regex) {
    if(length(ts_keys) > 1) {
      warning("regex = TRUE but length of ts_keys > 1, using only first element as pattern!")
    }
  }
  
  # timeseriesdb makes use of a temporary table that is joined against
  # to get the right data. This is much faster than WHERE clauses.
  n_to_read <- db_populate_ts_read(
    con,
    ts_keys,
    regex,
    schema,
    valid_on,
    respect_release_date
  )

  if(n_to_read == 0) {
    if(regex) {
      warning(sprintf("No series found matching '%s'!", ts_keys[1]))
      return(list())
    } else {
      warning("No series matching criteria found.")
      return(list())
    }
  }
  
  res <- dbSendQuery(con,
    query_select_time_series(con,
                             schema))

  while(!dbHasCompleted(res)) {
    chunk <- data.table(dbFetch(res, n = chunksize))
    
    tsl <- chunk[, .(ts_obj = list(json_to_ts(ts_data))), by = ts_key]$ts_obj
    names(tsl) <- chunk[, ts_key]
  }
  dbClearResult(res)
  
  tsl
}