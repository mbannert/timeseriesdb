#' Delete Time Series from the database
#' 
#' This function deletes time series AND their metainformation from the 
#' database. All meta information in all series will be deleted. 
#' To only edit the original time series use \code{\link{storeTimeSeries}}
#' to overwrite existing series. 
#' 
#' @param series character name of the timeseries
#' @param con a PostgreSQL connection object
#' @param chunksize integer max size of chunk when deleting chunkwise. Defaults 
#' to 10000.
#' @param tbl_main character name of the table that contains the 
#' main time series catalog. Defaults to 'timeseries_main'.
#' @param schema SQL schema name. Defaults to 'timeseries'.
#' @export
deleteTimeSeries <- function(con,
                             series,
                             chunksize = 10000,
                             tbl_main = 'timeseries_main',
                             schema = 'timeseries'){
  if(is.character(con)) {
    warning("You are using this function in a deprecated fashion. Use deleteTimeSeries(con, series, ...) in the future.")
    t <- con
    con <- series
    series <- t
  }
  
  s <- split(series,(seq(length(series))-1) %/% chunksize)
  lapply(s,function(x, tbl, schema){
    keys <- paste(x,collapse = "','")  
    del_statement <- sprintf("DELETE FROM %s.%s WHERE ts_key IN ('%s')",
                             schema, tbl, keys)
    out <- dbGetQuery(con,del_statement)
  },tbl = tbl_main, schema = schema)
  
  message(sprintf("DELETE operations in %s chunk(s) performed.",length(s)))
  
}
