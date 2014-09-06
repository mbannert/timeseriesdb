#' Read Time Series From PostgreSQL database
#' 
#' This function reads a time series from a postgres database,
#' that key value pair storage (hstore), to archive time series.
#' After reading the information from the database a standard
#' R time series object of class 'ts' is built and returned. 
#' 
#' @author Matthias Bannert
#' @param series character representation of the key of the time series
#' @param connect a connection object, defaults to looking for a connection object called con. 
#' @param tbl character string denoting the name of the main time series table in the PostgreSQL database.
#' @examples
#' ts1 <- ts(rnorm(100),start = c(1990,1),frequency = 4)
#' storeTimeseries(ts1)
#' readTimeseries("ts1")
readTimeseries <- function(series,connect = con,tbl = "timeseries_main"){
  # extract data from hstore 
  sql_statement_data <- sprintf("SELECT (each(ts_data)).key, (each(ts_data)).value FROM %s WHERE ts_key = '%s'",tbl,series)
  # get freq
  sql_statement_freq <- sprintf("SELECT ts_frequency FROM %s WHERE ts_key = '%s'",tbl,series)
  freq <- dbGetQuery(connect,sql_statement_freq)
  out <- dbGetQuery(connect,sql_statement_data)
  
  # create R time series object
  # find start date first
  out$key <- as.Date(out$key)
  start_date <- min(out$key)
  year <- as.numeric(format(as.Date(start_date), "%Y")) 
  period <- as.numeric(format(as.Date(start_date), "%m"))
  # return a time series object
  ts(as.numeric(out$value),
     start =c(year,period),
     frequency = as.numeric(freq))
  
  
}

