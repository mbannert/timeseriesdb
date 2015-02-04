#' Read Time Series From PostgreSQL database
#' 
#' This function reads a time series from a postgres database,
#' that key value pair storage (hstore), to archive time series.
#' After reading the information from the database a standard
#' R time series object of class 'ts' is built and returned. 
#' 
#' @author Matthias Bannert, Gabriel Bucur
#' @param series character representation of the key of the time series
#' @param con a PostgreSQL connection object
#' @param tbl character string denoting the name of the main time series table
#' in the PostgreSQL database.
#' @export
readTimeSeries <- function(series,con = options()$TIMESERIESDB_CON,
                           tbl = "timeseries_main"){
  
  if(is.null(con)) stop('Default TIMESERIESDB_CON not set in options() or no proper connection given to the con argument.')
  
  
  # Because we cannot really use a global binding to 
  # the postgreSQL connection object which does not exist at the time
  # of compilation, we use the character name of the object here. 
  
  # extract data from hstore 
  sql_statement_data <- sprintf("SELECT ((each(ts_data)).key)::date,
                                (each(ts_data)).value FROM %s WHERE ts_key = '%s'",tbl,series)
  # get freq
  sql_statement_freq <- sprintf("SELECT ts_frequency FROM %s WHERE ts_key = '%s'",tbl,series)
  freq <- DBI::dbGetQuery(con,sql_statement_freq)
  out <- DBI::dbGetQuery(con,sql_statement_data)
  
  # create R time series object
  # find start date first
  out$key <- as.Date(out$key)
  start_date <- min(out$key)
  year <- as.numeric(format(as.Date(start_date), "%Y"))
  
  # check whether it's quarterly or monthly time series 
  # in order to return a proper period when converting it to a YYYY-mm-dd format.
  if (freq == 4){
    period <- (as.numeric(format(as.Date(start_date), "%m")) -1) / 3 + 1
   } else if(freq == 12) {
    period <- as.numeric(format(as.Date(start_date), "%m"))
   } else {
    stop("Not a standard frequency.")
   }
   
  # return a time series object
  ts(as.numeric(out$value),
     start =c(year,period),
     frequency = as.numeric(freq))
  
  
}

