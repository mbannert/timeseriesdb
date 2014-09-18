#' Fetch Selected Results from Database
#' 
#' This function uses result sets selections to read multiple
#' time series from a database. 
#' 
#' @author Matthias Bannert
#' @param result_set object of class rs
#' @param connect character name of the PostgreSQL connection object, defaults to 'con'.
#' @param tbl character variable indicating the name the timeseries table in the databse.
#' @param envir environment to assign fetched time series to. If not specified a list of time series is returned. Defaults to NULL.
#' Defaults to timeseries_main.
#' 'con' in the global environment.
#' @export
fetchSelectedResults <- function(result_set,connect = "con", envir = NULL,
                                 tbl = "timeseries_main"){
  # Because we cannot really use a global binding to 
  # the postgreSQL connection object which does not exist at the time
  # of compilation, we use the character name of the object here. 
  connect <- get(connect)
  # sanity check because we can't read if nothing is selected
  if(is.null(result_set$selection)) stop("No series selected. Use selectResult to select at least one series.")
  
  selection <- result_set$ts_keys[result_set$selection]
  
  # issue a list or assign series to environment
  if(is.null(envir)){
    lapply(selection,readTimeseries,connect = connect)  
  } else {
    lapply(selection,function(x) {
      tmp_ts <- readTimeseries(x,connect = connect,tbl = tbl)
      assign(x,tmp_ts,envir = envir)
    })
  }
}