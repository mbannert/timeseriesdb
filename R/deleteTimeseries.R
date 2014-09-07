#' Delete Time Series from Database
#' 
#' This function deletes a time series from the database. 
#' It also has a rollback feature which stores a series to an R environment and 
#' keeps the deleted time series in memory as long as the R Session is alive or the 
#' environment is manually deleted. Note that your database user needs to rights to 
#' delete records from the database if this function should work. 
#' 
#' @author Matthias Bannert
#' @param series character name of the series that should be deleted
#' @param connect character name of the PostgreSQL connection object
#' @param rollback logical if TRUE the rollback functionality is enabled. Defaults
#' to TRUE. 
#' @param tbl character name of the table that holds the timeseries in the database. 
#' Defaults to 'timeseries_main'.
#' @export
deleteTimeseries <- function(series,connect = "con",rollback = T,
                             tbl = "timeseries_main"){
  # Because we cannot really use a global binding to 
  # the postgreSQL connection object which does not exist at the time
  # of compilation, we use the character name of the object here. 
  connect <- get(connect)
  
  # Store the timeseries that should be deleted in a 
  # separate environment to enable rollbacks
  # because SQL is pretty direct and immediately deletes the series
  # that others might want to use too. 
  if (rollback){
    if(!exists("db_rollback",envir = .GlobalEnv)){
      db_rollback <- new.env()
      assign("db_rollback",db_rollback,envir = .GlobalEnv)
    }
    assign(series,
           readTimeseries(series,connect = connect),
           envir = db_rollback)  
  }
  # Actually remove the time series from the database
  sql_statement <- sprintf("DELETE FROM %s WHERE ts_key = '%s'",tbl,series)
  dbGetQuery(connect,sql_statement)
}


