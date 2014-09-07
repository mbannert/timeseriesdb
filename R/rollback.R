#' Rollback all Deleted Time Series to Database
#' 
#' This function rolls back all deleted time series that were deleted by \code{\link{deleteTimeseries}} and thus were stored in rollback environment in memory. This function only works if the option rollback = TRUE was set when exectuing the delete query. 
#' 
#' @author Matthias Bannert
#' @param rollback_env character name of rollback environment.
#' @param connect character name of the PostgreSQL connection object.
#' @param tbl character variable name of the time series table in the database. Defaults to 'timeseries_main'.
#' @export
rollback <- function(rollback_env = "db_rollback",connect = "con",
                     tbl = "timeseries_main"){
  # Because we cannot really use a global binding to 
  # the postgreSQL connection object which does not exist at the time
  # of compilation, we use the character name of the object here. 
  connect <- get(connect)
  db_rollback <- get(rollback_env)
  # storeTimeseries needs string input in order to work flexibly
  # with single series and bulk processes like lapply
  lapply(ls(rollback_env),storeTimeseries,lookup_env = rollback_env)
}
