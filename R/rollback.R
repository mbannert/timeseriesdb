#' Rollback all Deleted Time Series to Database
#' 
#' This function rolls back all deleted time series that were deleted by \code{\link{deleteTimeseries}} and thus were stored in rollback environment in memory. This function only works if the option rollback = TRUE was set when exectuing the delete query. 
#' 
#' @author Matthias Bannert
#' @param rollback_env environment that contains time series that were deleted previously. Defaults to db_rollback.
#' @param connect connection object for PostgreSQL
#' @param tbl character variable name of the time series table in the database. Defaults to 'timeseries_main'.
#' @examples
#' ts1 <- ts(rnorm(100),start = c(1990,1),frequency = 4)
#' storeTimeseries(ts1)
#' deleteTimeseries("ts1")
#' rollback(db_rollback)
#' @export
rollback <- function(rollback_env = db_rollback,connect = con,
                     tbl = "timeseries_main"){
  # storeTimeseries needs string input in order to work flexibly
  # with single series and bulk processes like lapply
  lapply(ls(rollback_env),storeTimeseries,lookup_env = rollback_env)
}
