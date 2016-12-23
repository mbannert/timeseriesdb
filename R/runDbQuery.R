#' Run SELECT query
#' 
#' Run database queries using \code{\link{DBI::dbSendQuery}}, \code{\link{DBI::fetch}} and \code{\link{DBI::dbClearResult}} in similar fashion as \code{\link{DBI::dbGetQuery}} but provide better error handling. 
#'This function always returns a data.frame as opposed to different types in case of an exception. However, if the database query fails and empty data.frame is returned. Besides query status and database error are returned as attributes. Make sure to use BEGIN and COMMIT outside of these statements.
#'
#' @param con PostgreSQL connection object
#' @param sql_query character string containing a SQL query
#' @export
#' @examples 
#' # There's no connection, so this returns a proper error message.
#' out_obj <- runDbQuery(bogus_connection,"SELECT * FROM some_table") 
#' attributes(out_obj)
runDbQuery <- function(con,sql_query,...){
  tryCatch({
    rs <- dbSendQuery(con,sql_query,...)
    return_df <- fetch(rs,n = -1)
    attr(return_df,"query_status") <- "OK"
    on.exit(dbClearResult(rs))
    return_df
  },error = function(e){
    return_df <- data.frame()
    attr(return_df,"query_status") <- "Failure"
    attr(return_df,"db_error") <- geterrmessage()
    cat("DB query not valid, returned empty data.frame.\nRun attributes(your_return_object) to see database error message.")
    return_df
  })  
}
