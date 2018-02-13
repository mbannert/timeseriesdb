#' Run SELECT query
#' 
#' Run database queries using \code{\link[DBI]{dbSendQuery}}, \code{\link[DBI]{fetch}} and \code{\link[DBI]{dbClearResult}} in similar fashion as \code{\link[DBI]{dbGetQuery}} but provide better error handling. 
#'This function always returns a data.frame as opposed to different types in case of an exception. However, if the database query fails and empty data.frame is returned. Besides query status and database error are returned as attributes. Make sure to use BEGIN and COMMIT outside of these statements.
#'
#' @param con PostgreSQL connection object
#' @param sql_query character string containing a SQL query
#' @export
#' @examples 
#' # There's no connection, so this returns a proper error message.
#' \donttest{
#' out_obj <- runDbQuery(bogus_connection,"SELECT * FROM some_table") 
#' attributes(out_obj)
#' }
runDbQuery <- function(con,sql_query,...){
  # treat warnings as erors
  options(warn=2)
  tryCatch({
    return_df <- suppressMessages(dbGetQuery(con,sql_query,...))
    if(is.null(return_df)) return_df <- data.frame()
    attr(return_df,"query_status") <- "OK"
    options(warn=1)
    return_df
  },error = function(e){
    return_df <- data.frame()
    attr(return_df,"query_status") <- "Failure"
    em <- geterrmessage()
    class(em) <- "SQL"
    attr(return_df,"db_error") <- em
    options(warn=1)
    dbGetQuery(con,"ROLLBACK")
    return_df
  })  
}
