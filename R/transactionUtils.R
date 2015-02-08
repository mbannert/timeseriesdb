#' Convenience Wrapper to SQL classics for BEGIN,COMMIT, ROLLBACK
#' 
#' this set of function can speed up loops by starting a transaction, 
#' performing several queries and ending them with either commit or rollback. 
#' 
#' @param con PostgreSQL connection object.
#' @rdname transactionUtils
#' @export
beginTransaction <- function(con){
  if(class(con) != "PostgreSQLConnection") stop('Default TIMESERIESDB_CON not set in Sys.getenv or no proper connection given to the con argument. con is not a PostgreSQLConnection obj.')
  if(is.null(DBI::dbGetQuery(con,'BEGIN'))) print('BEGIN')
}

#' @export
commitTransaction <- function(con){
  if(is.null(DBI::dbGetQuery(con,'COMMIT'))) print('COMMIT')
}

#' @export
rollbackTransaction <- function(con){
  if(is.null(DBI::dbGetQuery(con,'COMMIT'))) print('ROLLBACK')
}
