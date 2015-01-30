#' Convenience Wrapper to SQL classics for BEGIN,COMMIT, ROLLBACK
#' 
#' this set of function can speed up loops by starting a transaction, 
#' performing several queries and ending them with either commit or rollback. 
#' 
#' @param con PostgreSQL connection object.
#' @rdname transactionUtils
#' @export
beginTransaction <- function(con = options()$TIMESERIESDB_CON){
  if(is.null(con)) stop('Default TIMESERIESDB_CON not set in options() or no proper connection given to the con argument.')
  if(is.null(DBI::dbGetQuery(con,'BEGIN'))) print('BEGIN')
}

#' @export
commitTransaction <- function(con = options()$TIMESERIESDB_CON){
  if(is.null(con)) stop('Default TIMESERIESDB_CON not set in options() or no proper connection given to the con argument.')
  if(is.null(DBI::dbGetQuery(con,'COMMIT'))) print('COMMIT')
}

#' @export
rollbackTransaction <- function(con = options()$TIMESERIESDB_CON){
  if(is.null(con)) stop('Default TIMESERIESDB_CON not set in options() or no proper connection given to the con argument.')
  if(is.null(DBI::dbGetQuery(con,'COMMIT'))) print('ROLLBACK')
}