#' Convenience Wrapper to SQL classics for BEGIN,COMMIT, ROLLBACK
#' 
#' this set of function can speed up loops by starting a transaction, 
#' performing several queries and ending them with either commit or rollback. 
#' 
#' @param con PostgreSQL connection object.
#' @importFrom DBI dbGetQuery
#' @rdname transactionUtils
#' @export
beginTransaction <- function(con, quiet = T){
  out <- dbGetQuery(con,'BEGIN')
  if(is.null(out) && !quiet) print('BEGIN')
}

#' @rdname transactionUtils
#' @export
commitTransaction <- function(con, quiet = T){
  out <- dbGetQuery(con,'COMMIT')
  if(is.null(out) && !quiet) print('COMMIT')
}

#' @rdname transactionUtils
#' @export
rollbackTransaction <- function(con, quiet = T){
  out <- dbGetQuery(con,'COMMIT')
  if(is.null(out) && !quiet) print('ROLLBACK')
}
