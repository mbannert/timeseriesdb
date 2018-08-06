#' Copy data.frame to postgres using bulk copy
#' 
#' @param con PostgreSQL connection object.
#' @param d data.frame
#' @param q character string containing a SQL query. 
#' @param chunksize integer, defaults to 10000.
#'
#' @export
pgCopyDf <- function(con, d, q, chunksize = 10000){
  l <- split(d, (seq(nrow(d))-1) %/% chunksize) 
  lapply(l,function(x){
    md_ok <- DBI::dbGetQuery(con,q)
    postgresqlCopyInDataframe(con, x)  
  })
}
