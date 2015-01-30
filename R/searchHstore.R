#' Search Key-Value Pairs
#' 
#' Search hstore key value in PostgreSQL. Very handsome when crawling the database by meta informaiton. 
#' Currently works for non translated meta information.
#' 
#' @param key character
#' @param value in the hstore
#' @param con PostgreSQL connection object
#' @param hstore name of the hstore column
#' @param tbl name of the table to be queried. defaults to  'meta_data_localized'
#' @rdname searchHstore
#' @export
searchKVP <- function(key,value,con = options()$TIMESERIESDB_CON,
                      hstore = 'meta_data',tbl = 'meta_data_unlocalized'){
  if(is.null(con)) stop('Default TIMESERIESDB_CON not set in options() or no proper connection given to the con argument.')
  sql_query <- sprintf("SELECT ts_key FROM %s WHERE %s->'%s'='%s'",tbl,hstore,key,value)
  result <- dbGetQuery(con,sql_query)
  result$ts_key
}


#' @export
lookForKey <- function(key,con = options()$TIMESERIESDB_CON,
                       hstore = 'meta_data',tbl = 'meta_data_unlocalized'){
  if(is.null(con)) stop('Default TIMESERIESDB_CON not set in options() or no proper connection given to the con argument.')
  sql_query <- sprintf("SELECT ts_key FROM %s WHERE %s ? '%s'",tbl,hstore,key)
  result <- dbGetQuery(con,sql_query)
  result$ts_key
}
  
