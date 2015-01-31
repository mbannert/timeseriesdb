#' Search Key-Value Pairs, look for existing keys in an Hstore
#' 
#' Search hstore key value in PostgreSQL. Very handsome when crawling the database by meta informaiton. 
#' Currently works for non translated meta information.
#' 
#' @param key character
#' @param value in the hstore
#' @param con PostgreSQL connection object
#' @param hstore name of the hstore column
#' @param tbl name of the table to be queried. defaults to  'meta_data_localized'
#' @param where character restrict the SQL query by an additional where clause. Defaults to NULL.
#' E.g.: ts_key LIKE '%ch%'.
#' @rdname searchHstore
#' @export
searchKVP <- function(key,value,con = options()$TIMESERIESDB_CON,
                      hstore = 'meta_data',tbl = 'meta_data_unlocalized',
                      where = NULL){
  if(is.null(con)) stop('Default TIMESERIESDB_CON not set in options() or no proper connection given to the con argument.')
  
  # optional AND 
  # Emulate sql's coalesce for dummies here 
  # cause sprintf doesn't like NULL either
  if(!is.null(where)) and <- paste0(" AND ",where)
  
  sql_query <- sprintf("SELECT ts_key FROM %s WHERE %s->'%s'='%s'%s",tbl,hstore,key,value)
  result <- dbGetQuery(con,sql_query)
  result$ts_key
}


#' @rdname searchHstore
#' @export
lookForKey <- function(key,con = options()$TIMESERIESDB_CON,
                       hstore = 'meta_data',tbl = 'meta_data_unlocalized',
                       where = NULL){
  if(is.null(con)) stop('Default TIMESERIESDB_CON not set in options() or no proper connection given to the con argument.')
  
  # optional AND 
  # Emulate sql's coalesce for dummies here 
  # cause sprintf doesn't like NULL either
  and <- ''
  if(!is.null(where)) and <- paste0(" AND ",where)
  
  sql_query <- sprintf("SELECT ts_key,%s->'%s' AS %s
                       FROM %s WHERE %s ? '%s'%s",hstore,key,key,
                       tbl,hstore,key,where)
  result <- dbGetQuery(con,sql_query)
  result
}
  
