loadTsSet <- function(con, set_name = NULL, user_name = Sys.info()['user'], tbl = 'timeseries_sets') {
  
  sql_query <- sprintf("SELECT * FROM %s WHERE username = '%s'", tbl, user_name)
  if (!is.null(set_name)) sql_query <- sprintf("%s AND setname = '%s'", sql_query, set_name)
  
  set <- dbGetQuery(con, sql_query)
  
  set
}