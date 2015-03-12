storeTsSet <- function(con, set_name, set_keys, user_name = Sys.info()['user'], description = '', tbl = 'timeseries_sets') {
  
  vector_values <-c(set_name, user_name, as.character(Sys.time()), createHstore(set_keys, fct = FALSE), description)
  row_values <- paste(lapply(vector_values, function(str) sprintf("'%s'", str)), collapse = ",")
  
  sql_query <- sprintf(
    "INSERT INTO %s VALUES (%s)",
    tbl, row_values
  )
  dbSendQuery(con, sql_query)
}