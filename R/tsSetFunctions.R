#' List All Time Series Sets for a Particular User
#' 
#' Show the names of all sets that are available to a particular user. 
#' 
#' @param con PostgreSQL connection object
#' @param user_name character name of the user. Defaults to system user. 
#' @param tbl character name of set tqble. Defaults to timeseries\_sets.
#' @author Matthias Bannert, Gabriel Bucur
#' @export
#' @rdname listTsSets
listTsSets <- function(con,user_name = Sys.info()['user'],tbl = "timeseries_sets"){
  sql_query <- sprintf("SELECT setname FROM %s WHERE username = '%s' AND active = TRUE",tbl,user_name)
  dbGetQuery(con,sql_query)$setname
}


#' Load a Time Series Set
#' 
#' Loads a Time Series Set.
#' 
#' @param con PostgreSQL connection object
#' @param user_name character name of the user. Defaults to system user. 
#' @param tbl character name of set tqble. Defaults to timeseries\_sets.
#' @author Matthias Bannert, Ioan Gabriel Bucur
#' @export
#' @rdname loadTsSet
loadTsSet <- function(con, set_name, user_name = Sys.info()['user'], tbl = 'timeseries_sets') {
  
  sql_query <- sprintf("SELECT setname,username,tstamp,
                       set_description,
                       key_set::json::text FROM %s WHERE username = '%s'
                       AND setname = '%s'",
                       tbl, user_name,set_name)
  
  set <- dbGetQuery(con, sql_query)
  
  result <- list()
  result$set_info <- set[,c("setname","username","tstamp","set_description")]
  json_conversion <- RJSONIO::fromJSON(set$key_set)
  result$keys <- names(json_conversion)
  result$key_type <- unique(json_conversion)
  if(length(result$key_type) != 1) stop("Multiple key type are not allowed in the same set yet.")
  result
}


#' Deactivate a Set of Time Series
#' 
#' This deactivates a set of time series to get out of the user's sight, 
#' but it's not the deleted because users may not delete sets.
#'
#' @param con PostgreSQL connection object
#' @param user_name character name of the user. Defaults to system user. 
#' @param tbl character name of set tqble. Defaults to timeseries\_sets.
#' @author Matthias Bannert, Ioan Gabriel Bucur
#' @export
#' @rdname deactivateTsSet
deactivateTsSet <- function(con,set_name,
                            user_name = Sys.info()['user'],
                            tbl = "timeseries_sets"){
  sql_query <- sprintf("UPDATE %s SET active = FALSE
                       WHERE username = '%s' AND setname = '%s'",
                       tbl,user_name,set_name)
  dbGetQuery(con,sql_query)
}


#' Store a New Set of Time Series
#' 
#' Store a new set of Time Series to the database. Users can select the time series keys
#' that should be grouped inside a set.
#' 
#' @param con PostgreSQL connection object
#' @param set_name character name of a set time series in the database.
#' @param set_keys list of keys contained in the set and their type of key. 
#' @param user_name character name of the user. Defaults to system user. 
#' @param tbl character name of set tqble. Defaults to timeseries\_sets.
#' @author Ioan Gabriel Bucur
#' @export
#' @rdname storeTsSet
storeTsSet <- function(con, set_name, set_keys,
                       user_name = Sys.info()['user'],
                       description = '', active = TRUE,
                       tbl = 'timeseries_sets') {
  vector_values <-c(set_name,
                    user_name, as.character(Sys.time()),
                    createHstore(set_keys, fct = TRUE), description, active)
  row_values <- paste(lapply(vector_values,
                             function(str) {
                               ifelse(grepl("hstore",str),
                                      sprintf("%s", str),
                                      sprintf("'%s'", str)
                                      )
                               }),
                      collapse = ",")
  
  sql_query <- sprintf(
    "INSERT INTO %s VALUES (%s)",
    tbl, row_values
  )
  dbSendQuery(con, sql_query)
}








