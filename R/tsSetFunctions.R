#' Store a New Set of Time Series
#' 
#' Store a new set of Time Series to the database. Users can select the time series keys
#' that should be grouped inside a set.
#' 
#' @param con PostgreSQL connection object
#' @param set_name character name of a set time series in the database.
#' @param set_keys list of keys contained in the set and their type of key. 
#' @param key_type character A vector of key types to store the keys as. Will be recycled to match the length of set_keys.
#' @param user_name character name of the user. Defaults to system user. 
#' @param description character description of the set to be stored in the db.
#' @param active logical should a set be active? Defaults to TRUE. If set to FALSE 
#' a set is not seen directly in the GUI directly after being stored and needs to be
#' activated first. 
#' @param tbl character name of set tqble. Defaults to timeseries\_sets.
#' @param schema character name of the database schema. Defaults to timeseries.
#' @author Ioan Gabriel Bucur, Matthias Bannert, Severin Thöni
#' @export
#' @rdname storeTsSet
storeTsSet <- function(con,
                       set_name,
                       set_keys,
                       user_name = Sys.info()['user'],
                       description = '',
                       active = TRUE,
                       tbl = "timeseries_sets",
                       schema = "timeseries") {
  UseMethod("storeTsSet", set_keys)
}

#' @export
storeTsSet.list <- function(con,
                            set_name,
                            set_keys,
                       user_name = Sys.info()['user'],
                       description = '', active = TRUE,
                       tbl = 'timeseries_sets',
                       schema = 'timeseries') {
  warning("Storing sets as lists is deprecated and will be removed in future versions.")
  storeTsSet(
    con,
    set_name,
    names(set_keys),
    user_name,
    description,
    active,
    tbl,
    schema
  )
}

#' @export
storeTsSet.character <- function(con,
                                 set_name,
                                 set_keys,
                                 user_name = Sys.info()['user'],
                                 description = '',
                                 active = TRUE,
                                 tbl = "timeseries_sets",
                                 schema = "timeseries") {
  sql_query <- sprintf("INSERT INTO %s.%s(setname, username, key_set, set_description, active)
                       VALUES('%s', '%s', ARRAY[%s], '%s', %s)",
                       schema,
                       tbl,
                       set_name,
                       user_name,
                       paste(sprintf("'%s'", set_keys), collapse=","),
                       description,
                       ifelse(active, "true", "false"))
  class(sql_query) <- "SQL"
  runDbQuery(con, sql_query)
}

#' Join two Time Series sets together
#'
#' This will create a new set set_name_new with the keys from both 
#' set_name_1 and set_name_2 combined.
#' By default the description will be a combination of the descriptions
#' of the subsets and the new set will only be active it BOTH subsets
#' were active.
#'
#' @param con PostgreSQL connection
#' @param set_name_1 Name of the first set
#' @param set_name_2 Name of the second set
#' @param set_name_new Name of the set to be created
#' @param user_name1 User name of the first set's owner
#' @param user_name2 User name of the second set's owner
#' @param user_name_new User name of the new set's owner
#' @param description Description of the new set
#' @param active Should the new set be marked as active
#' @param tbl The time series set table
#' @param schema The time series db schema to use
#' @export
#' @author Severin Thöni
# TODO: overwrite it set_name_new == set_name_1 or set_name2
joinTsSets <- function(con,
                       set_name_1, set_name_2, set_name_new,
                       user_name1 = Sys.info()['user'],
                       user_name2 = user_name1,
                       user_name_new = user_name1,
                       description = NULL,
                       active = NULL,
                       tbl = "timeseries_sets",
                       schema = "timeseries") {
  contents1 <- readTsSetKeys(con, set_name_1, user_name1, tbl, schema)
  contents2 <- readTsSetKeys(con, set_name_2, user_name2, tbl, schema)
  
  if(is.null(contents1)) {
    message(sprintf("Could not find set %s belonging to %s.", set_name_1, user_name1))
  } else if(is.null(contents2)) {
    message(sprintf("Could not find set %s belonging to %s.", set_name_2, user_name2))
  } else {
    if(is.null(description)) {
      description <- paste(contents1$set_info$description, " combined with ", contents2$set_info$description)
    }
    
    if(is.null(active)) {
      active = contents1$set_info$active && contents2$set_info$active
    }
    
    storeTsSet(con, set_name_new, union(contents1$keys, contents2$keys), user_name_new, description, active, tbl, schema)
  }
}

#' List All Time Series Sets for a Particular User
#' 
#' Show the names of all sets that are available to a particular user. 
#' 
#' @param con PostgreSQL connection object
#' @param user_name character name of the user. Defaults to system user. 
#' @param tbl character name of set tqble. Defaults to timeseries\_sets.
#' @param schema character name of the database schema. Defaults to timeseries.
#' @param list_inactive logical Should inactive sets be listed too?
#' @author Matthias Bannert, Gabriel Bucur
#' @export
#' @importFrom DBI dbGetQuery
#' @rdname listTsSets
listTsSets <- function(con, user_name = Sys.info()['user'], 
                       tbl = "timeseries_sets", schema = "timeseries",
                       list_inactive = FALSE){
  sql_query <- sprintf("SELECT setname FROM %s.%s 
                       WHERE username = '%s' 
                       %s",
                       schema,tbl,user_name,
                       ifelse(list_inactive, "", "AND active = TRUE"))
  class(sql_query) <- "SQL"
  dbGetQuery(con,sql_query)$setname
}


#' Load a Time Series Set
#' 
#' Loads a Time Series Set.
#' 
#' @param con PostgreSQL connection object
#' @param user_name character name of the user. Defaults to system user. 
#' @param set_name character name of the set to be loaded.
#' @param tbl character name of set tqble. Defaults to timeseries\_sets.
#' @param schema character name of the database schema. Defaults to timeseries.
#' @author Matthias Bannert, Ioan Gabriel Bucur
#' @export
#' @importFrom DBI dbGetQuery
#' @importFrom jsonlite fromJSON
#' @rdname readTsSetKeys
readTsSetKeys <- function(con, set_name, user_name = Sys.info()['user'],
                       tbl = 'timeseries_sets', schema = 'timeseries') {
  
  sql_query <- sprintf("SELECT setname,username,tstamp,
                       set_description, active,
                       unnest(key_set) as ts_keys FROM %s.%s
                       WHERE username = '%s'
                       AND setname = '%s'",
                       schema, tbl, user_name, set_name)
  class(sql_query) <- "SQL"
  set <- dbGetQuery(con, sql_query)
  if(nrow(set) == 0){
    message("No set with this set_name / user_name combination available.")
    return(NULL)
  }
  
  
  result <- list()
  result$set_info <- set[1, c("setname","username","tstamp","set_description","active")]
  result$ts_key <- set$ts_keys
  out <- as.data.table(result)
  names(out) <- gsub("set_info\\.", "", names(out))
  out
}

#' @export
#' @rdname readTsSetKeys
loadTsSet <- function(...) {
  warning("loadTsSet is deprecated and will be removed in future versions. Please use readTsSetKeys instead.")
  
  result <- readTsSetKeys(...)
  
  list(
    set_info = result[1, c("setname", "username", "tstamp", "set_description", "active")],
    keys = result[, ts_key]
  ) 
}

readTsSet <- function(con, set_name, user_name = Sys.info()['user'],
                      tbl = 'timeseries_sets', schema = 'timeseries') {
  # either select * from (select unnest(key_set) as ts_key from timeseriesdb_unit_tests.timeseries_sets where setname = 'set1') set join timeseriesdb_unit_tests.timeseries_main main on set.ts_key = main.ts_key;
  # or readTsSetKeys -> readTimeSeries (easier as conversion and error handling are already there but uses 2 queries)
  tstools::generate_random_ts(4)
}

#' Deactivate a Set of Time Series
#' 
#' This deactivates a set of time series to get out of the user's sight, 
#' but it's not the deleted because users may not delete sets.
#'
#' @param con PostgreSQL connection object
#' @param set_name character name of the set to be deactivated.
#' @param user_name character name of the user. Defaults to system user. 
#' @param tbl character name of set tqble. Defaults to timeseries\_sets.
#' @param schema character name of the database schema. Defaults to timeseries.
#' @author Matthias Bannert, Ioan Gabriel Bucur
#' @export
#' @importFrom DBI dbGetQuery
#' @rdname deactivateTsSet
deactivateTsSet <- function(con,set_name,
                            user_name = Sys.info()['user'],
                            tbl = "timeseries_sets",
                            schema = "timeseries"){
  sql_query <- sprintf("UPDATE %s.%s SET active = FALSE
                       WHERE username = '%s' AND setname = '%s'",
                       schema,tbl,user_name,set_name)
  class(sql_query) <- "SQL"
  runDbQuery(con,sql_query)
}



#' Activate a Set of Time Series
#' 
#' Activate a set of time series to get in the user's sight. 
#' Deactivated sets are not deleted though.
#'
#' @param con PostgreSQL connection object
#' @param user_name character name of the user. Defaults to system user. 
#' @param set_name character name of the set to be activated.
#' @param tbl character name of set tqble. Defaults to timeseries\_sets.
#' @param schema character name of the database schema. Defaults to timeseries.
#' @author Matthias Bannert, Ioan Gabriel Bucur
#' @export
#' @importFrom DBI dbGetQuery
#' @rdname activateTsSet
activateTsSet <- function(con,set_name,
                            user_name = Sys.info()['user'],
                            tbl = "timeseries_sets",
                            schema = "timeseries"){
  sql_query <- sprintf("UPDATE %s.%s SET active = TRUE
                       WHERE username = '%s' AND setname = '%s'",
                       schema,tbl,user_name,set_name)
  class(sql_query) <- "SQL"
  runDbQuery(con,sql_query)
}

#' Overwrite a Time Series set with a new one
#'
#' Completely replaces the set set_name of user_name with the new values
#' (keys, description, active) of the new one.
#' If the set does not yet exist for the given user it will be created.
#'
#' @param con PostgreSQL connection
#' @param set_name The name of the set to be overwritten
#' @param ts_keys The keys in the new set
#' @param user_name The owner of the set to be overwritten
#' @param description The description of the new set
#' @param active Should the new set be active?
#' @param tbl Name of the time series sets table
#' @param schema Schema of the time series database to use
#' @export
#' @author Severin Thöni
overwriteTsSet <- function(con,
                           set_name,
                           ts_keys,
                           user_name = Sys.info()['user'],
                           description = "",
                           active = TRUE,
                           tbl = "timeseries_sets",
                           schema = "timeseries") {
    deleted <- deleteTsSet(con, set_name, user_name, tbl, schema)
    
    if(attributes(deleted)$query_status == "OK") {
      storeTsSet(con, set_name, ts_keys, user_name, description, active, tbl, schema)
    } else {
      deleted
    }
}

#' Add keys to an existing Time Series set
#'
#' @param con PostgreSQL connection
#' @param set_name The name of the set
#' @param ts_keys A character vector of keys to be added
#' @param user_name The user name of the set's owner
#' @param tbl Name of the time series sets table
#' @param schema Schema of the time series database to use
#' @export
#' @author Severin Thöni
addKeysToTsSet <- function(con,
                           set_name,
                           ts_keys,
                           user_name = Sys.info()['user'],
                           tbl = "timeseries_sets",
                           schema = "timeseries") {
  set <- readTsSetKeys(con, set_name, user_name, tbl, schema)
  
  if(!is.null(set)) {
    pg_keys <- paste(sprintf("'%s'", unique(c(ts_keys, set$ts_key))), collapse = ",")
    sql_query <- sprintf("UPDATE %s.%s set key_set = ARRAY[%s] WHERE username = '%s' and setname = '%s'",
                         schema, tbl, pg_keys, user_name, set_name)
    runDbQuery(con, sql_query)
  } else {
    message("Set-User combination not found!")
  }
}

#' Remove keys from a Time Series set (if present)
#'
#' @param con PostgreSQL connection
#' @param set_name character name of a time series set.
#' @param ts_keys A character vector of keys to be removed.
#' @param user_name The user name of the set's owner.
#' @param tbl Name of the time series sets table.
#' @param schema Schema of the time series database to use.
#' @export
#' @author Severin Thöni
removeKeysFromTsSet <- function(con,
                                set_name,
                                ts_keys,
                                user_name = Sys.info()['user'],
                                tbl = "timeseries_sets",
                                schema = "timeseries") {
  set <- readTsSetKeys(con, set_name, user_name, tbl, schema)
  
  if(!is.null(set)) {
    pg_keys <- paste(sprintf("'%s'", setdiff(set$ts_key, ts_keys)), collapse = ", ")
    sql_query <- sprintf("UPDATE %s.%s set key_set = ARRAY[%s] WHERE username = '%s' and setname = '%s'",
                         schema, tbl, pg_keys, user_name, set_name)
    runDbQuery(con, sql_query)
  } else {
    message("Set-User combination not found!")
  }
}

#' Change the owner of a Time Series set
#'
#' @param con PostgreSQL connection
#' @param set_name Name of the set to be updates
#' @param old_owner User name of the set's current owner
#' @param new_owner User name of the set's new owner
#' @param tbl Name of the time series sets table
#' @param schema Schema of the time series database to use
#' @export
#' @author Severin Thöni
changeTsSetOwner <- function(con,
                             set_name,
                             old_owner = Sys.info()['user'],
                             new_owner,
                             tbl = "timeseries_sets",
                             schema = "timeseries") {
  sql_query <- sprintf("UPDATE %s.%s SET username = '%s' WHERE setname = '%s' AND username = '%s'",
                       schema, tbl, new_owner, set_name, old_owner);
  runDbQuery(con, sql_query)
}

#' Permanently delete a Set of Time Series Keys
#'
#' @param con PostgreSQL connection object
#' @param set_name The name of the set to be deleted
#' @param user_name Username to which the set belongs
#' @param tbl Name of set table
#' @param schema Name of timeseries schema
#' @author Severin Thöni
#' @export
deleteTsSet <- function(con,
                        set_name,
                        user_name = Sys.info()['user'],
                        tbl = "timeseries_sets",
                        schema = "timeseries") {
  sql_query <- sprintf("DELETE FROM %s.%s WHERE username = '%s' AND setname = '%s'",
                       schema, tbl, user_name, set_name)
  runDbQuery(con, sql_query)
}
