#' Bundles Keys into an Existing Collection or Adds a New Collection
#' @param  con PostgreSQL connection object created with RPostgres.
#' @param collection_name character name of the collection
#' @param keys character vector of time series keys.
#' @param description character description of the collection
#' @param user character name of the User. Defaults to current system user.
#' @param schema character name of the schema. Defaults to 'timeseries'.
#' @importFrom jsonlite fromJSON
#' @export
db_collection_add <- function(con,
                              collection_name,
                              keys,
                              description = NA,
                              user = Sys.info()['user'],
                              schema = "timeseries"){
  keys <- unique(keys)

  # if collection does not exist, create collection
  c_id <- db_call_function(con,
                           "collection_add",
                           list(
                             collection_name,
                             user,
                             description
                           ),
                           schema = schema)

  # by now collection should exist,
  # let's add keys: fill a temp table, anti-join the keys
  # INSERT non existing ones.
  dt <- data.table(c_id = c_id,
                   ts_key = keys)

  dbWriteTable(con,
               "tmp_collect_updates",
               dt,
               temporary = TRUE,
               overwrite = TRUE,
               field.types = c(
                 c_id = "uuid",
                 ts_key = "text")
  )

  db_return <- fromJSON(db_call_function(con,
                                "insert_collect_from_tmp",
                                schema = schema))

  if(db_return$status == "warning") {
    warning(db_return$message)
  }

  db_return
}


#' Remove Keys From a User's Collection
#'
#' Removes a vector of time series keys from an a set of
#' keys defined for that user.
#'
#' @param  con PostgreSQL connection object created with RPostgres.
#' @param collection_name character name of the collection
#' @param keys character vector of time series keys.
#' @param user character name of the User. Defaults to current system user.
#' @param schema character name of the schema. Defaults to 'timeseries'.
#' @importFrom jsonlite fromJSON
#' @export
db_collection_remove <- function(con,
                                 collection_name,
                                 keys,
                                 user = Sys.info()['user'],
                                 schema = "timeseries"){
  keys <- unique(keys)

  # write temp table
  dt <- data.table(ts_key = keys)
  dbWriteTable(con,
               "tmp_collection_remove",
               dt,
               temporary = TRUE,
               overwrite = TRUE,
               field.types = c(ts_key = "text")
  )

  db_return <- fromJSON(db_call_function(con,
                                "collection_remove",
                                list(collection_name, user)))

  if(db_return$status == "error") {
    stop(db_return$message)
  }

  db_return
}


#' Remove an Entire Time Series Key Collection
#'
#' @param con PostgreSQL connection object created with RPostgres.
#' @param collection_name character name of the collection
#' @param user character name of the User. Defaults to current system user.
#' @param schema character name of the schema. Defaults to 'timeseries'.
#'
#' @return
#'
#' @importFrom jsonlite fromJSON
#' @export
db_collection_delete <- function(con,
                                 collection_name,
                                 user = Sys.info()['user'],
                                 schema = "timeseries"
                                 ){
  db_return <- fromJSON(db_call_function(con,
                                "collection_delete",
                                list(collection_name, user),
                                schema = schema))

  # TODO: Discuss warning vs error esp wrt remove. remove treats this as error
  # since the expected change CAN NOT be achieved while here it PROBABLY ALREADY IS achieved.
  if(db_return$status == "warning") {
    warning(db_return$message)
  }

  db_return
}
















