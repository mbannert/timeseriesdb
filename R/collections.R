#' Bundles Keys into an Existing Collection or Adds a New Collection
#' @param  con PostgreSQL connection object created with RPostgres.
#' @param collection_name character name of the collection
#' @param keys character vector of time series keys.
#' @param description character description of the collection
#' @param user character name of the User. Defaults to current system user.
#' @param schema character name of the schema. Defaults to 'timeseries'.
#' @importFrom jsonlite fromJSON
#' @export
db_collection_add <- function(con, collection_name,
                              keys, description = NA,
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

  db_return <- db_call_function(con,
                                "insert_collect_from_tmp",
                                schema = schema)

  fromJSON(db_return)
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
  # Schemas can't be added through parameterized queries
  # therefore we need to sanitize the schema string here.
  schema <- dbQuoteIdentifier(con, Id(schema = schema))

  # get c_id using collection_name, user
  #! I would rather do
  #! store keys into temp table
  #! timeseries.collection_remove(collection_name, collection_owner)
  #! let the function figure out the id, no plain sql code needed here
  q <- sprintf("SELECT id FROM %scollections
                WHERE name = $1
                AND owner = $2",
               schema)
  c_id_q <- dbSendQuery(con, q)
  dbBind(c_id_q, list(collection_name, user))
  c_id <- dbFetch(c_id_q)$id
  if(dbHasCompleted(c_id_q)) dbClearResult(c_id_q)


  # write temp table
  dt <- data.table(c_id = c_id,
                   ts_key = keys)
  dbWriteTable(con,
               "tmp_collection_remove",
               dt,
               temporary = TRUE,
               overwrite = TRUE,
               field.types = c(
                 c_id = "uuid",
                 ts_key = "text")
  )

  q <- sprintf("SELECT * FROM %scollection_remove()", schema)
  q_rmv <- dbSendQuery(con, q)
  q_rmv_res <- dbFetch(q_rmv)$collection_remove
  if(dbHasCompleted(q_rmv)) dbClearResult(q_rmv)

  fromJSON(q_rmv_res)

}


db_collection_delete <- function(con,
                                 collection_name,
                                 user = Sys.info()['user'],
                                 schema = "timeseries"
                                 ){
  schema <- dbQuoteIdentifier(con, Id(schema = schema))

}
















