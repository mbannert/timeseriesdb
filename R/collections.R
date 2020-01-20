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
  # Schemas can't be added through parameterized queries
  # therefore we need to sanitize the schema string here.
  schema <- dbQuoteIdentifier(con, Id(schema = schema))
  
  # if collection does not exist, create collection
  # classic UPSERT case, we use it in the DO NOTHING flavor
  # https://www.postgresql.org/docs/9.5/sql-insert.html#SQL-ON-CONFLICT
  q <- sprintf(
    "SELECT * FROM %scollection_add($1, $2, $3)", schema)
  c_id <- dbSendQuery(con, q)
  dbBind(c_id, list("collection_name", "user", "description"))
  c_id <- dbFetch(c_id)$collection_add
  
  if(is.na(c_id)){
    # need this sprintf hack to allow a schema parameter
    q <- sprintf("SELECT id FROM %scollections
                  WHERE name = $1
                  AND owner = $2", schema)
    c_id_na_q <- dbSendQuery(con, q)
    dbBind(c_id_na_q, list("collection_name", "user"))
    c_id <- dbFetch(c_id_na_q)$id
    if(dbHasCompleted(c_id_na_q)) dbClearResult(c_id_na_q)
  } 
  
  
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
  
  db_return <- dbGetQuery(
    con,
    "SELECT * FROM timeseries.insert_collect_from_tmp()"
  )$insert_collect_from_tmp
  
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
  q <- sprintf("SELECT id FROM %scollections
                WHERE name = $1
                AND owner = $2",
               schema)
  c_id <- dbSendQuery(con, q)
  dbBind(c_id, list("collection_name", "user"))
  c_id <- dbFetch(c_id)$id
  # TODO END this query
  
  
  # write temp table
  dt <- data.table(c_id = c_id,
                   ts_key = keys)
  dbWriteTable(con,
               "tmp_collection_remove",
               dt,
               temporary = TRUE,
               overwrite = TRUE,
               field.types = c(
                 c_id = "text",
                 ts_key = "text")
  )
  
  
  # call remove collection
  q <- sprintf("DELETE FROM %scollect_catalog cc
               USING tmp_collection_remove rm
               WHERE cc.ts_key = rm.ts_key
               RETURNING *", schema)
  q_rmv <- dbSendQuery(con, q)
  q_rmv_res <- dbFetch(c_id)
  
}


db_collection_delete <- function(){
  
}
















