db_keys_to_collect <- function(con, collection_name, 
                               keys, description = NA,
                               user = Sys.info()['user'],
                               schema = "timeseries"){
  keys <- unique(keys)
  
  # if collection does not exist, create collection
  # classic UPSERT case, we use it in the DO NOTHING flavor
  # https://www.postgresql.org/docs/9.5/sql-insert.html#SQL-ON-CONFLICT
  # TODO: DO NOTHING has an issue with composite UNIQUE constraint, double check 
  c_id <- dbGetQuery(con,
                     sprintf("SELECT * FROM %s.add_collection('%s','%s','%s')",
                             schema, collection_name,
                             user, description))$add_collection
  
  if(is.na(c_id)) c_id <- dbGetQuery(con,
                                     "SELECT id FROM timeseries.collection
                                      WHERE name = collection_name
                                      AND owner = user")
  
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
