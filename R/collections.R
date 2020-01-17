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
  
  
  # TODO: WHERE NOT IN FAILS, need to cast it 
  # TODO: Change return strategy, this should fail gracefully
  dbGetQuery(con, "SELECT * FROM tmp_collect_updates")
  
  dbGetQuery(con, "SELECT * FROM timeseries.insert_collect_from_tmp()")
  
  dbGetQuery(con, "SELECT json_array_elements(jay) FROM (SELECT json_agg(ts_key) AS jay FROM tmp_collect_updates) AS j")
  
  # return keys in a warning that could not be added because
  # they were not part of the catalog
  
  
  
}
