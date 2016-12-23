.queryStoreNoVintage <- function(val,schema,tbl){
  sql_query <- sprintf("BEGIN;
                       CREATE TEMPORARY TABLE 
                       ts_updates(ts_key varchar, validity daterange, ts_data hstore, ts_frequency integer)
                       ON COMMIT DROP;
                       
                       INSERT INTO ts_updates(ts_key, validity, ts_data, ts_frequency) VALUES %s;
                       LOCK TABLE %s.%s IN EXCLUSIVE MODE;
                       
                       -- Update existing entries
                       UPDATE %s.%s
                       SET ts_data = ts_updates.ts_data,
                       validity = ts_updates.validity,
                       ts_frequency = ts_updates.ts_frequency
                       FROM ts_updates
                       WHERE ts_updates.ts_key = %s.%s.ts_key
                       AND %s.%s.validity @> CURRENT_DATE;
                       
                       -- Add new entries
                       INSERT INTO %s.%s
                       SELECT ts_updates.ts_key, validity, ts_updates.ts_data, ts_updates.ts_frequency
                       FROM ts_updates
                       LEFT OUTER JOIN %s.%s ON (%s.%s.ts_key = ts_updates.ts_key)
                       WHERE %s.%s.ts_key IS NULL;
                       
                       COMMIT;",
                       val,
                       schema,tbl,
                       schema,tbl,
                       schema,tbl,
                       schema,tbl,
                       schema,tbl,
                       schema,tbl,
                       schema,tbl,
                       schema,tbl
  )
  class(sql_query) <- "SQL"
  sql_query
}

.queryGetExistingKeys <- function(keys,validity,tbl,schema){
  vals <- paste(paste0("('",
                         paste(keys,
                               validity,
                               sep="','"),
                         "')"),
                  collapse = ",")
  
  sql_query <- sprintf("
                       CREATE TEMPORARY TABLE 
                       ts_search(ts_key varchar, validity daterange)
                       ON COMMIT DROP;
                       
                       INSERT INTO ts_search(ts_key, validity) VALUES %s;
                       
                       SELECT tm.ts_key FROM %s.%s tm
                       JOIN ts_search ts ON (tm.ts_key = ts.ts_key);",
                       vals,
                       schema,tbl)
  class(sql_query) <- "SQL"
  sql_query
}
