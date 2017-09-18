# store data #################
.queryBulkInsert <- function(){
  sql_query <- sprintf("BEGIN;
                         CREATE TEMPORARY TABLE 
                         ts_updates(ts_key varchar,
                                    ts_data hstore,
                                    ts_frequency integer) ON COMMIT DROP;
                         COPY ts_updates FROM STDIN;")
  class(sql_query) <- "SQL"
  sql_query
}

.queryBulkUpsert <- function(schema,tbl){
  sql_query <- sprintf("LOCK TABLE %s IN EXCLUSIVE MODE;
                                  UPDATE %s
                                  SET ts_data = ts_updates.ts_data,
                                  ts_frequency = ts_updates.ts_frequency
                                  FROM ts_updates
                                  WHERE ts_updates.ts_key = %s.ts_key;
                                  
                                  ---
                                  INSERT INTO %s
                                  SELECT ts_updates.ts_key,
                                  ts_updates.ts_data,
                                  ts_updates.ts_frequency
                                  FROM ts_updates
                                  LEFT OUTER JOIN %s 
                                  ON %s.ts_key = ts_updates.ts_key
                                  WHERE %s.ts_key IS NULL;
                                  COMMIT;",
                       tbl, tbl, tbl,
                       tbl, tbl, tbl, tbl)
  class(sql_query) <- "SQL"
  sql_query
}

