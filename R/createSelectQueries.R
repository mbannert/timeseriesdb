.queryStoreNoVintage <- function(val,schema,tbl){
  sql_query <- sprintf("BEGIN;
                      -- temp table & join is much faster than where in clause
                       CREATE TEMPORARY TABLE 
                       ts_updates(ts_key varchar, 
                                  ts_data hstore,
                                  ts_frequency integer)
                       ON COMMIT DROP;
                       
                       INSERT INTO ts_updates(ts_key,
                                              ts_data,
                                              ts_frequency) VALUES %s;
                       LOCK TABLE %s.%s IN EXCLUSIVE MODE;
                       
                       -- Update existing entries
                       UPDATE %s.%s
                       SET ts_data = ts_updates.ts_data,
                       ts_frequency = ts_updates.ts_frequency
                       FROM ts_updates
                       WHERE ts_updates.ts_key = %s.%s.ts_key;
                       
                       -- Add new entries
                       INSERT INTO %s.%s
                              SELECT ts_updates.ts_key,
                                     ts_updates.ts_data,
                                     ts_updates.ts_frequency
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

.queryStoreVintage <- function(val,schema,tbl){
  sql_query <- sprintf("BEGIN;
                       CREATE TEMPORARY TABLE 
                       ts_updates(ts_key text, 
                                  ts_validity daterange,
                                  ts_data hstore,
                                  ts_frequency integer);
                       
                       INSERT INTO ts_updates(ts_key,
                                              ts_validity,
                                              ts_data,
                                              ts_frequency) VALUES %s;
                       LOCK TABLE %s.%s IN EXCLUSIVE MODE;
                       
                       -- Update existing entries
                       -- (Note: dependency will
                       -- be updated automatically through FK)
                       -- Use coalesce because lower statement produces NULL
                       UPDATE %s.%s
                       SET ts_validity = ('['||coalesce(lower(ts_updates.ts_validity)::TEXT,'')||
                              ','||coalesce(upper(ts_updates.ts_validity)::TEXT,'')||
                              ')')::DATERANGE
                       FROM ts_updates
                       WHERE ts_updates.ts_key = %s.ts_key
                       AND %s.ts_validity @> CURRENT_DATE;

                       -- Add new entries
                       INSERT INTO %s.%s 
                       SELECT ts_updates.ts_key,
                              ts_updates.ts_validity,
                              ts_updates.ts_data,
                              ts_updates.ts_frequency
                       FROM ts_updates;",
                       val,
                       schema,tbl,
                       schema,tbl,
                       tbl,
                       tbl,
                       schema,tbl
                       )
  class(sql_query) <- "SQL"
  sql_query
}


.queryGetExistingKeys <- function(keys,tbl,schema){
  vals <- paste(paste0("('",
                         paste(keys,sep="','"),
                         "')"),
                  collapse = ",")
  
  sql_query <- sprintf("
                       CREATE TEMPORARY TABLE 
                       ts_search(ts_key varchar)
                       ON COMMIT DROP;
                       
                       INSERT INTO ts_search(ts_key) VALUES %s;
                       
                       SELECT tm.ts_key FROM %s.%s tm
                       JOIN ts_search ts ON (tm.ts_key = ts.ts_key);",
                       vals,
                       schema,tbl)
  class(sql_query) <- "SQL"
  sql_query
}

