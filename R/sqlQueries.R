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

.queryBulkUpsert <- function(tbl){
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


.queryBulkInsertMeta <- function(tbl){
  sql_query <- "BEGIN;
                CREATE TEMPORARY TABLE 
                md_updates(ts_key varchar, md_generated_by varchar,
                md_resource_last_update timestamptz,
                md_coverage_temp varchar,
                meta_data hstore) 
                ON COMMIT DROP;
                COPY md_updates FROM STDIN;"
  class(sql_query) <- "SQL"
  sql_query
}

.queryBulkUpsertMeta <- function(md_unlocal){
  sql_query <- sprintf("LOCK TABLE %s IN EXCLUSIVE MODE;
                        UPDATE %s
                        SET md_generated_by = md_updates.md_generated_by,
                        md_resource_last_update = md_updates.md_resource_last_update,
                        md_coverage_temp = md_updates.md_coverage_temp,
                        meta_data = md_updates.meta_data
                        FROM md_updates
                        WHERE md_updates.ts_key = %s.ts_key;
                       
                        INSERT INTO %s
                        SELECT md_updates.ts_key, md_updates.md_generated_by,
                        md_updates.md_resource_last_update,
                        md_updates.md_coverage_temp
                        FROM md_updates
                        LEFT OUTER JOIN %s
                        ON (%s.ts_key = md_updates.ts_key)
                        WHERE %s.ts_key IS NULL;
                        COMMIT;",
                       md_unlocal, md_unlocal, md_unlocal, md_unlocal,
                       md_unlocal, md_unlocal, md_unlocal)
  class(sql_query) <- "SQL"
  sql_query
}


