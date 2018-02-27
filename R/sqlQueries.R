# localized meta information does not HAVE to exist, which 
# means we have to have an insert here!  
.queryStoreNoVintage <- function(val,schema,tbl){
  sql_query <- sprintf("BEGIN;
                       -- temp table & join is much faster than where in clause
                       CREATE TEMPORARY TABLE 
                       ts_updates(ts_key varchar, 
                       ts_data hstore,
                       ts_frequency integer,
                       ts_release_date timestamp with time zone DEFAULT '1900-01-01 00:00:00')
                       ON COMMIT DROP;
                       
                       INSERT INTO ts_updates(ts_key,
                       ts_data,
                       ts_frequency,
                       ts_release_date) VALUES %s;
                       LOCK TABLE %s.%s IN EXCLUSIVE MODE;
                       
                       -- Update existing entries
                       UPDATE %s.%s
                       SET ts_data = ts_updates.ts_data,
                       ts_frequency = ts_updates.ts_frequency,
                       ts_release_date = ts_updates.ts_release_date
                       FROM ts_updates
                       WHERE ts_updates.ts_key = %s.%s.ts_key;
                       
                       -- Add new entries
                       INSERT INTO %s.%s
                       SELECT ts_updates.ts_key,
                       ts_updates.ts_data,
                       ts_updates.ts_frequency,
                       ts_updates.ts_release_date
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

.queryStoreVintage <- function(val,schema,tbl,vintage_date){
  sql_query <- sprintf("BEGIN;
                       CREATE TEMPORARY TABLE 
                       ts_updates(ts_key text, 
                       ts_validity daterange,
                       ts_data hstore,
                       ts_frequency integer,
                       ts_release_date timestamp with time zone DEFAULT '1900-01-01 00:00:00')
                       ON COMMIT DROP;
                       
                       INSERT INTO ts_updates(ts_key,
                       ts_validity,
                       ts_data,
                       ts_frequency,
                       ts_release_date) VALUES %s;
                       LOCK TABLE %s.%s IN EXCLUSIVE MODE;
                       
                       -- Update existing entries
                       -- (Note: dependency will
                       -- be updated automatically through FK)
                       -- Use coalesce because lower statement produces NULL
                       UPDATE %s.%s
                       SET ts_validity = ('['|| 
                       COALESCE(lower(%s.ts_validity):: TEXT,'') ||
                       ','|| 
                       COALESCE(lower(ts_updates.ts_validity) :: TEXT,'') ||
                       ')'):: DATERANGE
                       FROM ts_updates
                       WHERE ts_updates.ts_key = %s.ts_key
                       AND upper_inf(%s.ts_validity);
                       
                       -- Add new entries
                       INSERT INTO %s.%s 
                       SELECT ts_updates.ts_key,
                       ts_updates.ts_validity,
                       ts_updates.ts_data,
                       ts_updates.ts_frequency,
                       ts_updates.ts_release_date
                       FROM ts_updates;
                       COMMIT;",
                       val,
                       schema,tbl, # LOCK TABLE
                       schema,tbl, # UPDATE
                       tbl, # COALESCE
                       tbl, # WHERE
                       tbl, # AND
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


.queryStoreMeta <- function(vals,schema){
  sql_query <- sprintf("BEGIN;
                       CREATE TEMPORARY TABLE 
                       md_updates(ts_key varchar, md_generated_by varchar,
                       md_resource_last_update timestamptz,
                       md_coverage_temp varchar, meta_data hstore) ON COMMIT DROP;
                       
                       INSERT INTO md_updates(ts_key, md_generated_by,
                       md_resource_last_update,
                       md_coverage_temp) VALUES %s;
                       LOCK TABLE %s.meta_data_unlocalized IN EXCLUSIVE MODE;
                       
                       UPDATE %s.meta_data_unlocalized
                       SET md_generated_by = md_updates.md_generated_by,
                       md_resource_last_update = md_updates.md_resource_last_update,
                       md_coverage_temp = md_updates.md_coverage_temp
                       FROM md_updates
                       WHERE md_updates.ts_key = %s.meta_data_unlocalized.ts_key;
                       
                       INSERT INTO %s.meta_data_unlocalized
                       SELECT md_updates.ts_key, md_updates.md_generated_by,
                       md_updates.md_resource_last_update,
                       md_updates.md_coverage_temp
                       FROM md_updates
                       LEFT OUTER JOIN %s.meta_data_unlocalized
                       ON (%s.meta_data_unlocalized.ts_key = md_updates.ts_key)
                       WHERE %s.meta_data_unlocalized.ts_key IS NULL;
                       COMMIT;",
                       vals, schema, schema, schema, schema, schema, schema, schema)
  class(sql_query) <- "SQL"
  sql_query
}

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


