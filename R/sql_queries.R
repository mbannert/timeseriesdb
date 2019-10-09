library(RPostgres)
### CASE 1: Vintages, no release dates

runATest <- function(vintage) {
  con <- dbConnect(Postgres(), "postgres", "localhost", 1111, "pgpass", "postgres")
  
  dbWithTransaction(con, {
    
    # Why's this not work?
    # dbExecute(con, "
    #           DELETE FROM timeseries_1_0.timeseries_main
    #           WHERE release = $1
    #           AND lower(ts_validity) = $2
    #           AND upper_inf(ts_validity);",
    #           list(
    #             "test_release",
    #             vintage
    #           ))
    # 
    # dbExecute(con, "
    #           DELETE FROM timeseries_1_0.timeseries_main
    #           WHERE release = $1
    #           AND lower(ts_validity) = $2
    #           AND upper_inf(ts_validity);
    #           ",
    #           list(
    #             "test_release",
    #             vintage
    #           ))
    
    sql_a <- "
    UPDATE timeseries_1_0.releases
    SET ts_validity = daterange(lower(ts_validity), $1, '[)'),
    release_validity = tstzrange(lower(release_validity), $2, '[)')
    WHERE release = $3
    AND upper_inf(ts_validity);"
    
    dbExecute(con, sql_a, list(
      vintage,
      Sys.Date(),
      "test_release"
    ))
    
    sql_b <- "
    INSERT INTO timeseries_1_0.releases VALUES
    ($1, $2, tstzrange((SELECT max(upper(release_validity)) FROM timeseries_1_0.releases), null, '[)'), $3)"
    
    dbExecute(con, sql_b, list(
      "test_release",
      sprintf("[%s,)", vintage),
      "another release of test_release"
    ))
    
    sql_c <- "CREATE TEMPORARY TABLE ts_updates (LIKE timeseries_1_0.timeseries_main) ON COMMIT DROP"
    
    dbExecute(con, sql_c)
    
    sql_d <- "INSERT INTO ts_updates VALUES($1, daterange($2, null, '[)'), $3, $4, $5)"
    
    tsl <- tstools::generate_random_ts(2)
    class(tsl) <- c("tslist", "list")
    tsl <- to_ts_json(tsl)
    
    dbExecute(con, sql_d, list(
      names(tsl),
      rep(vintage, length(tsl)),
      unlist(tsl),
      rep("test_release", length(tsl)),
      rep("somebody_i_dont_care", length(tsl))
    ))
    
    sql_e <- "
    UPDATE timeseries_1_0.timeseries_main
    SET ts_validity = daterange(lower(timeseries_main.ts_validity), lower(ts_updates.ts_validity))
    FROM ts_updates
    WHERE ts_updates.ts_key = timeseries_main.ts_key
    AND upper_inf(timeseries_main.ts_validity);
    ";
    
    dbExecute(con, sql_e)
    
    sql_f <- "INSERT INTO timeseries_1_0.timeseries_main
    SELECT * FROM ts_updates;"
    
    dbExecute(con, sql_f)
    
    dbExecute(con, "DELETE FROM timeseries_1_0.timeseries_main WHERE release = $1 AND isempty(ts_validity)",
              list("test_release"))
    dbExecute(con, "DELETE FROM timeseries_1_0.releases WHERE release = $1 AND isempty(ts_validity)",
              list("test_release"))
  })
  
  dbDisconnect(con)
}

raeumAuf <- function() {
  con <- dbConnect(Postgres(), "postgres", "localhost", 1111, "pgpass", "postgres")
  dbExecute(con, "DELETE FROM timeseries_1_0.timeseries_main")
  dbExecute(con, "DELETE FROM timeseries_1_0.releases")
  dbDisconnect(con)
}

# -- Add new entries
# INSERT INTO %s.%s
# SELECT ts_updates.ts_key,
# ts_updates.ts_validity,
# ts_updates.ts_data,
# ts_updates.ts_frequency,
# ts_updates.ts_release_date
# FROM ts_updates;
# 
# 
# CREATE TEMPORARY TABLE ts_updates (LIKE timeseries_1_0.timeseries_main) ON COMMIT DROP;
# 
# -- links public validity on dataset level to series
# CREATE TABLE timeseries_1_0.releases(
#   release text,
#   ts_validity daterange,
#   release_validity tstzrange,
#   release_description text,
#   primary key (release, ts_validity)
# );
# 
# -- store different versions of time series
# CREATE TABLE timeseries_1_0.timeseries_main (
#   ts_key text,
#   ts_validity daterange,
#   ts_data json,
#   release text,
#   access text,
#   primary key (ts_key, ts_validity),
#   foreign key (release, ts_validity) references timeseries_1_0.releases
# );
# 
# .queryStoreVintage <- function(val,schema,tbl,vintage_date){
#   sql_query <- sprintf("BEGIN;
#                        CREATE TEMPORARY TABLE
#                        ts_updates(ts_key text,
#                        ts_validity daterange,
#                        ts_data hstore,
#                        ts_frequency integer,
#                        ts_release_date timestamp with time zone DEFAULT '1900-01-01 00:00:00')
#                        ON COMMIT DROP;
# 
#                        INSERT INTO ts_updates(ts_key,
#                        ts_validity,
#                        ts_data,
#                        ts_frequency,
#                        ts_release_date) VALUES %s;
#                        LOCK TABLE %s.%s IN EXCLUSIVE MODE;
# 
#                        -- Update existing entries
#                        -- (Note: dependency will
#                        -- be updated automatically through FK)
#                        -- Use coalesce because lower statement produces NULL
#                        UPDATE %s.%s
#                        SET ts_validity = ('['||
#                        COALESCE(lower(%s.ts_validity)::TEXT,'') ||
#                        ','||
#                        COALESCE(lower(ts_updates.ts_validity)::TEXT,'') ||
#                        ')')::DATERANGE
#                        FROM ts_updates
#                        WHERE ts_updates.ts_key = %s.ts_key
#                        AND upper_inf(%s.ts_validity);
# 
#                        -- Add new entries
#                        INSERT INTO %s.%s
#                        SELECT ts_updates.ts_key,
#                        ts_updates.ts_validity,
#                        ts_updates.ts_data,
#                        ts_updates.ts_frequency,
#                        ts_updates.ts_release_date
#                        FROM ts_updates;
#                        COMMIT;",
#                        val,
#                        schema,tbl, # LOCK TABLE
#                        schema,tbl, # UPDATE
#                        tbl, # COALESCE
#                        tbl, # WHERE
#                        tbl, # AND
#                        schema,tbl
#   )
#   class(sql_query) <- "SQL"
#   sql_query
# }