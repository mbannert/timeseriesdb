#' Write a vintages of time series to the database
#' 
#' This function uses INSERT INTO instead of the more convenient dbWritetable for performance reasons. DO NOT USE THIS FUNCTIONS IN LOOPS OR LAPPLY! This function can handle a set of time series on its own and is much faster than looping over a list. Non-unique primary keys are overwritten !
#' 
#' @author Matthias Bannert
#' @param series character name of a time series, S3 class ts. When used with lists it is convenient to set series to names(li). Note that the series name needs to be unique in the database!
#' @param con a PostgreSQL connection object.
#' @param li list of time series. Defaults to NULL to no break legacy calls that use lookup environments.
#' @param tbl character string denoting the name of the main time series table in the PostgreSQL database.
#' @param md_unlocal character string denoting the name of the table that holds unlocalized meta information.
#' @param overwrite logical should existing records (same primary key) be overwritten? Defaults to TRUE.
#' @param schema SQL schema name. Defaults to timeseries. 
#' @importFrom DBI dbGetQuery
#' @export
storeVintages <- function(series,
                          con,
                          vintage_range,
                          li,
                          store_freq = T,
                          tbl = "vintages",
                          md_unlocal = "vintages_meta_data",
                          overwrite = T,
                          schema = "timeseries"){
  # subset a list series 
  li <- li[series]
  
  # avoid overwriting
  if(!overwrite){
    db_keys <- DBI::dbGetQuery(con,sprintf("SELECT ts_key FROM %s",tbl))$ts_key
    series <- series[!(series %in% db_keys)]
    li <- li[series]
  }
  
  # CREATE ELEMENTS AND RECORDS ---------------------------------------------
  # use the form (..record1..),(..record2..),(..recordN..)
  # to be able to store everything in one big query
  
  keep <- sapply(li,function(x) inherits(x,c("ts","zoo","xts")))
  dontkeep <- !keep
  
  if(all(keep)){
    NULL #cat("No corrupted series found. \n")
  } else {
    cat("These series caused problems", names(series[dontkeep]),"\n")  
  }
  
  
  li <- li[keep]
  
  hstores <- unlist(lapply(li,createHstore))
  freqs <- sapply(li,function(x) {
    ifelse(inherits(x,"zoo"),'NULL',frequency(x))
  })
  
  vintage_range <- paste0(vintage_range,"::tstzrange")
  
  if(!store_freq){
    values <- paste(paste0("('",
                           paste(series,
                                 vintage_range,
                                 hstores,
                                 sep="','"),
                           "')"),
                    collapse = ",")
  } else {
    values <- paste(paste0("('",
                           paste(series,
                                 vintage_range,
                                 hstores,
                                 freqs,
                                 sep="','"),
                           "')"),
                    collapse = ",")
  }
  
  values <- gsub("''","'",values)
  values <- gsub("::hstore'","::hstore",values)
  values <- gsub("'NULL'","NULL",values)
  values <- gsub("::tstzrange'","'::tstzrange",values)
  
  # add schema name
  tbl <- paste(schema,tbl,sep = ".")
  md_unlocal <- paste(schema,md_unlocal,sep = ".")
  
  # CREATE META INFORMATION -------------------------------------------------
  # automatically generated meta information
  md_generated_by <- Sys.info()["user"]
  md_resource_last_update <- Sys.time()
  md_coverages <- unlist(lapply(li,function(x){
    sprintf('%s to %s',
            min(zooLikeDateConvert(x)),
            max(zooLikeDateConvert(x))
    )}
  ))
  
  # same trick as for data itself, one query
  md_values <- paste(paste0("('",
                            paste(series,
                                  vintage_range,
                                  md_generated_by,
                                  md_resource_last_update,
                                  md_coverages,
                                  sep = "','"),
                            "')"),
                     collapse = ",")
  
  md_values <- gsub("::tstzrange'","'::tstzrange",md_values)
  
  
  # SQL STATEMENTS ---------------------------------------------------------- 
  # we use the state for store frequency here because it is the easiest
  # way to store NULL values in the PostgreSQL table with the bulk
  # optimized process, just note the missing frequency in the insert
  # statement of the update table causing the NULL. 
  sql_query_data <- sprintf("BEGIN;
                            CREATE TEMPORARY TABLE ts_updates(
                            ts_key varchar,
                            vintage_range tstzrange,
                            ts_data hstore, ts_frequency integer) ON COMMIT DROP;
                            
                            INSERT INTO ts_updates(
                            ts_key, vintage_range,
                            ts_data,
                            ts_frequency)
                            VALUES %s;
                            
                            LOCK TABLE %s IN EXCLUSIVE MODE;
                            UPDATE %s
                            SET ts_data = ts_updates.ts_data,
                            ts_frequency = ts_updates.ts_frequency
                            FROM ts_updates
                            WHERE ts_updates.ts_key = %s.ts_key
                            AND ts_updates.vintage_range = %s.vintage_range;
                            
                            INSERT INTO %s
                            SELECT ts_updates.ts_key,
                            ts_updates.vintage_range,
                            ts_updates.ts_data,
                            ts_updates.ts_frequency
                            FROM ts_updates
                            LEFT OUTER JOIN %s ON (%s.ts_key = ts_updates.ts_key 
                            AND %s.vintage_range = ts_updates.vintage_range)
                            WHERE %s.ts_key IS NULL;
                            COMMIT;",
                            values,
                            tbl, tbl, tbl,tbl,
                            tbl, tbl, tbl, tbl, tbl)
  
  sql_query_meta_data <- sprintf("BEGIN;
                                 CREATE TEMPORARY TABLE md_updates(
                                 ts_key varchar,
                                 vintage_range tstzrange,
                                 md_generated_by varchar,
                                 md_resource_last_update timestamptz,
                                 md_coverage_temp varchar,
                                 meta_data hstore) ON COMMIT DROP;

                                 INSERT INTO md_updates(
                                 ts_key, vintage_range,
                                 md_generated_by,
                                 md_resource_last_update,
                                 md_coverage_temp) VALUES %s;

                                 LOCK TABLE %s IN EXCLUSIVE MODE;
                                 UPDATE %s
                                 SET md_generated_by = md_updates.md_generated_by,
                                 md_resource_last_update = md_updates.md_resource_last_update,
                                 md_coverage_temp = md_updates.md_coverage_temp
                                 FROM md_updates
                                 WHERE md_updates.ts_key = %s.ts_key
                                 AND md_updates.vintage_range = %s.vintage_range;

                                 INSERT INTO %s
                                 SELECT md_updates.ts_key,
                                 md_updates.vintage_range,
                                 md_updates.md_generated_by,
                                 md_updates.md_resource_last_update,
                                 md_updates.md_coverage_temp
                                 FROM md_updates
                                 LEFT OUTER JOIN %s ON (%s.ts_key = md_updates.ts_key 
                                                        AND %s.vintage_range = md_updates.vintage_range)
                                 WHERE %s.ts_key IS NULL;
                                 COMMIT;",
                                 md_values,
                                 md_unlocal, md_unlocal, md_unlocal, md_unlocal,
                                 md_unlocal, md_unlocal,md_unlocal, md_unlocal,md_unlocal
  )
  
  main_ok <- DBI::dbGetQuery(con,sql_query_data)
  md_ok <- DBI::dbGetQuery(con,sql_query_meta_data)
  
  l <- length(li)
  
  # if(is.null(main_ok) & is.null(md_ok)){
  #   paste0(l, " data and meta data records written successfully.")
  # } else {
  #   paste("An error occured, data could not be written properly. Check the database")
  # } 
}

