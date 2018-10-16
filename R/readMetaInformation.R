#' Read Meta Information from a Time Series Database
#' 
#' This function reads meta information from a timeseriesdb package PostgreSQL
#' database and puts into a meta information environment. 
#' 
#' @param series character name of a time series object.
#' @param con PostgreSQL connection object
#' @param overwrite_objects logical should the entire object for a key be overwritten. Defaults to FALSE.
#' @param overwrite_elements logical should single elements inside the environment be overwritten. Defaults to TRUE.
#' @param locale character denoting the locale of the meta information that is queried.
#' defaults to 'de' for German. At the KOF Swiss Economic Institute meta information should be available
#' als in English 'en', French 'fr' and Italian 'it'. Set the locale to NULL to query unlocalized meta information. 
#' @param tbl character name of the table that contains meta information. Defaults to 'meta_data_localized'. Choose meta 'meta_data_unlocalized' when locale is set to NULL. 
#' @param meta_env environment to which the meta information should be added. Defaults to NULL. In this case an environment will be returned. If you run this function in a loop best create an empty environment before the loop or apply call and pass the environment to this function. By doing so new elements will be added to the environment. 
#' @param schema SQL schema name. Defaults to timeseries.
#' @export 
readMetaInformation <- function(series,
                                con,
                                locale = 'de',
                                tbl = 'meta_data_localized',
                                overwrite_objects = F,
                                overwrite_elements = T,
                                meta_env = NULL,
                                schema = 'timeseries'){
  
  series <- paste(paste0("('", series, "')"), collapse=",")
  
  if(!is.null(locale)){
    
    read_SQL <-
      sprintf("
              BEGIN;
              CREATE TEMPORARY TABLE meta_read (ts_key text PRIMARY KEY) ON COMMIT DROP;
              INSERT INTO meta_read(ts_key) VALUES %s;
              
              SELECT *
              FROM (
              SELECT tm.ts_key, meta_data::text
              FROM %s.%s tm
              JOIN meta_read tr
              ON (tm.ts_key = tr.ts_key AND locale_info = '%s')
              ) t;",
              series, schema, tbl, locale)
    
    res <- as.data.table(dbGetQuery(con, read_SQL))
    commitTransaction(con)
    
    meta_list <- res[, .(meta_data = list(jsonlite::fromJSON(meta_data))), by = ts_key][, meta_data]
    names(meta_list) <- res[, ts_key]
    
  } else {
    # sanity check
    if(tbl != 'meta_data_unlocalized') {
      warning('DB table is not set to unlocalized, though locale is set!')
    }
    
    read_SQL <-
      sprintf("
              BEGIN;
              CREATE TEMPORARY TABLE meta_read (ts_key text PRIMARY KEY) ON COMMIT DROP;
              INSERT INTO meta_read(ts_key) VALUES %s;
              
              SELECT *
              FROM (
              SELECT tm.ts_key, tm.md_generated_by, tm.md_resource_last_update, md_coverage_temp, meta_data::text
              FROM %s.%s tm
              JOIN meta_read tr
              ON (tm.ts_key = tr.ts_key)
              ) t;",
              series, schema, tbl)
    
    res <- as.data.table(dbGetQuery(con, read_SQL))
    commitTransaction(con)
    
    meta_list <- res[, {
      md = jsonlite::fromJSON(meta_data)
      md$md_generated_by <- md_generated_by
      md$md_resource_last_update <- md_resource_last_update
      md$md_coverage_temp <- md_coverage_temp
      .(meta_data = list(md))
    }, by = ts_key][, meta_data]
    names(meta_list) <- res[, ts_key]
  }
  
  # TODO: if(!is.null(meta_env)) { merge meta_env and meta_list }
  # For backwards comp
  meta_list
}

