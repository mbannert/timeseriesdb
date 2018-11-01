#' Read Meta Information from a Time Series Database
#' 
#' This function reads meta information from a timeseriesdb package PostgreSQL
#' database and puts into a meta information environment. 
#' 
#' @param con PostgreSQL connection object
#' @param series character name of a time series object.
#' @param locale character denoting the locale of the meta information that is queried.
#' defaults to 'de' for German. At the KOF Swiss Economic Institute meta information should be available
#' als in English 'en', French 'fr' and Italian 'it'. Set the locale to NULL to query unlocalized meta information. 
#' @param tbl character name of the table that contains meta information. Defaults to 'meta_data_localized'.
#' Choose meta 'meta_data_unlocalized' when locale is set to NULL. 
#' @param schema SQL schema name. Defaults to timeseries.
#' @param as_list Should the result be returned as a tsmeta.list instead of a tsmeta.dt? Default TRUE
#' @export 
readMetaInformation <- function(con,
                                series,
                                locale = 'de',
                                tbl = 'meta_data_localized',
                                schema = 'timeseries',
                                as_list = TRUE){
  
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
    
    if(nrow(res) == 0) {
      stop(sprintf("None of the provided series were found in %s.%s", schema, tbl))
    }
    
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
    
    if(nrow(res) == 0) {
      stop(sprintf("None of the provided series were found in %s.%s", schema, tbl))
    }
    
    meta_list <- res[, {
      md = jsonlite::fromJSON(meta_data)
      md$md_generated_by <- md_generated_by
      md$md_resource_last_update <- md_resource_last_update
      md$md_coverage_temp <- md_coverage_temp
      .(meta_data = list(md))
    }, by = ts_key][, meta_data]
    names(meta_list) <- res[, ts_key]
  }
  
  meta_list <- rapply(meta_list, stringSafeAsNumeric, how = "list")
  
  # TODO: if(!is.null(meta_env)) { merge meta_env and meta_list }
  # For backwards comp
  if(as_list) {
    out <- as.tsmeta.list(meta_list)
  } else {
    out <- as.tsmeta.dt(meta_list)
  }
  
  if(!is.null(locale)) {
    attributes(out) <- c(attributes(out), list(locale = locale))
    
    if(as_list) {
      for(i in seq_along(out)) {
        attributes(out[[i]]) <- c(attributes(out[[i]]), list(locale = locale))
      }
    }
  }
  
  out
}

