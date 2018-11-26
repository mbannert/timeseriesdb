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
#' @param tbl_localized character name of the table that contains localized meta information. Defaults to 'meta_data_localized'.
#' @param tbl_unlocalized character name of the table that contains general meta information. Defaults to 'meta_data_unlocalized'.
#' @param schema SQL schema name. Defaults to timeseries.
#' @param as_list Should the result be returned as a tsmeta.list instead of a tsmeta.dt? Default TRUE
#' @export 
readMetaInformation <- function(con,
                                series,
                                locale = 'de',
                                tbl_localized = 'meta_data_localized',
                                tbl_unlocalized = 'meta_data_unlocalized',
                                schema = 'timeseries',
                                as_list = TRUE){
  
  pg_series <- paste(sprintf("('%s')", series), collapse = ",")
  
  # JOIN is much faster than WHERE IN with many keys
  query_create <- sprintf("
                                BEGIN;
                                CREATE TEMPORARY TABLE meta_read (ts_key text PRIMARY KEY) ON COMMIT DROP;
                                INSERT INTO meta_read(ts_key) VALUES %s;",
                          pg_series)
  
  query_mdul <- sprintf("
                                SELECT *
                                FROM (
                                SELECT tm.ts_key, tm.md_generated_by, tm.md_resource_last_update, md_coverage_temp, meta_data::text
                                FROM %s.%s tm
                                JOIN meta_read tr
                                ON (tm.ts_key = tr.ts_key)
                                ) t;",
                        schema, tbl_unlocalized)
  
  
  query_mdl <- sprintf("
                                SELECT *
                                FROM (
                                  SELECT tm.ts_key, meta_data::text
                                  FROM %s.%s tm
                                  JOIN meta_read tr
                                  ON (tm.ts_key = tr.ts_key AND locale_info = '%s')
                                ) t;",
                          schema, tbl_localized, locale)
  
  # Need a helper to get proper results where meta_data is NA
  expand_meta <- function(json) {
    if(!is.na(json)) {
      jsonlite::fromJSON(json)
    } else {
      jsonlite::fromJSON("{}")
    }
  }
  
  runDbQuery(con, query_create)
  
  mdul <- as.data.table(runDbQuery(con, query_mdul))
  
  mdl <- as.data.table(runDbQuery(con, query_mdl))
  
  commitTransaction(con)
  
  if(nrow(mdul) > 0) {
    mdul_meta_expanded <- mdul[, rbindlist(lapply(meta_data, expand_meta), fill = TRUE, idcol = TRUE)]
    
    # We don't need the mets_data column anymore
    if(nrow(mdul_meta_expanded) > 0) {
      md <- mdul_meta_expanded[mdul[, .id := 1:.N][, -"meta_data"], on = .(.id)][, -".id"]
    } else {
      md <- mdul[, -"meta_data"]
    }
    
    if(nrow(mdl) > 0) {
      mdl_meta_expanded <- mdl[, rbindlist(lapply(meta_data, expand_meta), fill = TRUE, idcol = TRUE)]
      
      mdl <- mdl_meta_expanded[mdl[, .id := 1:.N][, -"meta_data"], on = .(.id)][, -".id"]
      
      # ts_key should appear on the left
      setcolorder(mdl, c("ts_key"))
      
      md <- merge(md, mdl, all.x = TRUE)
    }
    
    # Attach missing series
    md <- md[series, , on = "ts_key"]
    
    if(as_list) {
      out <- as.tsmeta.list(md)
    } else {
      out <- as.tsmeta.dt(md)
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
  } else {
    if(as_list) {
      out <- as.tsmeta.list(lapply(series, function(x){list()}))
      names(out) <- series
      out
    } else {
      data.table(ts_key = series)
    }
  }
}

