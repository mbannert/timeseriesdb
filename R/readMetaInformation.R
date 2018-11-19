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
  
  pg_series <- sprintf("(%s)", paste(sprintf("'%s'", series), collapse = ","))
  
  query_mdul <- sprintf("
                  SELECT ts_key, md_generated_by, md_resource_last_update, md_coverage_temp, meta_data::text as meta_data
                  FROM %s.%s
                  WHERE ts_key in %s",
                        schema, tbl_unlocalized,
                        pg_series)

  query_mdl <- sprintf("
                      SELECT ts_key, meta_data::text 
                      FROM %s.%s
                      WHERE locale_info = '%s'
                      AND ts_key in %s",
                       schema, tbl_localized,
                       locale, pg_series)
  
  expand_meta <- function(json) {
    if(!is.na(json)) {
      jsonlite::fromJSON(json)
    } else {
      jsonlite::fromJSON("{}")
    }
  }
  

  mdul <- as.data.table(runDbQuery(con, query_mdul))
  
  mdul_meta_expanded <- mdul[, rbindlist(lapply(meta_data, expand_meta), fill = TRUE, idcol = TRUE)]
  if(nrow(mdul_meta_expanded) > 0) {
    mdul <- mdul_meta_expanded[mdul[, .id = 1:.N][, -"meta_data"], on = .(.id)][, -".id"]
  } else {
    mdul <- mdul[, -"meta_data"]
  }
  
  mdl <- as.data.table(runDbQuery(con, query_mdl))
  mdl_meta_expanded <- mdl[, rbindlist(lapply(meta_data, expand_meta), fill = TRUE, idcol = TRUE)]
  
  mdl <- mdl_meta_expanded[mdl[, .id := 1:.N][, -"meta_data"], on = .(.id)][, -".id"]
  setcolorder(mdl, c("ts_key"))
  
  md <- merge(mdul, mdl, all.x = TRUE)
  
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
}

