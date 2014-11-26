#' Quick Handle operators
#' 
#' Sometimes reading the entire meta description for all language or
#' multiple time series might not be necessary. Quick handle operators
#' help users to access the information quickly as a non-nested list 
#' for only one language is returned. These operators are alpha status, 
#' more will follow. 
#'
#' 
#' @param series an R time series object
#' @param lang character representation of a language, typically 'en', 'de', 'fr' or 'it' as this is a Swiss package.
#' @param con a Postgres Connection object, typically set in in options().
#' @param tbl character name of the table that contains localized meta information
#' @export
#' @rdname quick_handle
'%mdb%' <- function(series,lang,con = options()$TIMESERIESDB_CON,
                    tbl = 'meta_data_localized'){
  if(!exists('con')) stop('No standard TIMESERIESDB_CON set. Quick handle operators need standard connection. Use options() to set TIMESERIESDB_CON.')
  ts <- deparse(substitute(series))
  sql_statement <- sprintf("SELECT (each(meta_data)).key,
                             (each(meta_data)).value
                           FROM %s WHERE ts_key = '%s' AND locale_info = '%s'",
                           tbl,ts,lang)
  res <- dbGetQuery(con,sql_statement)
  ll <- as.list(res$value)
  names(ll) <- res$key
  ll
  
}

