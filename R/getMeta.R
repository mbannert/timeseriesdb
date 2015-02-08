#' Quickly Handle Meta Information
#' 
#' Sometimes reading the entire meta description for all language or
#' multiple time series might not be necessary. Quick handle operators
#' help users to access the information quickly as a non-nested list 
#' for only one language is returned. These functions are alpha status, 
#' more will follow. 
#'
#' 
#' @param series an R time series object
#' @param lang character name of the language of the meta information. Typically 'de', 'it', 'fr' or 'en'.
#' @param con connection object, defaults to object set by options().
#' @param tbl character name of the table that contains the meta information. 
#' @export
getMeta <- function(series, lang, con = get(Sys.getenv("TIMESERIESDB_CON")),
                    tbl = 'meta_data_localized'){
  if(class(con) != "PostgreSQLConnection") stop('Default TIMESERIESDB_CON not set in Sys.getenv or no proper connection given to the con argument. con is not a PostgreSQLConnection obj.')
  ts <- deparse(substitute(series))
  sql_statement <- sprintf("SELECT (each(meta_data)).key,
                             (each(meta_data)).value
                           FROM %s WHERE ts_key = '%s' AND locale_info = '%s'",
                           tbl,ts,lang)
  res <- DBI::dbGetQuery(con,sql_statement)
  ll <- as.list(res$value)
  names(ll) <- res$key
  ll
  
}

