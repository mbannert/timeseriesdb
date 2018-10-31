#' Update Meta Information Records
#' 
#' When a time series is stored to the database by \code{\link{storeTimeSeries}} 
#' a minimal unlocalized (i.e. untranslatable) meta information record is being 
#' generated. This meta information can be supplement using the storeMetaInformation
#' method.
#' 
#' @param meta a tsmeta.list or tsmets.dt object
#' @param con a PostgreSQL connection object
#' @param schema character name of the schema to write to. Defaults to 'timeseries'.
#' @param tbl character name of the meta information table to write to. 
#' Defaults to 'meta_data_unlocalized'.
#' @param locale character iso 2 digit locae description. Defaults to NULL.
#' @param keys character vector of time series. If specified only the selected 
#' meta information is stored. Defaults to NULL which stores all meta information
#' records in the environment.
#' @param quiet logical should function be quiet instead of returning a message when done? Defaults to FALSE.  
#' @param chunksize integer max size of chunks to split large query in. 
#' @export
storeMetaInformation <- function(meta,con,
                          schema = "timeseries",
                          tbl = "meta_data_unlocalized",
                          locale = NULL,
                          keys = NULL,
                          quiet = FALSE,
                          chunksize = 10000) {
  UseMethod("storeMetaInformation")
} 

#' @rdname storeMetaInformation
#' @export
storeMetaInformation.meta_env <- function(meta,con,
                                           schema = "timeseries",
                                           tbl = "meta_data_unlocalized",
                                           locale = NULL,
                                           keys = NULL,
                                           quiet = FALSE,
                                           chunksize = 10000){
  
  l <- as.list(meta)
 
  # Minimal sanitizer to avoid trouble
  # when meta information gets too crazy... maybe need to escape things.
  # Did you really name your son Robert); DROP table students?
  l <- lapply(l,function(x){
    san <- lapply(x, gsub, pattern="'DROP|DELETE|TRUNCATE|UPDATE|",
                  replacement = "", ignore.case = T)
    class(san) <- c('miro','list')
    san
  })
  
  
  json <- lapply(l, createJSON)

  md_df <- data.frame(ts_key = names(json),
                      meta_data = unlist(json),
                      stringsAsFactors = F)
    
  storeMetaInformation.data.table(md_df, con, schema, tbl, locale, keys, quiet, chunksize)
}

#' @export
storeMetaInformation.list <- storeMetaInformation.meta_env

#' @export
storeMetaInformation.tsmeta.list <- storeMetaInformation.meta_env

#' @export
storeMetaInformation.tsmeta.dt <- function(meta,
                                             con,
                                             schema = "timeseries",
                                             tbl = "meta_data_unlocalized",
                                             locale = NULL,
                                             keys = NULL,
                                             quiet = FALSE,
                                             chunksize = 10000) {
  tbl <- paste(schema, tbl, sep=".")
  
  meta <- createJSON(meta)
  
  if(!is.null(keys)) meta <- meta[meta$ts_key %in% keys,]
  
  if(is.null(locale)){
    query_meta_data_create <- sprintf("BEGIN;
                                      CREATE TEMPORARY TABLE 
                                      md_updates(ts_key varchar,
                                      meta_data json) ON COMMIT DROP;")
    
    query_meta_data_insert <- "COPY md_updates FROM STDIN;"
    
    
    query_meta_data_update <- sprintf("LOCK TABLE %s IN EXCLUSIVE MODE;
                                      UPDATE %s
                                      SET meta_data = md_updates.meta_data
                                      FROM md_updates
                                      WHERE md_updates.ts_key = %s.ts_key;
                                      COMMIT;",
                                      tbl,
                                      tbl,
                                      tbl)
  } else {
    meta$locale <- locale
    
    # Columns in DF are c("ts_key", "meta_data", "locale"), table must reflect that
    # See #55
    query_meta_data_create <- sprintf("BEGIN;
                                      CREATE TEMPORARY TABLE 
                                      md_updates(ts_key varchar,
                                      meta_data json,
                                      locale varchar) ON COMMIT DROP;")
    
    query_meta_data_insert <- "COPY md_updates FROM STDIN;"
    
    # localized meta information does not HAVE to exist, which 
    # means we have to have an insert here!  
    query_meta_data_update <- sprintf("LOCK TABLE %s IN EXCLUSIVE MODE;
                                      UPDATE %s
                                      SET meta_data = md_updates.meta_data,
                                      locale_info = md_updates.locale
                                      FROM md_updates
                                      WHERE md_updates.ts_key = %s.ts_key
                                      AND md_updates.locale = %s.locale_info;
                                      
                                      ---
                                      INSERT INTO %s
                                      SELECT md_updates.ts_key,
                                      md_updates.locale,
                                      md_updates.meta_data
                                      FROM md_updates
                                      LEFT OUTER JOIN %s 
                                      ON %s.ts_key = md_updates.ts_key
                                      AND %s.locale_info = md_updates.locale
                                      WHERE %s.ts_key IS NULL 
                                      AND %s.locale_info IS NULL;
                                      COMMIT;",
                                      tbl, tbl, tbl, tbl,
                                      tbl, tbl, tbl, tbl, tbl, tbl)
  }
  
  class(query_meta_data_update) <- "SQL"
  class(query_meta_data_insert) <- "SQL"
  
  md_create <- DBI::dbGetQuery(con,query_meta_data_create)
  pgCopyDf(con, meta, q = query_meta_data_insert, chunksize = chunksize)
  md_ok2 <- DBI::dbGetQuery(con,query_meta_data_update)
  if(!quiet) {
    if(is.null(md_ok2)) cat("Meta information updated.")  
  }
}

#' @export
storeMetaInformation.data.table <- function(meta,
                                             con,
                                             schema = "timeseries",
                                             tbl = "meta_data_unlocalized",
                                             locale = NULL,
                                             keys = NULL,
                                             quiet = FALSE,
                                             chunksize = 10000) {
  storeMetaInformation.tsmeta.dt(as.tsmeta.dt(meta), con, schema, tbl, locale, keys, quiet, chunksize)
}

#' @export
updateMetaInformation(meta,
                      con,
                      schema = "timeseries",
                      tbl = "meta_data_unlocalized",
                      locale = NULL,
                      keys = NULL,
                      quiet = FALSE,
                      chunksiize = 10000) {
  warning("updateMetaInformation is deprecated. Please use storeMetaInformation instead.")
  storeMetaInformation(meta, con, schema, tbl, locale, keys, quiet, chunksize)
}