#' Store Meta Information to the Database
#' 
#' This function stores meta information to the database for a given time series.
#' Make sure that corresponding time series had been inserted to the main table before. 
#' 
#' @param series a character name of an time series object
#' @param con a PostgreSQL connection object
#' @param tbl name of the meta information table, defaults to localized meta data: meta_data_localized. Alternatively choose meta_data_unlocalized if you are not translating meta information.
#' @param lookup_env name of the R environment in which to look for meta information objects
#' @param overwrite logical, TRUE
#' @param localized logical is meta information localized. Defaults to TRUE.
#' @export
storeMetaInformation <- function(series,
                                 con = options()$TIMESERIESDB_CON,
                                 tbl = 'meta_data_localized',
                                 lookup_env = 'meta_data_localized',
                                 overwrite = T,
                                 localized = T){
  
  if(is.null(con)) stop('Default TIMESERIESDB_CON not set in options() or no proper connection given to the con argument.')
  
  # get an object from the meta environment
  mi <- get(series,envir = get(lookup_env))
  
  # creata a list of hstores
  hstore_list <- createHstore(mi)
  
  # lapply a write hstore to db
  if(overwrite){
    out <- lapply(names(hstore_list),function(x){
      if(localized){
        sql_query <- sprintf("INSERT INTO %s (ts_key,locale_info,meta_data) VALUES 
                         ('%s','%s','%s')",tbl,series,x,hstore_list[[x]])
      } else {
        sql_query <- sprintf("UPDATE %s SET meta_data = '%s' WHERE ts_key = '%s'",
                             tbl,hstore_list[[x]],series)
      }
      
      # return proper status messages for every lang
      if(is.null(DBI::dbGetQuery(con,sql_query))){
        paste0(x,' meta information successfully written.')
      } else{
        paste0(x,' meta information fail.')
      }
    }
    )
    
    cat(paste0(unlist(out),collapse = " \n"))
    
  }  
  
}

