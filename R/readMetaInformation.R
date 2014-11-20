#' Read Meta Information from a Time Series Database
#' 
#' This function reads meta information from timeseriesdb package PostgreSQL
#' database and puts into a meta information environment. 
#' 
#' @param series character name of a time series object.
#' @param con PostgreSQL connection object
#' @param overwrite logical should data be overwritten
#' @param type character representation of type of meta information, defaults to
#' localized.
#' @export 
readMetaInformation <- function(series,
                                con = options()$TIMESERIESDB_CON,
                                overwrite,type = "localized"){
  
  if(is.null(con)) stop('Default TIMESERIESDB_CON not set in options() or no proper connection given to the con argument.')
  
  
  if(type == 'localized'){
    meta_env = 'meta_localized'
    tbl = 'meta_data_localized'
    sql_statement <- sprintf("SELECT (each(meta_data)).key,
                             (each(meta_data)).value,
                             locale_info FROM %s WHERE ts_key = '%s'",
                             tbl,series)
    res <- dbGetQuery(con,sql_statement)
    res_list <- split(res,factor(res$locale_info))
    res_list <- lapply(res_list,function(x){
      nms <- x$key
      li <- as.list(x$value)
      names(li) <- nms
      li
    })
    
    
    addMetaInformation(series,res_list,overwrite = overwrite)
    
    
    
    
  }
}

