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
#' @param tbl character name of the table that contains the
#' localized meta information
#' @param meta_env character name of the environment that holds the localized meta information. 
#' @export 
readMetaInformation <- function(series,
                                con = options()$TIMESERIESDB_CON,
                                overwrite,type = "localized",
                                tbl = 'meta_data_localized',
                                meta_env = 'meta_localized'){
  
  if(is.null(con)) stop('Default TIMESERIESDB_CON not set in options() or no proper connection given to the con argument.')
  
  
  if(type == 'localized'){
    sql_statement <- sprintf("SELECT (each(meta_data)).key,
                             (each(meta_data)).value,
                             locale_info FROM %s WHERE ts_key = '%s'",
                             tbl,series)
    res <- DBI::dbGetQuery(con,sql_statement)
    res_list <- split(res,factor(res$locale_info))
    res_list <- lapply(res_list,function(x){
      nms <- x$key
      li <- as.list(x$value)
      names(li) <- nms
      li
    })
    
    # returns an environment of class meta_env
    addMetaInformation(series,res_list,overwrite = overwrite)    
  }
}

