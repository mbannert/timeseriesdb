#' Read Meta Information From the Database Given a Time Series Key
#' 
#' This function is not really operational yet. WARNING
#' 
#' @param connect character string name of a database connection object. 
#' @param series character string time series key
#' @param meta_fixed logical should non-translatable data be queried? defaults to  to FALSE. Gets information such as legacy key or frequency. 
#' @param meta_localized character either a two character language abbreviation or "all" which fetches all available meta information for a particular series.
#' @author Matthias Bannert
readMetaInformation <- function(series, meta_fixed = TRUE,
                                meta_localized = T,
                                connect = "con",
                                meta_table_fixed = "meta_data_unlocalized",
                                meta_table_localized = "meta_data_localized"){
  
  connect <- get(connect)
  
  # Fetch Fixed Meta Information --------------------------------------------
  # Add fixed meta information to the the environment fixed meta in order 
  # not to loose meta information like you might loose attributes of R objects
  # when processing R objects. 
  if(meta_fixed){
    # extract data.frame from hstore
    sql_statement_fixed = sprintf("SELECT (each(meta_data)).key, (each(meta_data)).value FROM s% WHERE ts_key = '%s'",meta_table_fixed,series)
    meta_df <- dbGetQuery(connect,sql_statement_fixed)
    
    # create an own S3 classes for fixed meta information and its env
    # in order to have custom print and show methods
    if(!exists("meta_fixed",envir = .GlobalEnv)){
      meta_fixed <- new.env()
      class(meta_fixed) <- c("environment","meta_env")
      assign("meta_fixed",meta_fixed,envir = .GlobalEnv)
    } 
    
    meta_df <- list(meta_df)
    class(meta_df) <- c("list","meta_fixed")
    assign(series,meta_df,envir = meta_fixed)
  }
  
  
  
}



