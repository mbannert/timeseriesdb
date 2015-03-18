#' Search the Database by Keys
#' 
#' Quick handle operator to search the database by keys. All time series whose key fit 
#' the regular expression which was handed to the operator are returned in a list. 
#' 
#' @param conObj PostgreSQL Connection object.
#' @param regexp character regular expression pattern. 
#' @rdname quickHandleOps
#' @export
"%k%" <- function(conObj,regexp,schema = Sys.getenv("TIMESERIESDB_SCHEMA")){
  
  # get time series keys that suit the regexp
  sql_query <- sprintf("SELECT ts_key FROM %s.timeseries_main WHERE ts_key ~ '%s'",
                       schema,regexp)
  keys <- dbGetQuery(conObj,sql_query)$ts_key
  
  ts_list <- readTimeSeries(keys,conObj)
  return(ts_list)
}


#' Create Custom Quick Handle Operators For Unlocalized Meta Information
#' 
#' Create '%letter%' style operator to conveniently access meta data by hstore keys. 
#' This function creates a new function operator for a particular key. Name the function 
#' operator style to get the most out of it.
#' 
#' @param key character name of the key inside the hstore. 
#' @export
#' @rdname quickHandleOps
createMetaDataHandle <- function(key,schema = "timeseries"){
  
  sql_query <- sprintf("SELECT ts_key,
                        meta_data->'%s' AS %s
                        FROM %s.meta_data_unlocalized WHERE meta_data->'%s' ~ '",
                        key,key,schema,key)
  
  sql_query <- paste0(sql_query,"%s'")

  fct <- sprintf("
            function(conObj,regexp){
            sql_query <- sprintf(\"%s\",regexp)
            
            key_df <- dbGetQuery(conObj,sql_query)
            
            ts_list <- readTimeSeries(key_df$ts_key,conObj)
            names(ts_list) <- key_df[match(names(ts_list), key_df$ts_key),'%s']
            return(ts_list)
  }",sql_query,key)
  
  eval(parse(text = fct))
}  
  




