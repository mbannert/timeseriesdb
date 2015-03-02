"%k%" <- function(conObj,regexp){
  
  # get time series keys that suit the regexp
  sql_query <- sprintf("SELECT ts_key FROM timeseries_main WHERE ts_key ~ '%s'",
                       regexp)
  keys <- dbGetQuery(conObj,sql_query)$ts_key
  
  ts_list <- readTimeSeries(keys,conObj)
  return(ts_list)
}


"%l%" <- function(conObj,regexp){
  sql_query <- sprintf("SELECT ts_key,
                       meta_data->'legacy_key' AS legacy_key
                       FROM meta_data_unlocalized WHERE meta_data->'legacy_key' ~ '%s'",
                       regexp)
  
  key_df <- dbGetQuery(conObj,sql_query)
  nms <- key_df$legacy_key
  
  
  ts_list <- readTimeSeries(key_df$ts_key,conObj)
  names(ts_list) <- nms
  return(ts_list)
}


"%a%" <- function(conObj,regexp){
  sql_query <- sprintf("SELECT ts_key,
                       meta_data->'alias' AS alias
                       FROM meta_data_unlocalized WHERE meta_data->'alias' ~ '%s'",
                       regexp)
  
  key_df <- dbGetQuery(conObj,sql_query)
  nms <- key_df$legacy_key
  
  
  ts_list <- readTimeSeries(key_df$ts_key,conObj)
  names(ts_list) <- nms
  return(ts_list)
}


"%h%" <- function(conObj,regexp){
  Sys.getenv("LOOKUP")
  sql_query <- sprintf("SELECT ts_key,
                       meta_data->'alias' AS alias
                       FROM meta_data_unlocalized WHERE meta_data->'alias' ~ '%s'",
                       regexp)
  
  key_df <- dbGetQuery(conObj,sql_query)
  nms <- key_df$legacy_key
  
  
  ts_list <- readTimeSeries(key_df$ts_key,conObj)
  names(ts_list) <- nms
  return(ts_list)
}



