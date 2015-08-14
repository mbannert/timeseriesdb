storeTsVintage <- function(series,con,
                           s_name = NULL,
                           vintage_key = "regular",
                           vintage_date = format(Sys.Date(),"%Y-%m-01"),
                           tbl = "timeseries_vintage",
                           schema = "timeseries"){
  
  # sanity checks
  #if(!timeseriesdb::dbIsValid(con)) stop("DB Connection is not valid anymore.")
  # if(!inherits(ts_key,"character")) stop("Time series key needs to be a character.")
  if(!inherits(series,c("zoo","ts","xts"))) stop("Series needs to be a time series object of class ts, zoo or xts.")
  
  # extract series name from object if no other name is given
  if(is.null(s_name)) s_name <- deparse(substitute(series))
  
  ts_hstore <- createHstore(series)
  ts_freq <- frequency(series)
  tbl <- paste(schema,tbl,sep = ".")
  
  
  
  values <- paste(paste0("('",
                         paste(s_name,
                               vintage_key,
                               vintage_date,
                               ts_hstore,
                               ts_freq,
                               sep="','"),
                         "')"),
                  collapse = ",")
  
  sql_query <- sprintf("INSERT INTO %s(ts_key,vnt_type,vnt_date,vnt_data,ts_frequency) VALUES %s",
                       tbl, values)
  dbGetQuery(con,sql_query)
}
