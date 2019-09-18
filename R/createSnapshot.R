#' Create a Snapshot of Selected Time Series
#' 
#' Creating snapshots of entire time series datasets
#' is useful when a set of time series is composed in advance of 
#' a forecast. The state of a dataset is archived in order to 
#' benchmark forecasting methods and make forecasts reproducible. 
#' 
#' @param con PostgreSQL connection object
#' @param series character vector containing time series to be snapshot.
#' Set to NULL if you want a snapshot of all series in a schema. 
#' @param valid_from date or character formatted date denotes the start of the 
#' validity of the new time series version. 
#' @param schema character name of the schema that contains the series to be backed up. 
#' This is also the target schema if vintage_schema is not set. 
#' @param vintage_schema character name of the schema snapshots should be stored to. 
#' Defaults to NULL, using the schema parameter. 
#' @importFrom DBI dbGetQuery
#' @export
createSnapshot <- function(con, series,
                           valid_from = Sys.Date(),
                           schema, 
                           vintage_schema = NULL){
  
  if(is.null(vintage_schema)) vintage_schema <- schema
  
  if(is.null(series)){
    sql <- sprintf("SELECT ts_key FROM %s.timeseries_main",
                   schema)
    keys <- dbGetQuery(con, sql)$ts_key
    tsl <- readTimeSeries(con, keys, schema = schema)
  } else{
    tsl <- readTimeSeries(con, series, schema = schema)
  }
  
  storeTimeSeries(con, tsl, valid_from = valid_from,
                  schema = vintage_schema)
}
