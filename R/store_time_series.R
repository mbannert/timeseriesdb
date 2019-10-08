#' @export
store_time_series <- function(con,
                              x,
                              subset,
                              valid_from,
                              release_date,
                              tbl,
                              overwrite,
                              schema){
  UseMethod("store_time_series", object = x)
}

store_time_series.tslist <- function(con,
                                     tsl,
                                     subset = names(tsl),
                                     valid_from = NULL,
                                     release_date = NULL,
                                     tbl = "timeseries_main",
                                     overwrite = TRUE,
                                     schema = "timeseries"){
  tsl <- tsl[subset]
  
  if(length(tsl) == 0){
    message("No time series in subset - returned empty list.")
    return(list())
  } 
  
  # SANITY CHECK ##############
  keep <- sapply(tsl, function(x) inherits(x,c("ts","zoo","xts")))
  dontkeep <- !keep
  
  if(!all(keep)){
    message("These elements are no valid time series objects: \n",
            paste0(names(tsl[dontkeep])," \n"))  
  }
  
  tsl <- tsl[keep]
  class(tsl) <- c("tslist", "list")
  
  # Alternatively: `[.tslist` <- function(x, i){x <- unclass(x); out <- x[i]; class(out) <- c("tslist", "list"); out}
  # But where does the tslist class live i.e. which package should define this selector?
  
  store_time_series.ts_json(con, to_ts_json(tsl), subset, valid_from, release_date, tbl, overwrite, schema)
}

store_time_series.data.table <- function(con,
                                    dt,
                                    subset = dt[, id],
                                    valid_from = NULL, # Is there a need for valid_from != today?
                                    release_date = NULL,
                                    tbl = "timeseries_main",
                                    overwrite = TRUE, # Might keep that to indicate whether old vintages should be deleted when storing single record?
                                    schema = "timeseries") {
  if(!all(c("id", "time", "value") %in% names(dt))) {
    stop("This does not look like a ts data.table. Expected column names id, time and value!")
  }
  
  dt <- dt[id %in% subset]
  
  if(dt[, .N] == 0) {
    message("No time series in subset - returned empty list.")
    return(list())
  } 
  
  store_time_series.ts_json(con, to_ts_json(dt), subset, valid_from, release_date, tbl, overwrite, schema)
}


store_time_series.ts_json <- function(con,
                                      tsj,
                                      subset = names(tsj),
                                      valid_from = NULL, # Is there a need for valid_from != today?
                                      release_date = NULL,
                                      tbl = "timeseries_main",
                                      overwrite = TRUE, # Might keep that to indicate whether old vintages should be deleted when storing single record?
                                      schema = "timeseries"){
  
  # some_sql
  message("Here to store the following ts json:")
  print(tsj)
  message("*cheap party horn sound*")
}

# TODO: re-add removed arguments - the old name needs to keep the signature
#' @export
storeTimeSeries <- function(con,
                            li,
                            subset = names(li),
                            valid_from = NULL, # Is there a need for valid_from != today? For building vintages from scratch I guess?
                            release_date = NULL,
                            tbl = "timeseries_main",
                            overwrite = TRUE, # Might keep that to indicate whether old vintages should be deleted when storing single record?
                            schema = "timeseries") {
  .Deprecated("storeTimeSeries")
  # back in the days the argument order was different, 
  # so if con is a character we know we need to flip things and continue 
  # to work. Of course this is not optimal, that's why the user gets a
  # message to get used to the new, consistent behavior. 
  if(is.character(con)) {
    warning("You are not only using an old function, but also an inconsistent argument order.\n
            Use store_time_series(con, series, li, ...) in the future.")
    char_series <- con
    con <- li # connection object
    li <- subset # list object
    subset <- char_series
  }
  
  store_time_series(con, li, subset, valid_from, release_Date, tbl, overwrite, schema)
}