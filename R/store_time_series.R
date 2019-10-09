#' @export
store_time_series <- function(con,
                              x,
                              release,
                              access,
                              subset,
                              release_desc,
                              valid_from,
                              release_date,
                              overwrite,
                              schema){
  UseMethod("store_time_series", object = x)
}

store_time_series.tslist <- function(con,
                                     tsl,
                                     release,
                                     access,
                                     subset = names(tsl),
                                     release_desc = "",
                                     valid_from = NULL,
                                     release_date = NULL,
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

  store_records(con,
                to_ts_json(tsl),
                release,
                access,
                release_desc,
                valid_from,
                release_date,
                overwrite,
                schema)
}

store_time_series.data.table <- function(con,
                                    dt,
                                    release,
                                    access,
                                    subset = dt[, id],
                                    release_desc = "",
                                    valid_from = NULL,
                                    release_date = NULL,
                                    overwrite = TRUE,
                                    schema = "timeseries") {
  if(!all(c("id", "time", "value") %in% names(dt))) {
    stop("This does not look like a ts data.table. Expected column names id, time and value.")
  }
  
  dt <- dt[id %in% subset]
  
  if(dt[, .N] == 0) {
    message("No time series in subset - returned empty list.")
    return(list())
  } 
  
  store_records(con,
                to_ts_json(dt),
                release,
                access,
                release_desc,
                valid_from,
                release_date,
                overwrite,
                schema)
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
