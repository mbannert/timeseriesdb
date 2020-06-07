#' Store a set of time series to database
#' 
#' @param con RPostgres database connection
#'
#' @param x Object containing time series to store
#' @param release Title for the release
#' @param access Access level for all ts to be stored. Defaults to 'main'.
#' @param subset Ts keys of the subset of x to store
#' @param release_desc Description for the release
#' @param valid_from Start of vintage validity for all ts in x
#' @param release_date Release date of all ts in x
#' @param overwrite Not currently used
#' @param schema 
#' @param chunk_size 
#'
#' @export
store_time_series <- function(con,
                              x,
                              access = "main",
                              valid_from,
                              release_date,
                              schema){
  UseMethod("store_time_series", object = x)
}

#'@export
store_time_series.list <- function(con,
                                   tsl,
                                   access,
                                   valid_from = NA,
                                   release_date = NA,
                                   schema = "timeseries"){
  
  is_tsl <- sapply(tsl, function(x) inherits(x,c("ts","zoo","xts")))
  tsl <- tsl[is_tsl]
  class(tsl) <- c("tslist","tsl")
  store_time_series(con, tsl,
                    access = access,
                    valid_from = valid_from,
                    release_date = release_date,
                    schema = schema)
}

store_time_series.tslist <- function(con,
                                     tsl,
                                     access,
                                     valid_from = NA,
                                     release_date = NA,
                                     schema = "timeseries"){
  if(length(tsl) == 0) {
    warning("Ts list is empty. This is a no-op.")
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
                access,
                "timeseries_main",
                valid_from,
                release_date,
                schema)
}

store_time_series.data.table <- function(con,
                                         dt,
                                         access,
                                         valid_from = NA,
                                         release_date = NA,
                                         schema = "timeseries") {
  if(!all(c("id", "time", "value") %in% names(dt))) {
    stop("This does not look like a ts data.table. Expected column names id, time and value.")
  }
  
  if(dt[, .N] == 0) {
    warning("No time series in data.table. This is a no-op.")
    return(list())
  } 
  
  store_records(con,
                to_ts_json(dt),
                access,
                "timeseries_main",
                valid_from,
                release_date,
                schema)
}

# TODO: re-add removed arguments - the old name needs to keep the signature
#' @export
storeTimeSeries <- function(con,
                            li,
                            series = names(li),
                            valid_from = Sys.Date(),
                            release_date = Sys.time(),
                            store_freq = TRUE,
                            tbl = "timeseries_main",
                            tbl_vintages = "timeseries_vintages",
                            md_unlocal = "meta_data_unlocalized",
                            overwrite = TRUE,
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
    li <- series # list object
    series <- char_series
  }
  
  warning("This is the old version of storeTimeSeries! You should strongly consider upgrading to
          store_time_series for the following reasons:\n
          1) all time series are stored with ??? access when using storeTimeSeries\n
          2) the tbl argiments are ignored, all time series are stored in the table \"timeseries_main\"\n
          3) I'm sure there are more reasons")
  
  store_time_series(con,
                    li,
                    release = sprintf("legacy_call_%s", format(Sys.time(), "%Y%m%d_%H%M%S")),
                    "???", # TODO!! Maybe we should just drop support for the old syntax altogether.
                    series,
                    valid_from = valid_from,
                    release_description = "a summary of arguments used",
                    release_date = release_date,
                    overwrite = overwrite,
                    schema = "schema")
}
