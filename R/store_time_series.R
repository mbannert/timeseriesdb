#' Store a set of time series to database
#'
#' @param con RPostgres database connection
#'
#' @param x Object containing time series to store
#' @param release Title for the release
#' @param access Access level for all ts to be stored. If set to NA (default) the database set it to 'main' access.
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
                              access = NA,
                              valid_from,
                              release_date,
                              schema){
  UseMethod("store_time_series", object = x)
}

#'@export
# TODO: Add a test for this
store_time_series.list <- function(con,
                                   tsl,
                                   access = NA,
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
                                     access = NA,
                                     valid_from = NA,
                                     release_date = NA,
                                     schema = "timeseries"){
  if(length(tsl) == 0) {
    warning("Ts list is empty. This is a no-op.")
    return(list())
  }

  if(any(duplicated(names(tsl)))) {
    stop("Time series list contains duplicate keys.")
  }

  # SANITY CHECK ##############
  keep <- sapply(tsl, function(x) inherits(x,c("ts","zoo","xts")))
  dontkeep <- !keep

  if(!all(keep)){
    message("These elements are no valid time series objects: \n",
            paste0(names(tsl[dontkeep])," \n"))
  }

  tsl <- tsl[keep]

  modes <- sapply(tsl, mode)
  if(any(modes != "numeric")) {
    stop("All time series must be numeric!")
  }

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
                                         access = NA,
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

  if(dt[, mode(value)] != "numeric") {
    stop("\"value\" must be numeric.")
  }

  if(anyDuplicated(dt, by = c("id", "time")) > 0) {
    stop("data.table contains duplicated (id, time) pairs. Are there duplicate series?")
  }

  store_records(con,
                to_ts_json(dt),
                access,
                "timeseries_main",
                valid_from,
                release_date,
                schema)
}
