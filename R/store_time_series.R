#' Store a Time Series to the Database
#'
#' Stores one or more time series to the database.
#'
#' @param x Object containing time series to store. Single ts or xts objects are allowed as well as objects of type list, tslist, and data.table.
#' @param release_date character date from which on this version of the time series should be made available when release date is respected. Applies to all time series in x.
#' @param access character Access level for all ts to be stored. If set to NA (default) the database set it to 'main' access.
#' @inheritParams param_defs
#' @family time series functions
#'
#' @export
#'
#' @examples
#'
#' \dontrun{
#' # storing zrh_airport data that is a list with two xts objects.
#' store_time_series(con = connection, zrh_airport, schema = "schema")
#'
#' # to store different versions of the data, use parameter valid_from
#' # different versions are stored with the same key
#' ch.kof.barometer <- kof_ts["baro_2019m11"]
#' names(ch.kof.barometer) <- c("ch.kof.barometer")
#' store_time_series(
#'   con = connection,
#'   ch.kof.barometer,
#'   valid_from = "2019-12-01",
#'   schema = "schema"
#' )
#'
#' ch.kof.barometer <- kof_ts["baro_2019m12"]
#' names(ch.kof.barometer) <- c("ch.kof.barometer")
#' store_time_series(
#'   con = connection,
#'   ch.kof.barometer,
#'   valid_from = "2020-01-01",
#'   schema = "schema"
#' )
#' }
store_time_series <- function(con,
                              x,
                              access = NA,
                              valid_from = NA,
                              release_date = NA,
                              schema = "timeseries") {
  UseMethod("store_time_series", object = x)
}

#' @export
store_time_series.list <- function(con,
                                   tsl,
                                   access = NA,
                                   valid_from = NA,
                                   release_date = NA,
                                   schema = "timeseries") {
  is_tsl <- sapply(tsl, function(x) inherits(x, c("ts", "zoo", "xts")))
  tsl <- tsl[is_tsl]
  class(tsl) <- c("tslist", "tsl")
  store_time_series(con, tsl,
    access = access,
    valid_from = valid_from,
    release_date = release_date,
    schema = schema
  )
}

#' @export
store_time_series.tslist <- function(con,
                                     tsl,
                                     access = NA,
                                     valid_from = NA,
                                     release_date = NA,
                                     schema = "timeseries") {
  if (length(tsl) == 0) {
    warning("Ts list is empty. This is a no-op.")
    return(list())
  }

  if (any(duplicated(names(tsl)))) {
    stop("Time series list contains duplicate keys.")
  }

  # SANITY CHECK ##############
  keep <- sapply(tsl, function(x) inherits(x, c("ts", "zoo", "xts")))
  dontkeep <- !keep

  if (!all(keep)) {
    message(
      "These elements are no valid time series objects: \n",
      paste0(names(tsl[dontkeep]), " \n")
    )
  }

  tsl <- tsl[keep]

  modes <- sapply(tsl, mode)
  if (any(modes != "numeric")) {
    stop("All time series must be numeric!")
  }

  store_records(
    con,
    to_ts_json(tsl),
    access,
    "timeseries_main",
    valid_from,
    release_date,
    schema
  )
}

#' @import data.table
#' @export
store_time_series.data.table <- function(con,
                                         dt,
                                         access = NA,
                                         valid_from = NA,
                                         release_date = NA,
                                         schema = "timeseries") {
  if (!all(c("id", "time", "value") %in% names(dt))) {
    stop("This does not look like a ts data.table. Expected column names id, time and value.")
  }

  if (dt[, .N] == 0) {
    warning("No time series in data.table. This is a no-op.")
    return(list())
  }

  if (dt[, mode(value)] != "numeric") {
    stop("\"value\" must be numeric.")
  }

  if (anyDuplicated(dt, by = c("id", "time")) > 0) {
    stop("data.table contains duplicated (id, time) pairs. Are there duplicate series?")
  }

  store_records(
    con,
    to_ts_json(dt),
    access,
    "timeseries_main",
    valid_from,
    release_date,
    schema
  )
}

#' @export
store_time_series.ts <- function(con,
                              x,
                              access = NA,
                              valid_from = NA,
                              release_date = NA,
                              schema = "timeseries"){
  store_time_series(con,
                    structure(
                      list(x),
                      names = deparse(substitute(x))
                    ),
                    access,
                    valid_from,
                    release_date,
                    schema)
}

#' @export
store_time_series.xts <- function(con,
                                 x,
                                 access = NA,
                                 valid_from = NA,
                                 release_date = NA,
                                 schema = "timeseries"){
  store_time_series(con,
                    structure(
                      list(x),
                      names = deparse(substitute(x))
                    ),
                    access,
                    valid_from,
                    release_date,
                    schema)
}
