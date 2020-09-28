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
#' db_ts_store(con = connection, zrh_airport, schema = "schema")
#'
#' # to store different versions of the data, use parameter valid_from
#' # different versions are stored with the same key
#' ch.kof.barometer <- kof_ts["baro_2019m11"]
#' names(ch.kof.barometer) <- c("ch.kof.barometer")
#' db_ts_store(
#'   con = connection,
#'   ch.kof.barometer,
#'   valid_from = "2019-12-01",
#'   schema = "schema"
#' )
#'
#' ch.kof.barometer <- kof_ts["baro_2019m12"]
#' names(ch.kof.barometer) <- c("ch.kof.barometer")
#' db_ts_store(
#'   con = connection,
#'   ch.kof.barometer,
#'   valid_from = "2020-01-01",
#'   schema = "schema"
#' )
#' }
db_ts_store <- function(con,
                        x,
                        access = NULL,
                        valid_from = NULL,
                        release_date = NULL,
                        schema = "timeseries"){
  UseMethod("db_ts_store", object = x)
}

#' @export
db_ts_store.list <- function(con,
                             x,
                             access = NULL,
                             valid_from = NULL,
                             release_date = NULL,
                             schema = "timeseries"){

  is_tsl <- sapply(x, function(y) inherits(y, c("ts","zoo","xts")))

  x <- x[is_tsl]
  class(x) <- c("tslist", "tsl")
  db_ts_store(con, x,
              access = access,
              valid_from = valid_from,
              release_date = release_date,
              schema = schema
  )
}

#' @export
db_ts_store.tslist <- function(con,
                               x,
                               access = NULL,
                               valid_from = NULL,
                               release_date = NULL,
                               schema = "timeseries"){
  if(length(x) == 0) {
    warning("Ts list is empty. This is a no-op.")
    return(list())
  }

  if (any(duplicated(names(x)))) {
    stop("Time series list contains duplicate keys.")
  }

  # SANITY CHECK ##############
  keep <- sapply(x, function(y) inherits(y, c("ts", "zoo", "xts")))
  dontkeep <- !keep

  if (!all(keep)) {
    message(
      "These elements are no valid time series objects: \n",
      paste0(names(x[dontkeep]), " \n")
    )
  }

  x <- x[keep]

  modes <- sapply(x, mode)
  if (any(modes != "numeric")) {
    stop("All time series must be numeric!")
  }

  store_records(
    con,
    to_ts_json(x),
    access,
    "timeseries_main",
    valid_from,
    release_date,
    schema
  )
}

#' @import data.table
#' @export
db_ts_store.data.table <- function(con,
                                   x,
                                   access = NULL,
                                   valid_from = NULL,
                                   release_date = NULL,
                                   schema = "timeseries") {
  if (!all(c("id", "time", "value") %in% names(x))) {
    stop("This does not look like a ts data.table. Expected column names id, time and value.")
  }

  if (x[, .N] == 0) {
    warning("No time series in data.table. This is a no-op.")
    return(list())
  }

  if (x[, mode(value)] != "numeric") {
    stop("\"value\" must be numeric.")
  }

  if (anyDuplicated(x, by = c("id", "time")) > 0) {
    stop("data.table contains duplicated (id, time) pairs. Are there duplicate series?")
  }

  store_records(
    con,
    to_ts_json(x),
    access,
    "timeseries_main",
    valid_from,
    release_date,
    schema
  )
}

#' @export
db_ts_store.ts <- function(con,
                           x,
                           access = NA,
                           valid_from = NA,
                           release_date = NA,
                           schema = "timeseries"){
  db_ts_store(con,
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
db_ts_store.xts <- function(con,
                            x,
                            access = NA,
                            valid_from = NA,
                            release_date = NA,
                            schema = "timeseries"){
  db_ts_store(con,
              structure(
                list(x),
                names = deparse(substitute(x))
              ),
              access,
              valid_from,
              release_date,
              schema)
}
