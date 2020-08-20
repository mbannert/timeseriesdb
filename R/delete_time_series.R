#' Remove Time Series from the Database
#'
#' This function completely removes a time series from the database, including
#' all vintages and metadata.
#'
#' Due to the potentially severe consequences of such a deletion only timeseries
#' admins may perform this action and should do so very diligently.
#'
#' @inheritParams param_defs
#' @family time series functions
#'
#' @export
#'
#' @importFrom jsonlite fromJSON
#'
#' @examples
#'
#' \dontrun{
#' # Store zrh_airport data
#' store_time_series(con = connection, zrh_airport, schema = "schema")
#'
#' # Deleting one key
#' db_ts_delete(
#'   con = connection,
#'   ts_keys = "ch.zrh_airport.departure.total",
#'   schema = "schema"
#' )
#'
#' # Deleting multiple keys
#' db_ts_delete(
#'   con = connection,
#'   ts_keys = c(
#'     "ch.zrh_airport.departure.total",
#'     "ch.zrh_airport.arrival.total"
#'   ),
#'   schema = "schema"
#' )
#' }
db_ts_delete <- function(con,
                         ts_keys,
                         schema = "timeseries") {
  message("This operation will PERMANENTLY delete the specified time series, including their histories and metadata. If this is what you intend to do, please type yes below.")

  ans <- readline("answer: ")

  if (ans != "yes") {
    stop(sprintf("You typed %s, aborting.", ans))
  }

  out <- db_with_temp_table(con,
    "tmp_ts_delete_keys",
    data.frame(
      ts_key = ts_keys
    ),
    field.types = c(ts_key = "text"),
    {
      tryCatch(
        db_call_function(con, "delete_ts", schema = schema),
        error = function(e) {
          if (grepl("permission denied for function delete_ts", e)) {
            stop("Only timeseries admins may delete time series.")
          } else {
            stop(e)
          }
        }
      )
    },
    schema = schema
  )
  fromJSON(out)
}

#' Delete the Latest Vintage of a Time Series
#'
#' Vintages of time series should not be deleted as they are versions and
#' represent a former status of a time series that may not be stored elsewhere,
#' even not with their original provider. To benchmark forecasts it is essential
#' to keep the versions to evaluate real time performance of forecasts. However,
#' when operating at current edge of a time series, i.e., its last update, mistakes
#' may happen. Hence timeseriesdb allows to update / delete the last iteration.
#' Do not loop recursively through iterations to delete an entire time series.
#' There are admin level functions for that.
#'
#' @inheritParams param_defs
#' @family time series functions
#'
#' @export
#'
#' @importFrom jsonlite fromJSON
#'
#' @examples
#'
#' \dontrun{
#'
#' # Store different versions of the time series data
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
#'
#' db_ts_delete_latest_version(
#'   con = connection,
#'   ts_keys = "ch.kof.barometer",
#'   schema = "schema"
#' )
#' }
db_ts_delete_latest_version <- function(con,
                                        ts_keys,
                                        schema = "timeseries") {
  out <- db_with_temp_table(con,
    "tmp_ts_delete_keys",
    data.frame(
      ts_key = ts_keys
    ),
    field.types = c(ts_key = "text"),
    {
      tryCatch(
        db_call_function(con, "delete_ts_edge", schema = schema),
        error = function(e) {
          if (grepl("permission denied for function delete_ts_edge", e)) {
            stop("Only timeseries admins may delete vintages.")
          } else {
            stop(e)
          }
        }
      )
    },
    schema = schema
  )

  fromJSON(out)
}

#' Remove Vintages from the Beginning
#'
#' Removes any vintages of the given time series that are older than a specified date.
#'
#' In some cases only the last few versions of time series are of interest. This
#' function can be used to trim off old vintages that are no longer relevant.
#'
#' @param older_than Date cut off point
#'
#' @inheritParams param_defs
#' @family time series functions
#'
#' @export
#' @importFrom jsonlite fromJSON
#'
#' @examples
#'
#' \dontrun{
#'
#' # Store different versions of the time series data
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
#'
#' db_ts_trim_history(
#'   con = connection,
#'   ts_keys = "ch.kof.barometer",
#'   older_than = "2019-12-31",
#'   schema = "schema"
#' )
#' }
db_ts_trim_history <- function(con,
                               ts_keys,
                               older_than,
                               schema = "timeseries") {
  out <- db_with_temp_table(con,
    "tmp_ts_delete_keys",
    data.frame(
      ts_key = ts_keys
    ),
    field.types = c(ts_key = "text"),
    {
      tryCatch(
        db_call_function(
          con,
          "delete_ts_old_vintages",
          list(
            older_than
          ),
          schema = schema
        ),
        error = function(e) {
          if (grepl("permission denied for function delete_ts_old_vintages", e)) {
            stop("Only timeseries admins may delete vintages.")
          } else if (grepl("input syntax for type date", e)) {
            stop("Invalid date supplied. older_than must be a Date or a string of the form YYYY-MM-DD.")
          } else {
            stop(e)
          }
        }
      )
    },
    schema = schema
  )

  fromJSON(out)
}
