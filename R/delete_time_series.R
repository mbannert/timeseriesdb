#' Remove Time Series from the Database
#'
#' This function completely removes a time series from the database, including
#' all vintages and metadata.
#'
#' Due to the potentially severe consequences of such a deletion only timeseries
#' admins may perform this action and should do so very dilligently.
#'
#' @param con RPostgres connection object
#' @param ts_keys character Vector of ts keys to delete
#' @param schema character Time series schema name
#'
#' @export
#'
#' @importFrom jsonlite fromJSON
db_delete_time_series <- function(con,
                                  ts_keys,
                                  schema = "timeseries") {
  message("This operation will PERMANENTLY delete the specified time series, including their histories and metadata. If this is what you intend to do, please type yes below.")

  ans <- readline("answer: ")

  if(ans != "yes") {
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
                                  if(grepl("permission denied for function delete_ts", e)) {
                                    stop("Only timeseries admins may delete time series.")
                                  } else {
                                    stop(e)
                                  }
                                }
                              )
                            },
                            schema = schema)
  fromJSON(out)
}

#' Delete the Latest Vintage of a Time Series
#'
#' @param con RPostgres connection object
#' @param ts_keys character Vector of ts keys for which to remove the latest vintage
#' @param schema Time series schema name
#'
#' @export
#'
#' @importFrom jsonlite fromJSON
db_delete_latest_vintage <- function(con,
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
                                  if(grepl("permission denied for function delete_ts_edge", e)) {
                                    stop("Only timeseries admins may delete vintages.")
                                  } else {
                                    stop(e)
                                  }
                                }
                              )
                            },
                            schema = schema)

  fromJSON(out)
}

#' Remove Vintages from the Beginning
#'
#' Removes any vintages of the given time series that are older than a specified date.
#'
#' In some cases only the last few versions of time series are of interest. This
#' function can be used to trim off old vintages that are no longer relevant.
#'
#' @param con RPostgres connection object
#' @param ts_keys character Vector of time series keys
#' @param older_than Date cut off point
#' @param schema character Time series schema name
#'
#' @export
#' @importFrom jsonlite fromJSON
db_trim_history <- function(con,
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
                                  if(grepl("permission denied for function delete_ts_old_vintages", e)) {
                                    stop("Only timeseries admins may delete vintages.")
                                  } else if(grepl("input syntax for type date", e)) {
                                    stop("Invalid date supplied. older_than must be a Date or a string of the form YYYY-MM-DD.")
                                  } else {
                                    stop(e)
                                  }
                                }
                              )
                            },
                            schema = schema)

  fromJSON(out)
}
