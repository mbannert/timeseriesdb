#' Title
#'
#' @param con
#' @param ts_keys
#' @param schema
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

#' Title
#'
#' @param con
#' @param ts_keys
#' @param schema
#'
#' @export
#'
#' @importFrom jsonlite fromJSON
db_delete_latest_vintage <- function(con,
                                     ts_keys,
                                     schema = "timeseries") {
  # TODO: ask for confirmation?

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

db_delete_old_vintages <- function(con,
                                   ts_keys,
                                   older_than,
                                   schema = "timeseries") {
  # TODO: ask for confirmation?

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
                                  } else {
                                    stop(e)
                                  }
                                }
                              )
                            },
                            schema = schema)

  fromJSON(out)
}
