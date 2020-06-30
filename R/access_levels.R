#' Change the Access Level for Time Series
#'
#' @param con RPostgres connection
#' @param ts_keys Time Series Keys for which to change access
#' @param new_access_level Access level to set to
#' @param validity If provided only change the access level for vintages with the given validity.
#'                 By default the access level for all vintages is updated.
#' @param schema Time Series schema name
#'
#' @export
#' @importFrom jsonlite fromJSON
db_change_access_level <- function(con,
                                   ts_keys,
                                   new_access_level,
                                   validity = NA,
                                   schema = "timeseries") {
  out <- db_with_temp_table(con,
                            "tmp_ts_access_keys",
                            data.frame(
                              ts_key = ts_keys
                            ),
                            field.types = c(
                              ts_key = "text"
                            ),
                            {
                              db_call_function(con,
                                               "change_access_level",
                                               list(
                                                 new_access_level,
                                                 validity
                                               ),
                                               schema = schema)
                            },
                            schema = schema)
  parsed <- fromJSON(out)

  if(parsed$status == "error") {
    stop(parsed$message)
  }

  parsed
}
