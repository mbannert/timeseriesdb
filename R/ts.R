#' Rename Time Series by Assigning a New Key
#'
#' @inheritParams param_defs
#' @param ts_key character Vector of keys to rename
#' @param ts_key_new character Vector of new names
#'
#' @importFrom jsonlite fromJSON
db_ts_rename <- function(con,
                         ts_key,
                         ts_key_new,
                         schema = "timeseries") {
  if(length(ts_key) != length(ts_key_new)) {
    stop("ts_key and ts_key_new must have the same length!")
  }

  out <- db_with_temp_table(con,
                     "tmp_ts_rename",
                     data.table(
                       ts_key = ts_key,
                       ts_key_new = ts_key_new
                     ),
                     field.types = c(
                       ts_key = "text",
                       ts_key_new = "text"
                     ),
                     {
                       db_call_function(con,
                                        "rename_ts",
                                        schema = schema)
                     },
                     schema = schema)

  out_parsed <- fromJSON(out)

  if(out_parsed$status == "warning") {
    warning(out_parsed$message)
  }

  out_parsed
}
