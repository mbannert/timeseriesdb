
# Not exported, internal helper.
#' @importFrom RPostgres dbGetQuery dbWithTransaction dbExecute
#' @importFrom jsonlite fromJSON
store_records <- function(con,
                          records,
                          access,
                          tbl,
                          valid_from = Sys.Date(),
                          release_date = Sys.time(),
                          pre_release_access = NULL,
                          schema = "timeseries"){
  if(!is.null(valid_from)) {
    valid_from <- format(as.POSIXct(valid_from), "%Y-%m-%d")
  }

  if(!is.null(release_date)) {
    release_date <- format(as.POSIXct(release_date), "%Y-%m-%d %T %z")
  }

  # RPostgres/data.table play better with NA (not length 0 for example)
  if(is.null(pre_release_access)) {
    pre_release_access <- NA
  }

  dt <- data.table(
    ts_key = names(records),
    ts_data = unlist(records),
    coverage = NA
  )
  out <- tryCatch(
      db_with_temp_table(con,
                         "tmp_ts_updates",
                         dt,
                         field.types = c(
                           ts_key = "text",
                           ts_data = "json",
                           coverage = "daterange"
                         ),
                         fromJSON(db_call_function(con,
                                                   "ts_insert",
                                                   list(
                                                     valid_from,
                                                     release_date,
                                                     access,
                                                     pre_release_access
                                                   ),
                                                   schema)),
                         schema = schema),
    error = function(e) {
      if(grepl("timeseries_main_access_fkey", e)) {
        stop(sprintf("\"%s\" is not a valid access level. Use db_get_access_levels to find registered levels.", access))
      } else {
        stop(e)
      }
  })

  if(out$status == "error") {
    stop(out$message)
  }

  out
}
