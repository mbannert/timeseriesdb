#' @importFrom RPostgres dbGetQuery dbWithTransaction dbExecute
#' @importFrom jsonlite fromJSON
store_records <- function(con,
                          records,
                          access,
                          tbl,
                          valid_from = Sys.Date(),
                          release_date = Sys.time(),
                          schema = "timeseries"){
  if(!is.null(valid_from)) {
    valid_from <- format(as.POSIXct(valid_from), "%Y-%m-%d")
  }

  if(!is.null(release_date)) {
    release_date <- format(as.POSIXct(release_date), "%Y-%m-%d %T %z")
  }

  # TODO: add mechanism for setting column names (for e.g. metadata)
  # Note, it's important to create the coverage column here because of an
  # rights issue: The tmp_ts_updates table will belong to the user logged in.
  # Because in PostgreSQL tables can only be altered by the OWNER and therefore
  # the insert function which runs as SECURITY DEFINER (the rights of the user
  # who created them) can't AlTER the temp table it needs to
  # contain the coverage column from the start.
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
                                                   "insert_from_tmp",
                                                   list(
                                                     valid_from,
                                                     release_date,
                                                     access
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
