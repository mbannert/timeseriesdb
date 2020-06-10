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

  tryCatch(
    dbWithTransaction(con, {
      db_tmp_store(con,
                   records)


      fromJSON(db_call_function(con,
                                "insert_from_tmp",
                                list(
                                  valid_from,
                                  release_date,
                                  access
                                ),
                                schema))
    }),
    error = function(e) {
      if(grepl("permission denied for function insert_from_tmp", e)) {
        stop("Only writer and above may store time series.")
      } else {
        stop(e)
      }
  })
}
