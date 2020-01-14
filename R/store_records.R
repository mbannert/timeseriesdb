#' @importFrom RPostgres dbGetQuery dbWithTransaction dbExecute
store_records <- function(con,
                          records,
                          access,
                          tbl,
                          valid_from = Sys.Date(),
                          release_date = Sys.time(),
                          schema = "timeseries",
                          chunk_size = 10000){
  n_records <- length(records)
  
  if(!is.null(valid_from)) {
    valid_from <- as.POSIXct(valid_from)
  }
  
  if(!is.null(release_date)) {
    release_date <- as.POSIXct(release_date)
  }
  
  dbWithTransaction(con, {
    for(i in seq(1, n_records, chunk_size)) {
      db_tmp_store(con,
                   records[i:min(n_records, i+chunk_size)],
                   valid_from,
                   release_date,
                   access,
                   schema)
      
      # TODO: Error handling
      dbExecute(con,
                "SELECT * FROM timeseries.insert_from_tmp()")
    }
  })
}