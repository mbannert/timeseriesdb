#' @importFrom RPostgres dbGetQuery dbWithTransaction dbExecute
store_records <- function(con,
                          records,
                          access,
                          tbl,
                          valid_from = Sys.Date(),
                          release_date = Sys.time(),
                          schema = "timeseries"){
  n_records <- length(records)
  
  if(!is.null(valid_from)) {
    valid_from <- as.POSIXct(valid_from)
  }
  
  if(!is.null(release_date)) {
    release_date <- as.POSIXct(release_date)
  }
  
  dbWithTransaction(con, {
    db_tmp_store(con,
                 records,
                 valid_from,
                 release_date,
                 access,
                 schema)
    
    jsonlite::parse_json(dbGetQuery(con,
              "SELECT * FROM timeseries.insert_from_tmp()")$insert_from_tmp, simplifyVector = TRUE)
  })
}