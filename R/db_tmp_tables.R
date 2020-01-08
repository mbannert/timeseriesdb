#' Populate temporary ts_updates table with records
#' first properly formatting validity ranges
#'
#' @param con 
#' @param records 
#' @param valid_from 
#' @param release_date 
#' @param access 
#' @param schema
#' @importFrom RPostgres dbWriteTable
db_tmp_store <- function(con,
                         records,
                         valid_from,
                         release_date,
                         access,
                         schema = "timeseriesd") {
  
  # TODO: Would be nice to use current_date and current_timestamp from DB here
  # set validities to NA in dt if param is null, then do an update (requires 2 extra queries...)
  ts_validity <- format(valid_from, "%Y-%m-%d")
  release_validity <- format(release_date, "%Y-%m-%d %T %z")
  
  # TODO: add mechanism for setting column names (for e.g. metadata)
  dt <- data.table(
    ts_key = names(records),
    ts_data = unlist(records),
    validity = ts_validity,
    release_date = release_validity,
    access = access
  )
  
  dbWriteTable(con,
               "ts_updates",
               dt,
               temporary = TRUE,
               overwrite = TRUE,
               field.types = c(
                 ts_key = "text",
                 ts_data = "json",
                 validity = "date",
                 release_date = "timestamptz",
                 access = "text"
               )
  )
}


#' Create and populate a temporary table ts_read with desired (ts_key, ts_validity) pairs for
#' joining against timeseries_main and reading.
#'
#' if regex == TRUE the first entry of ts_keys will be used as the pattern
#'
#' @param con 
#' @param ts_keys 
#' @param regex 
#' @param schema 
#' @param table 
#' @param valid_on 
#' @param respect_release_date 
#' @importFrom RPostgres dbExecute dbWriteTable
db_populate_ts_read <- function(con,
                                ts_keys,
                                regex,
                                schema,
                                table,
                                valid_on,
                                respect_release_date) {
  if(regex) {
    if(length(ts_keys) > 1) {
      warning("regex = TRUE but length of ts_keys > 1, using only first element as pattern!")
    }
  }
  
  if(!is.null(valid_on)) {
    valid_on <- as.Date(valid_on)
  }
  
  # TODO: How to suppress notices if they do not exist?
  dbExecute(con, "DROP TABLE IF EXISTS ts_read")
  dbExecute(con, "DROP TABLE IF EXISTS ts_read_keys")
  
  if(regex) {
    dbExecute(con,
              query_populate_ts_read_keys_regex(con,
                                                schema,
                                                ts_keys[1]))
  } else {
    dt <- data.table(
      ts_key = ts_keys
    )
    
    dbWriteTable(con,
                 "ts_read_keys",
                 dt,
                 temporary = TRUE, # Praise be for this parameter!
                 overwrite = TRUE,
                 field.types = c(
                   ts_key = "text"
                 )
    )
  }
  
  dbExecute(con,
            query_populate_ts_read(
              con,
              schema,
              table,
              valid_on,
              respect_release_date
            ))
}
