store_records <- function(con,
                          records,
                          release,
                          access,
                          tbl,
                          release_desc = "",
                          valid_from = NULL,
                          release_date = NULL,
                          overwrite = TRUE,
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
    release_id <- dbGetQuery(con,
                             query_create_release(con,
                                                  schema),
                             list(
                               release,
                               release_desc
                             ))$id

    for(i in seq(1, n_records, chunk_size)) {
      db_populate_ts_updates(con,
                             schema,
                             release_id,
                             valid_from,
                             release_date,
                             records[i:min(n_records, i+chunk_size)],
                             access)
      
      dbExecute(con,
                query_close_ranges_main(con,
                                        schema,
                                        tbl))
      
      db_remove_previous_versions(con, schema, tbl, valid_from, release_date)
      
      # This will throw an error in case already versioned ts are stored w/o valid_from
      # 1) either build in checks before even making this call
      # 2) catch it and return some more understandable error
      dbExecute(con,
                query_insert_main(con,
                                  schema,
                                  tbl))
      
      dbExecute(con, 
                query_delete_empty_validity_main(con,
                                                 schema,
                                                 tbl))
    }
  })
}