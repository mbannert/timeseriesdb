store_records <- function(con,
                          records,
                          release,
                          access,
                          tbl,
                          release_desc = "",
                          valid_from = NULL,
                          release_date = NULL,
                          overwrite = TRUE,
                          schema = "timeseries"){
  # also: chunking? RPostgres seems to be pretty good about it tho with param queries
  # anyway, if it is to be done then HERE is the spot. right here vvv 
  
  dbWithTransaction(con, {
    db_close_releases(con, schema, release, valid_from, release_date)
    db_insert_releases(con, schema, release, release_desc, valid_from, release_date)
    db_populate_ts_updates(con, schema, release, valid_from, records, access)
    db_close_validity_main(con, schema, tbl, valid_from, release_date)
    db_insert_new_records(con, schema, tbl, valid_from, release_date)
    db_cleanup_empty_versions(con, schema, tbl, valid_from, release_date)
  })
}