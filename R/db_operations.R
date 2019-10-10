db_close_releases <- function(con,
                            schema,
                            release,
                            valid_from,
                            release_date) {
  if (!is.null(valid_from) && is.null(release_date)) {
    dbExecute(
      con,
      query_close_releases(schema, valid_from, release_date),
      list(valid_from,
           valid_from,
           release)
    )
  } else {
    stop("You seem to have reached a point of unimplemented code. sorries!")
  }
}

db_insert_releases <- function(con,
                             schema,
                             release,
                             release_desc,
                             valid_from,
                             release_date) {
  if (!is.null(valid_from) && is.null(release_date)) {
    dbExecute(
      con,
      query_insert_releases(schema, valid_from, release_date),
      list(release,
           sprintf("[%s,)", valid_from),
           release_desc)
    )
  } else {
    stop("Unimplemented")
  }
}

db_populate_ts_updates <- function(con,
                                schema,
                                release,
                                valid_from,
                                records,
                                access) {

  dt <- data.table(
    ts_key = names(records),
    ts_validity = sprintf("[%s,)", valid_from),
    ts_data = unlist(records),
    release = release,
    access = access
  )
  
  dbWriteTable(con,
               "ts_updates",
               dt,
               temporary = TRUE, # Praise be for this parameter!
               overwrite = TRUE,
               field.types = c(
                 ts_key = "text",
                 ts_validity = "daterange",
                 ts_data = "json",
                 release = "text",
                 access = "text"
               )
  )
}

db_close_validity_main <- function(con,
                                schema,
                                tbl,
                                valid_from,
                                release_date) {
  dbExecute(con,
            query_close_main(schema, tbl))
}

db_insert_new_records <- function(con,
                               schema,
                               tbl,
                               valid_from,
                               release_date) {
  dbExecute(con,
            query_insert_main(schema, tbl))
}

db_cleanup_empty_versions <- function(con,
                                   schema,
                                   tbl,
                                   valid_from,
                                   release_date) {
  dbExecute(con, 
            query_delete_empty_validity_main(schema, tbl))
  
  dbExecute(con,
            query_delete_empty_validity_releases(schema))
}