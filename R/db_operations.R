db_create_release <- function(con,
                              schema,
                              release,
                              release_desc) {
  dbGetQuery(con,
             query_create_release(schema),
             list(
               release,
               release_desc
             ))$id
}

db_populate_ts_updates <- function(con,
                                   schema,
                                   release_id,
                                   valid_from,
                                   release_date,
                                   records,
                                   access) {

  use_case <- get_use_case(valid_from, release_date)
  
  ts_validity <- ifelse(use_case %in% c(1, 2),
                        sprintf("[%s,)", valid_from),
                        "(,)")
  
  release_validity <- ifelse(use_case %in% c(2, 4),
                             sprintf("[%s,)", release_date),
                             "(,)")
  
  dt <- data.table(
    ts_key = names(records),
    ts_data = unlist(records),
    release_id = release_id,
    ts_validity = ts_validity,
    release_validity = release_validity,
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
                 release_id = "uuid",
                 ts_validity = "daterange",
                 release_validity = "tstzrange",
                 access = "text"
               )
  )
}

## TODO: remove these two.
## If there is only a single statement and no branching there is no point in these wrappers.
db_close_ranges_main <- function(con,
                                schema,
                                tbl) {
  dbExecute(con,
            query_close_ranges_main(schema, tbl))
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
}

db_populate_ts_read <- function(con,
                                ts_keys,
                                regex,
                                schema,
                                valid_on,
                                respect_release_date) {
  dbExecute(con, "DROP TABLE IF EXISTS ts_read")
  
  if(regex) {
    dbExecute(con,
              query_populate_ts_read_regex(schema, ts_keys[1], valid_on, respect_release_date))
  } else {
    
    # Including ts_validity here saves us an ALTER TABLE
    dt <- data.table(
      ts_key = ts_keys,
      ts_validity = NA
    )
  
    dbWriteTable(con,
                 "ts_read",
                 dt,
                 temporary = TRUE, # Praise be for this parameter!
                 overwrite = TRUE,
                 field.types = c(
                   ts_key = "text",
                   ts_validity = "daterange"
                 )
    )
    
    dbExecute(con,
              query_update_ts_read(schema, valid_on, respect_release_date))
  }
}