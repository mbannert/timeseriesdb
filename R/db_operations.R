db_populate_ts_updates <- function(con,
                                   schema,
                                   release_id,
                                   valid_from,
                                   release_date,
                                   records,
                                   access) {

  use_case <- get_use_case(valid_from, release_date)
  
  ts_validity <- ifelse(use_case %in% c(1, 2),
                        sprintf("[%s,)", format(valid_from, "%Y-%m-%d")),
                        "(,)")
  
  release_validity <- ifelse(use_case %in% c(2, 4),
                             sprintf("[%s,)", format(release_date, "%Y-%m-%d %T %z")),
                             "(,)")
  
  dt <- data.table(
    ts_key = names(records),
    ts_data = unlist(records),
    release_id = release_id,
    ts_validity = ts_validity,
    release_validity = release_validity,
    access = access,
    usage_type = use_case
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
                 access = "text",
                 usage_type = "integer"
               )
  )
}

db_remove_previous_versions <- function(con,
                                        schema,
                                        tbl,
                                        valid_from,
                                        release_date) {
  use_case <- get_use_case(valid_from, release_date)
  if(use_case == 3) {
    dbExecute(con,
              sprintf("DELETE FROM %s.%s
                      WHERE usage_type = 3
                      AND ts_key IN (SELECT ts_key FROM ts_updates)",
                      schema,
                      tbl))
  } else if (use_case == 4) {
    # clean up stale versions, they are not needed anymore
    dbExecute(con,
              sprintf("DELETE FROM %s.%s
                      WHERE usage_type = 4
                      AND (upper(release_validity) <= now() OR isempty(release_validity))",
                      schema,
                      tbl))
  }
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
              query_populate_ts_read_regex(con,
                                           schema,
                                           ts_keys[1],
                                           valid_on,
                                           respect_release_date))
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
              query_update_ts_read(con,
                                   schema,
                                   valid_on,
                                   respect_release_date))
  }
}