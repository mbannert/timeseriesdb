#' Populate temporary ts_updates table with records
#' first properly formatting validity ranges
#'
#' @param con 
#' @param schema 
#' @param release_id 
#' @param valid_from 
#' @param release_date 
#' @param records 
#' @param access 
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
  
  # TODO: add mechanism for setting column names (for e.g. metadata)
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

#' Helper for removing rows no longer needed in use case 3 and 4
#' In 3: Simply delete everything with the ts_key to be inserted
#' In 4: Delete rows whose release_validity lies in the past
#' 
#' @param con 
#' @param schema 
#' @param tbl 
#' @param valid_from 
#' @param release_date 
db_remove_previous_versions <- function(con,
                                        schema,
                                        tbl,
                                        valid_from,
                                        release_date) {
  use_case <- get_use_case(valid_from, release_date)
  if(use_case == 3) {
    dbExecute(con,
              query_delete_old_versions(con, schema, tbl))
  } else if (use_case == 4) {
    # clean up stale versions, they are not needed anymore
    dbExecute(con,
              query_delete_stale_versions(con, schema, tbl))
  }
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
#' @param valid_on 
#' @param respect_release_date 
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