#' @importFrom RPostgres dbRemoveTable dbWriteTable
db_with_temp_table <- function(con,
                               name,
                               content,
                               field.types,
                               code,
                               schema = "timeseries") {
  dbWriteTable(con,
               name,
               content,
               field.types = field.types,
               temporary = TRUE,
               overwrite = TRUE)

  db_grant_to_admin(con, name, schema)

  on.exit(tryCatch(
    dbRemoveTable(con, name),
    warning = function(w) {
      suppressWarnings(dbRemoveTable(con, name, fail_if_missing = FALSE))
      if(grepl("Closing open result set", w)) {
        NULL
      } else {
        warning(w)
      }
    })
  )

  force(code)
}

#' Helper to Create and Populate a Temporary Table for Fast Reading
#'
#' This function is not exported. It creates a tempory table containing the
#' keys that should be read to join them against the time series storage.
#' This is much faster for larger selections than simple where clauses.
#'
#'
#' @inheritParams param_defs
#' @param regex logical if set to TRUE, the ts_keys parameter is interpreted as a regular expression pattern.
#' @importFrom RPostgres dbExecute dbWriteTable
db_with_tmp_read <- function(con,
                             ts_keys,
                             regex = FALSE,
                             code,
                             schema = "timeseries") {
  if(regex) {
    if(length(ts_keys) > 1) {
      warning("regex = TRUE but length of ts_keys > 1, using only first element as pattern!")
    }
  }

  on.exit(tryCatch(
    dbRemoveTable(con, "tmp_ts_read_keys"),
    warning = function(w) {
      suppressWarnings(dbRemoveTable(con, "tmp_ts_read_keys", fail_if_missing = FALSE))
      if(grepl("Closing open result set", w)) {
        NULL
      } else {
        warning(w)
      }
    })
  )

  if(regex) {
    # TODO: Why not db_with_temp_table(db_call_function))?

    # Pre-create table to make it belong to SESSION_USER
    # and grant admin user INSERT rights
    dbWriteTable(con,
                 "tmp_ts_read_keys",
                 data.table(ts_key = NA),
                 temporary = TRUE,
                 overwrite = TRUE,
                 field.types = c(
                   ts_key = "text"
                 ))

    db_grant_to_admin(con, "tmp_ts_read_keys", schema)
    dbExecute(con,
              sprintf("SELECT 1 FROM %skeys_fill_read_regex(%s)",
                      dbQuoteIdentifier(con, Id(schema = schema)),
                      dbQuoteLiteral(con, ts_keys[1])))
  } else {
    dt <- data.table(
      ts_key = ts_keys
    )

    dbWriteTable(con,
                 "tmp_ts_read_keys",
                 dt,
                 temporary = TRUE, # Praise be for this parameter!
                 overwrite = TRUE,
                 field.types = c(
                   ts_key = "text"
                 )
    )

    db_grant_to_admin(con, "tmp_ts_read_keys", schema)
  }


  force(code)
}
