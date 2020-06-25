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

  on.exit(dbRemoveTable(con, name))

  force(code)
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
db_with_tmp_read <- function(con,
                             ts_keys,
                             regex,
                             code,
                             schema = "timeseries") {
  if(regex) {
    if(length(ts_keys) > 1) {
      warning("regex = TRUE but length of ts_keys > 1, using only first element as pattern!")
    }
  }

  on.exit(dbRemoveTable(con, "tmp_ts_read_keys"))

  if(regex) {
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
              sprintf("SELECT 1 FROM %sfill_read_tmp_regex(%s)",
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
