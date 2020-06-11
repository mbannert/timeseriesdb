#' Populate Temporary ts_updates table with records
#' first properly formatting validity ranges
#'
#' @param con RPostgres connection object.
#' @param records
#' @param valid_from
#' @param release_date
#' @param access
#' @param schema
#' @importFrom RPostgres dbWriteTable
db_tmp_store <- function(con,
                         records) {

  # TODO: add mechanism for setting column names (for e.g. metadata)
  # Note, it's important to create the coverage column here because of an
  # rights issue: The tmp_ts_updates table will belong to the user logged in.
  # Because in PostgreSQL tables can only be altered by the OWNER and therefore
  # the insert function which runs as SECURITY DEFINER (the rights of the user
  # who created them) can't AlTER the temp table it needs to
  # contain the coverage column from the start.
  dt <- data.table(
    ts_key = names(records),
    ts_data = unlist(records),
    coverage = NA
  )

  dbWriteTable(con,
               "tmp_ts_updates",
               dt,
               temporary = TRUE,
               overwrite = TRUE,
               field.types = c(
                 ts_key = "text",
                 ts_data = "json",
                 coverage = "daterange"
               )
  )

  db_grant_to_admin(con, "tmp_ts_updates", schema)

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
db_tmp_read <- function(con,
                        ts_keys,
                        regex,
                        schema) {
  if(regex) {
    if(length(ts_keys) > 1) {
      warning("regex = TRUE but length of ts_keys > 1, using only first element as pattern!")
    }
  }

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

  }

  db_grant_to_admin(con, "tmp_ts_read_keys", schema)
}
