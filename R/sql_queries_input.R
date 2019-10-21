### GENERAL (release date independent)

#' Create a query to insert contents of ts_updats into schema.tbl
#' 
#' @param con 
#'
#' @param schema 
#' @param tbl 
#'
#' @importFrom RPostgres Id dbQuoteIdentifier
query_insert_main <- function(con,
                              schema,
                              tbl) {
  sprintf("
      INSERT INTO %s
      SELECT * FROM ts_updates
    ",
    dbQuoteIdentifier(con, Id(schema = schema, table = tbl))
  )
}

#' Create a query to delete all rows from schema.tbl where ts_validity is empty
#' 
#' @param con 
#'
#' @param schema 
#' @param tbl 
#'
#' @importFrom RPostgres Id dbQuoteIdentifier
query_delete_empty_validity_main <- function(con,
                                             schema,
                                             tbl) {
  sprintf("
      DELETE FROM %s
      WHERE isempty(ts_validity)
    ",
    dbQuoteIdentifier(con, Id(schema = schema, table = tbl))
  )
}

#' Create a query to delete all rows in schema."releases" where ts_validity is empty
#' 
#' @param con 
#'
#' @param schema 
#'
#' @importFrom RPostgres Id dbQuoteIdentifier
query_delete_empty_validity_releases <- function(con,
                                                 schema) {
  sprintf("
      DELETE FROM %s
      WHERE isempty(ts_validity)
    ",
    dbQuoteIdentifier(con, Id(schema = schema, table = "releases"))
  )
}

#' Create a query to insert ($1, $2) into schema."releases"(release, release_description) returning the release id
#' 
#' @param con 
#'
#' @param schema 
#'
#' @importFrom RPostgres Id dbQuoteIdentifier
query_create_release <- function(con,
                                 schema) {
  sprintf("
    INSERT INTO %s(release, release_description)
    VALUES ($1, $2)
    RETURNING id
  ",
  dbQuoteIdentifier(con, Id(schema = schema, table = "releases"))
  )
}

#' Create a query to appropriately close validity ranges in schema.table based on ts_updates
#' 
#' 
#' @param con 
#'
#' @param schema 
#' @param tbl 
#'
#' @importFrom RPostgres Id dbQuoteIdentifier
query_close_ranges_main <- function(con,
                                    schema,
                                    tbl) {
  schema_table <- dbQuoteIdentifier(con, Id(schema = schema, table = tbl))
  table <- dbQuoteIdentifier(con, Id(table = tbl))
  
  sprintf("
      UPDATE %s
      SET ts_validity = daterange(lower(%s.ts_validity), lower(ts_updates.ts_validity)),
      release_validity = tstzrange(lower(%s.release_validity), lower(ts_updates.release_validity))
      FROM ts_updates
      WHERE ts_updates.ts_key = %s.ts_key
      AND upper_inf(%s.ts_validity)
      AND upper_inf(%s.release_validity);
    ",
          schema_table,
          table,
          table,
          table,
          table,
          table
  )
}

### Use case specific queries
