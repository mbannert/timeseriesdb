#' Install timeseriesdb
#'
#' Install timeseriesdb in a given PostgreSQL schema. Make sure the database user
#' has sufficient rights to perform the necessary operations on the schema. In the process
#' tables, roles, triggers and functions will be created. Also extensions will be installed and
#' rights will be granted and revoked from the freshly created roles.
#' Note also, that the functions created are created as SECURITY DEFINER roles.
#'
#' @inheritParams param_defs
#' @family setup SQL functions
#'
#' @importFrom RPostgres dbConnect Postgres dbGetQuery dbIsValid
#'
#' @export
install_timeseriesdb <- function(con,
                                 schema = "timeseries",
                                 verbose = FALSE,
                                 install_tables = TRUE,
                                 install_functions = TRUE) {
  # schema_exists <- dbGetQuery(con,
  #                             "SELECT true
  #                             FROM information_schema.schemata
  #                             WHERE schema_name = $1;",
  #                             list(schema))$bool
  #
  # if(length(schema_exists) == 0) {
  #   stop(sprintf("Schema %s does not exist. Please read the Installation Guide vignette.", schema))
  # }

  prnt <- function(x) {
    if(verbose) {
      message(x)
    }
  }

  current_user <- dbGetQuery(con, "SELECT CURRENT_USER as cu")$cu

  # Switch role so the objects belong to timeseries_admin as they should
  dbExecute(con, sprintf("SET ROLE %s_admin", schema))

  if(install_tables) {
    setup_sql_tables(con, schema, prnt)
  }

  if(install_functions) {
    setup_sql_functions(con, schema, prnt)
  }

  setup_sql_triggers(con, schema, prnt)
  setup_sql_grant_rights(con, schema, prnt)

  dbExecute(con, sprintf("SET ROLE %s", current_user))
}


# stuff to be run as root ------------------------------------------------


#' Install PostgreSQL Schemas and Extensions
#'
#' Installs schema, uuid-ossp, btree_gist.
#' This function must be run with a connection of a database level admin.
#'
#' @param con RPostgres connection object.
setup_sql_extentions <- function(con, schema = "timeseries"){
  sql <- readLines(system.file("sql/create_extensions.sql",
                               package = "timeseriesdb"))
  sql <- gsub("timeseries", schema, sql)
  # split up SQL by a new set of lines everytime CREATE FUNCTION
  # occurs in order to send single statements using multiple execute calls
  # which is DBI / RPostgres compliant
  lapply(split(sql, cumsum(grepl("CREATE OR REPLACE ", sql))),
         function(x){
           dbExecute(con, paste(x, collapse = "\n"))
         })
}

#' Create Roles needed for operation of timeseriesdb
#'
#' This function must be run with a connection of a database level admin.
#'
#' @param con RPostgres connection object
#' @param schema schema character schema name, defaults to 'timeseries'.
setup_sql_roles <- function(con, schema = "timeseries") {
  sql <- readLines(system.file("sql/create_roles.sql",
                               package = "timeseriesdb"))
  sql <- gsub("timeseries_", sprintf("%s_", schema), sql)

  lapply(split(sql, cumsum(grepl("CREATE|GRANT", sql))),
         function(x) {
           dbExecute(con, paste(x, collapse = "\n"))
         })
}

# stuff to be run as timeseries admin -------------------------------------

#' Install {timeseriesdb} System Tables
#'
#' Installs tables needed to operated {timeseriesdb} in
#' a given PostgreSQL schema. The tables use a default SQL file installed
#' with the package to generate SQL tables. The default schema 'timeseries'
#' can be replaced using the 'schema' parameter.
#'
#' @param con PostgreSQL connection object created by the RPostgres package.
#' @param schema character schema name, defaults to 'timeseries'.
setup_sql_tables <- function(con, schema = "timeseries", prnt = identity){
  prnt("Setting up tables...")
  sql <- readLines(system.file("sql/create_tables.sql",
                               package = "timeseriesdb"))
  sql <- gsub("timeseries\\.", sprintf("%s.", schema), sql)
  # split up SQL by a new set of lines everytime CREATE TABLES or INSERT INTO
  # occurs in order to send single statements using multiple execute calls
  # which is DBI / RPostgres compliant
  lapply(split(sql, cumsum(grepl("CREATE|INSERT INTO",sql))),
         function(x){
           prnt(x[1])
           dbExecute(con, paste(x, collapse = "\n"))
         })
  prnt("Done")
}





#' Install {timeseriesdb} System Functions
#'
#' Installs functions needed to operated {timeseriesdb} in
#' a given PostgreSQL schema. The functions uses a default SQL file installed
#' with the package to generate SQL functions. The default schema 'timeseries'
#' can be replaced using the 'schema' parameter.
#'
#' @param con PostgreSQL connection object created by the RPostgres package.
#' @param schema character schema name, defaults to 'timeseries'.
setup_sql_functions <- function(con, schema = "timeseries", prnt = identity){
  prnt("Setting up functions")
  fls <- list.files(
    system.file(
      "sql",
      package = "timeseriesdb"
    ),
    "create_functions",
    full.names = TRUE
  )

  for(f in fls) {
    prnt(f)
    sql <- readLines(f)
    # [^m] to exclude the TABLE timeseries_main but include timeseries_admin
    sql <- gsub("timeseries([.,_](?!main))", sprintf("%s\\1", schema), sql, perl = TRUE)
    # split up SQL by a new set of lines everytime CREATE FUNCTION
    # occurs in order to send single statements using multiple execute calls
    # which is DBI / RPostgres compliant
    lapply(split(sql, cumsum(grepl("CREATE OR REPLACE FUNCTION",sql))),
           function(x){
             prnt(x[1])
             dbExecute(con, paste(x, collapse = "\n"))
           })
  }

  prnt("Done")
}


#' Install {timeseriesdb} Triggers
#'
#' Installs functions needed for timeseriesdb triggers and sets up these triggers in
#' a given PostgreSQL schema. The functions uses a default SQL file installed
#' with the package to generate SQL functions. The default schema 'timeseries'
#' can be replaced using the 'schema' parameter.
#'
#' @param con PostgreSQL connection object created by the RPostgres package.
#' @param schema character schema name, defaults to 'timeseries'.
setup_sql_triggers <- function(con, schema = "timeseries", prnt = identity){
  prnt("Setting up triggers")
  sql <- readLines(system.file("sql/create_triggers.sql",
                               package = "timeseriesdb"))
  sql <- gsub("timeseries", schema, sql)
  # split up SQL by a new set of lines everytime CREATE FUNCTION
  # occurs in order to send single statements using multiple execute calls
  # which is DBI / RPostgres compliant
  lapply(split(sql, cumsum(grepl("CREATE OR REPLACE FUNCTION|CREATE TRIGGER|DROP TRIGGER",sql))),
         function(x){
           prnt(x[1])
           dbExecute(con, paste(x, collapse = "\n"))
         })
  prnt("Done")
}

#' Grant execute on {timeseriesdb} functions
#'
#' @param con RPostgres connection object
#' @param schema character schema name, defaults to 'timeseries'
#'
#' @return
setup_sql_grant_rights <- function(con, schema = "timeseries", prnt = identity) {
  prnt("Setting up function rights")
  sql <- readLines(system.file("sql/grant_rights.sql",
                               package = "timeseriesdb"))
  sql <- gsub("timeseries", schema, sql)

  lapply(split(sql, cumsum(grepl("GRANT|REVOKE", sql))),
         function(x) {
           prnt(x[1])
           dbExecute(con, paste(x, collapse = "\n"))
         })
  prnt("Done")
}
