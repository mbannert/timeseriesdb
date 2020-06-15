# w/o root admin (for now)
#' Title
#'
#' @param user database user name
#' @param password database user password
#' @param database database name
#' @param host database host (default 'localhost')
#' @param port database port (default 5432)
#' @param schema timeseries schema name (default 'timeseries')
#'
#' @importFrom RPostgres dbConnect Postgres dbGetQuery dbIsValid
#'
#' @export
#'
install_timeseriesdb <- function(username,
                                 password,
                                 database,
                                 host = "localhost",
                                 port = 5432,
                                 schema = "timeseries") {
  con <- dbConnect(Postgres(), database, host, port, username, password)

  schema_exists <- dbGetQuery(con,
                              "SELECT true
                              FROM information_schema.schemata
                              WHERE schema_name = $1;",
                              list(schema))$bool

  if(length(schema_exists) == 0) {
    # TODO: ya know...
    stop(sprintf("Schema %s does not exist. blabla admin bla documentation"))
  }

  setup_sql_tables(con, schema)
  setup_sql_functions(con, schema)
  setup_sql_triggers(con, schema)
  grant_sql_rights(con, schema)
}


# stuff to be run as root ------------------------------------------------


#' Install PostgreSQL Schemas and Extensions
#'
#' Installs schema, uuid-ossp, btree_gist.
#' This function must be run with a connection of a database level admin.
#'
#' @param con RPostgres connection object.
#' @export
setup_sql_extentions <- function(con, schema = "timeseries"){
  sql <- readLines(system.file("sql/create_extensions.sql",
                               package = "timeseriesdb"))
  sql <- gsub("timeseries", schema, sql)
  # split up SQL by a new set of lines everytime CREATE FUNCTION
  # occurs in order to send single statements using multiple execute calls
  # which is DBI / RPostgres compliant
  lapply(split(sql, cumsum(grepl("CREATE ", sql))),
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
#' @export
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
#' @export
setup_sql_tables <- function(con, schema = "timeseries"){
  sql <- readLines(system.file("sql/create_tables.sql",
                               package = "timeseriesdb"))
  sql <- gsub("timeseries\\.", sprintf("%s.", schema), sql)
  # split up SQL by a new set of lines everytime CREATE TABLES or INSERT INTO
  # occurs in order to send single statements using multiple execute calls
  # which is DBI / RPostgres compliant
  lapply(split(sql, cumsum(grepl("CREATE|INSERT INTO",sql))),
         function(x){
           dbExecute(con, paste(x, collapse = "\n"))
         })
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
#' @export
setup_sql_functions <- function(con, schema = "timeseries"){
  fls <- list.files(
    system.file(
      "sql",
      package = "timeseriesdb"
    ),
    "create_functions",
    full.names = TRUE
  )

  for(f in fls) {
    sql <- readLines(f)
    # [^m] to exclude the TABLE timeseries_main but include timeseries_admin
    sql <- gsub("timeseries([.,_](?!main))", sprintf("%s\\1", schema), sql, perl = TRUE)
    # split up SQL by a new set of lines everytime CREATE FUNCTION
    # occurs in order to send single statements using multiple execute calls
    # which is DBI / RPostgres compliant
    lapply(split(sql, cumsum(grepl("CREATE FUNCTION",sql))),
           function(x){
             dbExecute(con, paste(x, collapse = "\n"))
           })
  }

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
#' @export
setup_sql_triggers <- function(con, schema = "timeseries"){
  sql <- readLines(system.file("sql/create_triggers.sql",
                               package = "timeseriesdb"))
  sql <- gsub("timeseries", schema, sql)
  # split up SQL by a new set of lines everytime CREATE FUNCTION
  # occurs in order to send single statements using multiple execute calls
  # which is DBI / RPostgres compliant
  lapply(split(sql, cumsum(grepl("CREATE FUNCTION|CREATE TRIGGER",sql))),
         function(x){
           dbExecute(con, paste(x, collapse = "\n"))
         })

}

#' Grant execute on {timeseriesdb} functions
#'
#' @param con RPostgres connection object
#' @param schema character schema name, defaults to 'timeseries'
#'
#' @return
#' @export
#'
#' @examples
grant_sql_rights <- function(con, schema = "timeseries") {
  sql <- readLines(system.file("sql/grant_rights.sql",
                               package = "timeseriesdb"))
  sql <- gsub("timeseries", schema, sql)

  lapply(split(sql, cumsum(grepl("GRANT|REVOKE", sql))),
         function(x) {
           dbExecute(con, paste(x, collapse = "\n"))
         })
}
