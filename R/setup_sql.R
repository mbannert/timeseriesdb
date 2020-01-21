#' Install PostgreSQL Schemas and Extensions
#' 
#' Installs schema, uuid-ossp, btree_gist.
#' 
#' @param con PostgreSQL connection object
#' @export
setup_sql_extentions <- function(con, schema = "timeseries"){
  sql <- readLines(system.file("sql/create_extensions.sql",
                               package = "timeseriesdb"))
  sql <- gsub("timeseries\\.", sprintf("%s.", schema), sql)
  # split up SQL by a new set of lines everytime CREATE FUNCTION 
  # occurs in order to send single statements using multiple execute calls
  # which is DBI / RPostgres compliant 
  lapply(split(sql, cumsum(grepl("CREATE ", sql))),
         function(x){
           dbExecute(con, paste(x, collapse = "\n"))
         })
}


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
  lapply(split(sql, cumsum(grepl("CREATE TABLE|INSERT INTO",sql))),
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
  sql <- readLines(system.file("sql/create_functions.sql",
                               package = "timeseriesdb"))
  sql <- gsub("timeseries\\.", sprintf("%s.", schema), sql)
  # split up SQL by a new set of lines everytime CREATE FUNCTION 
  # occurs in order to send single statements using multiple execute calls
  # which is DBI / RPostgres compliant 
  lapply(split(sql, cumsum(grepl("CREATE FUNCTION",sql))),
         function(x){
           dbExecute(con, paste(x, collapse = "\n"))
         })

}


