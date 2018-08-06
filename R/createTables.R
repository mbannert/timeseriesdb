#' Create Statements for PostgreSQL tables
#' 
#' These function creates statements to set up 5 Tables used to manage and archive time series information in PostgreSQL.
#' Make sure you have sufficient rights to create relations in your PostgreSQL schema. These function are only used for an initial setup. You can either run this group of functions separately or use \code{\link{runCreateTables}} to 
#' run all functions at once. 
#' 
#' @details 
#' The following tables will be create in the given schema.
##' \itemize{
##'  \item{"timeseries_main"}{contains time series themselves as hstore key value pairs.}
##'  \item{"timeseries_vintages"}{contains vintages of time series.
##'   This is useful for published data that can be revised. see also OECD defintion of vintages}
##'  \item{"timeseries_sets"}{contains a vector of time series keys. This table can be used like a shopping cart in an e-commerce application. }
##'  \item{"meta_data_unlocalized"}{contains translation agnostic meta information, e.g., username.}
##'  \item{"meta_data_localized"}{contains translation specific meta information, e.g., wording of a question.}
##' }
##' 
#' @references OECD Defintion of vintages: \url{http://www.oecd.org/std/40315408.pdf}
#' 
#' @param schema character denoting a PostgreSQL schema
#' @param tbl character denoting a table name
#' @param main character denoting name of the main table for referencing. This argument is only available to meta data statements.
#' @rdname createTable
#' @export 
createTimeseriesMain <- function(schema = "timeseries",
                                 tbl = "timeseries_main"){
  sql_query <- sprintf("CREATE TABLE %s.%s (ts_key text primary key,
                        ts_data hstore, 
                        ts_frequency integer,
                        ts_release_date timestamp with time zone DEFAULT '1900-01-01 00:00:00')",
                       schema,
                       tbl)
  class(sql_query) <- "SQL"
  
  sql_query
}

addReleaseDateToTimeseriesMain <- function(schema = "timeseries",
                                  tbl = "timeseries_main") {
  sql_query <- sprintf("ALTER TABLE %s.%s ADD IF NOT EXISTS ts_release_date timestamp with time zone DEFAULT '1900-01-01 00:00:00'",
                       schema, tbl)
  class(sql_query) <- "SQL"
  
  sql_query
}

# OECD vintage defintion: 
# http://www.oecd.org/std/40315408.pdf

#' @export
#' @rdname createTable
createTimeseriesVintages <- function(schema = "timeseries",
                                     tbl = "timeseries_vintages"){
  sql_query <- sprintf("
                       CREATE TABLE %s.%s (ts_key text,
                                           ts_validity daterange,
                                           ts_data hstore, 
                                           ts_frequency integer,
                                           ts_release_date timestamp with time zone DEFAULT '1900-01-01 00:00:00');
                       ALTER TABLE %s.%s
                       ADD PRIMARY KEY (ts_key, ts_validity);
                       ALTER TABLE %s.%s
                       ADD EXCLUDE USING GIST (ts_key WITH =, ts_validity WITH &&);
                       ",
                       schema,tbl,
                       schema,tbl,
                       schema,tbl)
  class(sql_query) <- "SQL"
  sql_query
}

addReleaseDateToTimeseriesVintages <- function(schema = "timeseries",
                                           tbl = "timeseries_vintages") {
  sql_query <- sprintf("ALTER TABLE %s.%s ADD IF NOT EXISTS ts_release_date timestamp with time zone DEFAULT '1900-01-01 00:00:00'",
                       schema, tbl)
  class(sql_query) <- "SQL"
  
  sql_query
}

#' @export
#' @rdname createTable
createTimeseriesSets <- function(schema = "timeseries",
                                 tbl = "timeseries_sets"){
  sql_query <- sprintf("CREATE TABLE %s.%s (
                        setname text,
                        username text,
                        tstamp timestamptz,
                        key_set hstore,
                        set_description varchar,
                        active bool,
                        primary key(setname, username));",
                       schema,tbl)
  class(sql_query) <- "SQL"
  sql_query
}



#' @export
#' @rdname createTable
createMetaUnlocalized <- function(schema = "timeseries",
                                  tbl = "meta_data_unlocalized",
                                  main = "timeseries_main"){
  sql_query <- sprintf("CREATE TABLE %s.%s (
                        ts_key text,
                        md_generated_by text,
                        md_resource_last_update timestamptz,
                        md_coverage_temp varchar,
                        meta_data hstore,
                        primary key (ts_key),
                        foreign key (ts_key) references %s.%s (ts_key) on delete cascade);",
                       schema,tbl,
                       schema,main)
  class(sql_query) <- "SQL"
  sql_query
}

#' @export
#' @rdname createTable
createMetaLocalized <- function(schema = "timeseries",
                                tbl = "meta_data_localized",
                                main = "timeseries_main"){
  sql_query <- sprintf("CREATE TABLE %s.%s (
                        ts_key varchar,
                        locale_info varchar, 
                        meta_data hstore,
                        primary key (ts_key, locale_info),
                        foreign key (ts_key) references %s.%s (ts_key) on delete cascade);",
                       schema,tbl,
                       schema,main)
  class(sql_query) <- "SQL"
  sql_query
}


#' @export
#' @rdname createTable
createMetaDatasets <- function(schema = "timeseries",
                                tbl = "meta_datasets"){
  
  sql_query <- sprintf("CREATE TABLE %s.%s (
                       dataset_id text,
                       meta_data jsonb,
                       primary key(dataset_id));",
                       schema,tbl)
  
  class(sql_query) <- "SQL"
  sql_query
}





#' Run Setup: Create all mandatory tables
#' 
#' Creates all tables absolutely needed for timeseriesdb to work correctly. 
#' This function should only be run once as an initial setup. Make sure you got sufficient 
#' access rights. The function returns a list of status reports for the its 5 database queries. 
#' look at this helps you to see whether anything went wrong. 
#' 
#' @param con PostgreSQL connection object. Typically created with \code{\link{createConObj}}.
#' @param schema character denoting a PostgreSQL schema.
#' @rdname runCreateTables
#' @export
runCreateTables <- function(con,schema = "timeseries"){
  status <- list()
  status$timeseries_main <- attributes(runDbQuery(con,createTimeseriesMain(schema = schema)))
  status$timeseries_vintages <- attributes(runDbQuery(con,createTimeseriesVintages(schema = schema)))
  status$timeseries_sets <- attributes(runDbQuery(con,createTimeseriesSets(schema = schema)))
  status$meta_localized <- attributes(runDbQuery(con,createMetaLocalized(schema = schema)))
  status$meta_unlocalized <- attributes(runDbQuery(con,createMetaUnlocalized(schema = schema)))
  status$meta_datasets <- attributes(runDbQuery(con, createMetaDatasets(schema = schema)))
  status
}


#' Add Release Date Column to Tables
#' 
#' Adds a release column to tables of older versions of timeseriesdb. 
#' 
#'
#' @param con PostgreSQLL connection object
#' @param schema database schema, defaults to 'timeseries'.
#'
#' @export
runUpgradeTables <- function(con, schema = "timeseries") {
  status <- list()
  status$timeseries_main <- attributes(runDbQuery(con, addReleaseDateToTimeseriesMain(schema = schema)))
  status$timeseries_vintages <- attributes(runDbQuery(con, addReleaseDateToTimeseriesVintages(schema = schema)))
  status
}
