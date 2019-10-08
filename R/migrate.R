addReleaseDateToTimeseriesMain <- function(schema = "timeseries",
                                  tbl = "timeseries_main") {
  sql_query <- sprintf("ALTER TABLE %s.%s ADD IF NOT EXISTS ts_release_date timestamp with time zone DEFAULT '1900-01-01 00:00:00'",
                       schema, tbl)
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
#' @rdname createTables
alterTimeseriesSets <- function(schema = "timeseries",
                                tbl = "timeseries_sets"){
  sql_query <- sprintf("ALTER TABLE %s.%s ALTER COLUMN key_set TYPE TEXT[] using akeys(key_set);", schema, tbl)
  class(sql_query) <- "SQL"
  sql_query
}


alterMetaUnlocalized <- function(schema = "timeseries",
                                 tbl = "meta_data_unlocalized") {
  sql_query <- sprintf("ALTER TABLE %s.%s ALTER COLUMN meta_data TYPE JSONB USING meta_data::JSONB", schema, tbl)
  class(sql_query) <- "SQL"
  sql_query
}


alterMetaLocalized <- function(schema = "timeseries",
                               tbl = "meta_data_localized") {
  sql_query <- sprintf("ALTER TABLE %s.%s ALTER COLUMN meta_data TYPE JSONB USING meta_data::JSONB", schema, tbl)
  class(sql_query) <- "SQL"
  sql_query
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
  status$alter_meta_unlocalized <- attributes(runDbQuery(con, alterMetaUnlocalized(schema = schema)))
  status$alter_meta_localized <- attributes(runDbQuery(con, alterMetaLocalized(schema = schema)))
  status$alter_sets <- attributes(runDbQuery(con, alterTimeseriesSets(schema = schema)))
  
  status
}
