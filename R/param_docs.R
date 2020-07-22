#' @name param_defs
#' aka paramses
#' @param con RPostgres connection object.
#' @param schema character name of the database schema. Defaults to 'timeseries'
#' @param ts_keys \strong{character} vector of time series identifiers.
#' @param dataset character name of the dataset. Datasets are group of time series.
#' @param valid_on character representation of a date in the form of 'YYYY-MM-DD'. valid_on selects the 
#' version of a time series that is valid at the specified time. 
#' @param valid_from character representation of a date in the form of 'YYYY-MM-DD'. valid_from starts a new version 
#' of a time series that is valid from the specified date.
#' @param access_level character describing the access level of the time series or dataset. 
#' @param collection_name character name of a collection. Collection are bookmark lists that contain time series keys. 
#' @param owner character username that is the owner of a collection.
#' @param set_name character name of a dataset.
#' @param metadata list 
#' @param regex boolean
#' @param locale character
#' @param respect_release_date boolean
#' @param chunksize 

