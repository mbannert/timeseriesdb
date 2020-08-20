#' Common parameters
#'
#' @param con RPostgres connection object.
#' @param schema \strong{character} name of the database schema. Defaults to 'timeseries'
#' @param ts_keys \strong{character} vector of time series identifiers.
#' @param dataset \strong{character} name of the dataset. Datasets are group of time series.
#' @param datasets \strong{character} vector of the datasets. Dataset is a group of time series.
#' @param valid_on \strong{character} representation of a date in the form of 'YYYY-MM-DD'. valid_on selects the
#' version of a time series that is valid at the specified time.
#' @param valid_from character representation of a date in the form of 'YYYY-MM-DD'. valid_from starts a new version
#' @param code expression Code to be evaluated after populating the temporary table on the database
#' of a time series that is valid from the specified date.
#' @param collection_name \strong{character} name of a collection to read. Collection are bookmark lists that contain time series keys.
#' @param access_level \strong{character} describing the access level of the time series or dataset.
#' @param set_name \strong{character} name of a dataset.
#' @param regex \strong{boolean} indicating if ts_keys should be interpreted as a regular expression pattern. Defaults to FALSE.
#' @param locale \strong{character} indicating the language of the meta information to be store. We recommend to use ISO country codes to represent languages. Defaults to NULL. When local is set to NULL, metadata are stored without localization. Note that, when localizing meta information by assigning a language, multiple meta information objects can be stored for a single time series.
#' @param respect_release_date \strong{boolean} indicating if it should the release embargo of a time series be respected. Defaults to FALSE. This option makes sense when the function is used in an API. In that sense, users do not have direct access to this function and therefore cannot simply switch parameters.
#' @param chunksize set a limit of the number of time series requested in the function.
#' @param collection_owner \strong{character} username that is the owner of a collection.
#' @param user character name of the database user. Defaults to the user of the R session.
#'             this is often the user for the database, too so you do not have to specify
#'             your username explicitly if that is the case.
#' @name param_defs
NULL
