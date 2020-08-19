#' Read Time Series From PostgreSQL into R
#'
#' Read specific version of a time series given time series key (unique identifier) and validity. By default, this function returns the most recent version of a time series.
#'
#'
#' @param con RPostgres connection object.
#' @param ts_keys character vector containing time series keys which identify a time series uniquely.
#' @param valid_on character representing a date of the form YYYY-MM-DD.
#' @param regex boolean should ts_keys be interpreted as regular expression? Defaults to FALSE.
#' @param respect_release_date boolean Should the release embargo of a time series be respected? Defaults to FALSE. This option makes sense when the function is used in an API in that sense that users do not have direct access to this function and therefore cannot simply switch barameters.
#' @param schema character name of the schema
#' @param chunksize
#'
#' @return list of time series. List elements vary depending on nature of time series, i.e., regular vs. irregular time series.
#' @import data.table
#' @importFrom RPostgres dbSendQuery dbClearResult dbQuoteLiteral dbQuoteIdentifier Id
#' @export
read_time_series <- function(con,
                             ts_keys,
                             valid_on = NULL,
                             regex = FALSE,
                             respect_release_date = FALSE,
                             schema = "timeseries",
                             chunksize = 10000) {

  # RPostgres plays nicer with NA than with NULL
  if(is.null(valid_on)) {
    valid_on <- NA
  }

  # timeseriesdb makes use of a temporary table that is joined against
  # to get the right data. This is much faster than WHERE clauses.
  tsl <- db_with_tmp_read(con,
                          ts_keys,
                          regex,
                          {
                            res <- dbSendQuery(con, sprintf("select * from %sread_ts_raw(%s, %s)",
                                                     dbQuoteIdentifier(con, Id(schema = schema)),
                                                     dbQuoteLiteral(con, valid_on),
                                                     dbQuoteLiteral(con, respect_release_date)))
                            tsl <- get_tsl_from_res(res, chunksize)
                            dbClearResult(res)
                            tsl
                          },
                          schema = schema)


  class(tsl) <- c("tslist", "list")

  tsl
}

#' Read the Entire History of a Time Series
#'
#' This function returns a list whose keys correspond to the date on which the
#' contained version of the time series took effect.
#'
#' @param con RPostgres connection
#' @param ts_key character The key of the series to read
#' @param respect_release_date boolean Should the release embargo of a time series be respected? Defaults to FALSE.
#' @param schema character Time Series Schema Name
#'
#' @export
#' @importFrom RPostgres dbSendQuery dbQuoteIdentifier dbQuoteLiteral Id
read_time_series_history <- function(con,
                                     ts_key,
                                     respect_release_date = FALSE,
                                     schema = "timeseries") {
  res <- dbSendQuery(con, sprintf("SELECT * FROM %sread_ts_history_raw(%s, %s)",
                                  dbQuoteIdentifier(con, Id(schema = schema)),
                                  dbQuoteLiteral(con, ts_key),
                                  dbQuoteLiteral(con, respect_release_date)))


  tsl <- get_tsl_from_res(res, chunksize = 10000, id.col = "validity")
  class(tsl) <- c("tslist", "list")

  dbClearResult(res)
  tsl
}

#' Read all Time Series in a Dataset
#'
#' @param con RPostgres connection object.
#' @param datasets character Names of the datasets to read
#' @param valid_on character representing a date of the form YYYY-MM-DD.
#' @param respect_release_date boolean Should the release embargo of a time series be respected? Defaults to FALSE.
#'                             This option makes sense when the function is used in an API in that sense that users
#'                             do not have direct access to this function and therefore cannot simply switch barameters.
#' @param schema character name of the schema
#' @param chunksize
#'
#' @export
db_dataset_read_ts <- function(con,
                                        datasets,
                                        valid_on = NULL,
                                        respect_release_date = FALSE,
                                        schema = "timeseries",
                                        chunksize = 10000) {
  if(is.null(valid_on)) {
    valid_on <- NA
  }

  db_with_temp_table(con,
              "tmp_datasets_read",
              data.frame(
                set_id = datasets
              ),
              field.types = c(set_id = "text"),
              {
                res <- dbSendQuery(con, sprintf("SELECT * FROM %sread_ts_dataset_raw(%s, %s)",
                                                dbQuoteIdentifier(con, Id(schema = schema)),
                                                dbQuoteLiteral(con, valid_on),
                                                dbQuoteLiteral(con, respect_release_date)))

                tsl <- get_tsl_from_res(res, chunksize)
                dbClearResult(res)
                class(tsl) <- c("tslist", "list")
                tsl
              },
              schema = schema)
}

#' Read all Time Series in a User Collection
#'
#'
#' @param con RPostgres connection object.
#' @param collection_name character Name of the collection to read
#' @param collection_owner character Owner of the collection to read
#' @param valid_on character representing a date of the form YYYY-MM-DD.
#' @param respect_release_date boolean Should the release embargo of a time series be respected? Defaults to FALSE.
#'                             This option makes sense when the function is used in an API in that sense that users
#'                             do not have direct access to this function and therefore cannot simply switch barameters.
#' @param schema character name of the schema
#' @param chunksize
#'
#' @details
#' Collections are identified by their name and owner. Several collections
#' with the same name but different owners may exist, therefore both need to be supplied
#' in order to uniquely identify a collection.
#'
#' @export
db_collection_read_ts <- function(con,
                                           collection_name,
                                           collection_owner,
                                           valid_on = NULL,
                                           respect_release_date = FALSE,
                                           schema = "timeseries",
                                           chunksize = 10000) {

  if(is.null(valid_on)) {
    valid_on <- NA
  }

  res <- dbSendQuery(con, sprintf("SELECT * FROM %sread_ts_collection_raw(%s, %s, %s, %s)",
                                  dbQuoteIdentifier(con, Id(schema = schema)),
                                  dbQuoteLiteral(con, collection_name),
                                  dbQuoteLiteral(con, collection_owner),
                                  dbQuoteLiteral(con, valid_on),
                                  dbQuoteLiteral(con, respect_release_date)))

  tsl <- get_tsl_from_res(res, chunksize)
  dbClearResult(res)
  class(tsl) <- c("tslist", "list")
  tsl
}

#' @importFrom RPostgres dbHasCompleted dbFetch
#' @import data.table
get_tsl_from_res <- function(res, chunksize = 10000, id.col = "ts_key") {
  tsl <- list()
  while(!dbHasCompleted(res)) {
    chunk <- data.table(dbFetch(res, n = chunksize))

    tsl[chunk[, get(id.col)]] <- chunk[, .(ts_obj = list(json_to_ts(ts_data))), by = list(get(id.col))]$ts_obj
  }

  tsl
}
