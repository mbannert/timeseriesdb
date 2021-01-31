#' Read Time Series From PostgreSQL into R
#'
#' Read specific version of a time series given time series key (unique identifier) and validity. By default, this function returns the most recent version of a time series.
#'
#' @inheritParams param_defs
#' @family time series functions
#'
#' @return list of time series. List elements vary depending on nature of time series, i.e., regular vs. irregular time series.
#' @import data.table
#' @importFrom RPostgres dbSendQuery dbClearResult dbQuoteLiteral dbQuoteIdentifier Id
#' @export
#'
#' @examples
#'
#' \dontrun{
#' db_ts_store(con = connection, zrh_airport, schema = "schema")
#' db_ts_read(con = connection, ts_keys = "ch.zrh_airport.departure.total", schema = "schema")
#' }
db_ts_read <- function(con,
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

  keys_unique <- unique(ts_keys)

  if(length(keys_unique) != length(ts_keys)) {
    warning("Duplicate keys removed. Return list will only contain one instance of each series.")
  }

  # timeseriesdb makes use of a temporary table that is joined against
  # to get the right data. This is much faster than WHERE clauses.
  tsl <- db_with_tmp_read(con,
                          keys_unique,
                          regex,
                          {
                            res <- dbSendQuery(con, sprintf("select * from %sts_read_raw(%s, %s)",
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
#' @inheritParams param_defs
#' @family time series functions
#'
#' @export
#' @importFrom RPostgres dbSendQuery dbQuoteIdentifier dbQuoteLiteral Id
#'
#' @examples
#'
#' \dontrun{
#'
#' # Storing different versions of the data, use parameter valid_from
#' # different versions are stored with the same key
#' ch.kof.barometer <- kof_ts["baro_2019m11"]
#' names(ch.kof.barometer) <- c("ch.kof.barometer")
#' db_ts_store(con = connection,
#'                   ch.kof.barometer,
#'                   valid_from = "2019-12-01",
#'                   schema = "schema")
#'
#' ch.kof.barometer <- kof_ts["baro_2019m12"]
#' names(ch.kof.barometer) <- c("ch.kof.barometer")
#' db_ts_store(con = connection,
#'                   ch.kof.barometer,
#'                   valid_from = "2020-01-01",
#'                   schema = "schema")
#'
#' # Reading all versions
#' db_ts_read_history(con = connection,
#'                          ts_key = "ch.kof.barometer",
#'                          schema = "schema")
#' }
db_ts_read_history <- function(con,
                                     ts_key,
                                     respect_release_date = FALSE,
                                     schema = "timeseries") {
  res <- dbSendQuery(con, sprintf("SELECT * FROM %sts_read_history_raw(%s, %s)",
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
#' @inheritParams param_defs
#' @family time series functions
#'
#' @export
#'
#' @examples
#'
#' \dontrun{
#' db_dataset_create(con = connection,
#'                   set_name = "zrh_airport_data",
#'                   set_description = "Zurich airport arrivals and departures ",
#'                   schema = "schema")
#'
#' db_ts_assign_dataset(con = connection,
#'                      ts_keys = c("ch.zrh_airport.departure.total",
#'                                  "ch.zrh_airport.arrival.total"),
#'                      set_name = "zrh_airport_data",
#'                      schema = "schema")
#'
#' db_dataset_read_ts(con = connection,
#'                    datasets = "zrh_airport_data",
#'                    schema = "schema")
#' }
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
                res <- dbSendQuery(con, sprintf("SELECT * FROM %sts_read_dataset_raw(%s, %s)",
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
#' @inheritParams param_defs
#' @family time series functions
#'
#' @details
#' Collections are identified by their name and owner. Several collections
#' with the same name but different owners may exist, therefore both need to be supplied
#' in order to uniquely identify a collection.
#'
#' @export
#'
#' @examples
#'
#' \dontrun{
#'
#' db_ts_store(con = connection, zrh_airport, schema = "schema")
#' db_ts_store(con = connection, kof_ts, schema = "schema")
#'
#' db_collection_add_ts(
#'   con = connection,
#'   collection_name = "barometer and departures zurich",
#'   ts_keys = c(
#'     "ch.zrh_airport.departure.total",
#'     "ch.zrh_airport.departure.total",
#'     "ch.kof.barometer"
#'   ),
#'   schema = "schema"
#' )
#'
#' db_collection_read_ts(
#'   con = connection,
#'   collection_name = "barometer and departures zurich",
#'   collection_owner = "user_name",
#'   schema = "schema"
#' )
#' }
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


  res <- dbSendQuery(con, sprintf("SELECT * FROM %sts_read_collection_raw(%s, %s, %s, %s)",
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


#' Get the times series last update
#'
#' @inheritParams param_defs
#' @family time series functions
#'
#' @export
#'
#' @examples
#'
#' \dontrun{
#' db_ts_store(con = connection, zrh_airport, schema = "schema")
#'
#' # get last update for one key
#' db_ts_get_last_update(
#'   con = connection,
#'   ts_keys = "ch.zrh_airport.departure.total",
#'   schema = "schema")
#'
#' # get last update for multiple keys
#' db_ts_get_last_update(
#'   con = connection,
#'   ts_keys = c(
#'     "ch.zrh_airport.departure.total",
#'     "ch.zrh_airport.arrival.total"
#'   ),
#'   schema = "schema"
#' )
#' }
db_ts_get_last_update <- function(con,
                                  ts_keys,
                                  schema = "timeseries") {
  db_with_tmp_read(con,
                   ts_keys,
                   code = {
                      db_call_function(con,
                                       "ts_get_last_update",
                                       schema = schema)
                   },
                   schema = schema)
}

#' Get All keys that follow a pattern
#'
#' @inheritParams param_defs
#' @param pattern \strong{character} that represents a regular expression to find keys
#' @family access levels functions
#'
#' @export
#'
#' @examples
#'
#' \dontrun{
#' db_ts_store(con = connection, zrh_airport, schema = "schema")
#'
#' # get all keys that start with "ch"
#' db_ts_find_keys(
#'   con = connection,
#'   "^ch",
#'   schema = "schema")
#' }
db_ts_find_keys <- function(con,
                            pattern,
                            schema = "timeseries") {

  out <- db_call_function(con,
                          "ts_find_keys",
                          list(
                            pattern
                          ),
                          schema = schema)$ts_key
  out
}

