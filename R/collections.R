#' Bundles Keys into an Existing Collection or Adds a New Collection
#'
#' @param description \strong{character} description of the collection.
#'
#' @inheritParams param_defs
#' @family collections functions
#'
#' @importFrom jsonlite fromJSON
#' @export
#'
#' @examples
#'
#' \dontrun{
#' store_time_series(con = connection, zrh_airport, schema = "schema")
#' store_time_series(con = connection, kof_ts, schema = "schema")
#'
#' db_ts_add_to_collection(
#'   con = connection,
#'   collection_name = "barometer and departures zurich",
#'   ts_keys = c(
#'     "ch.zrh_airport.departure.total",
#'     "ch.zrh_airport.departure.total",
#'     "ch.kof.barometer"
#'   ),
#'   schema = "schema"
#' )
#' }
db_ts_add_to_collection <- function(con,
                                    collection_name,
                                    ts_keys,
                                    description = NA,
                                    user = Sys.info()["user"],
                                    schema = "timeseries") {
  keys <- unique(ts_keys)

  # let's add keys: fill a temp table, anti-join the keys
  # INSERT non existing ones.
  dt <- data.table(
    ts_key = keys
  )

  db_return <- db_with_temp_table(con,
    "tmp_collect_updates",
    dt,
    field.types = c(
      ts_key = "text"
    ),
    fromJSON(db_call_function(con,
      "insert_collect_from_tmp",
      list(collection_name, user, description),
      schema = schema
    )),
    schema = schema
  )

  if (db_return$status == "warning") {
    warning(db_return$message)
  }

  db_return
}


#' Remove Keys From a User's Collection
#'
#' Removes a vector of time series keys from an a set of
#' keys defined for that user.
#'
#' @inheritParams param_defs
#' @family collections functions
#'
#' @importFrom jsonlite fromJSON
#' @export
#'
#' @examples
#'
#' \dontrun{
#' store_time_series(con = connection, zrh_airport, schema = "schema")
#' store_time_series(con = connection, kof_ts, schema = "schema")
#'
#' db_ts_add_to_collection(
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
#' db_ts_remove_from_collection(
#'   con = connection,
#'   collection_name = "barometer and departures zurich",
#'   ts_keys = "ch.zrh_airport.departure.total",
#'   schema = "schema"
#' )
#' }
db_ts_remove_from_collection <- function(con,
                                         collection_name,
                                         ts_keys,
                                         user = Sys.info()["user"],
                                         schema = "timeseries") {
  keys <- unique(ts_keys)

  # write temp table
  dt <- data.table(ts_key = keys)

  db_return <- db_with_temp_table(con,
    "tmp_collection_remove",
    dt,
    field.types = c(
      ts_key = "text"
    ),
    fromJSON(db_call_function(
      con,
      "collection_remove",
      list(collection_name, user),
      schema
    )),
    schema = schema
  )

  if (db_return$status == "error") {
    stop(db_return$message)
  }

  db_return
}


#' Remove an Entire Time Series Key Collection
#'
#' @inheritParams param_defs
#' @family collections functions
#'
#' @return
#'
#' @importFrom jsonlite fromJSON
#' @export
#'
#' @examples
#'
#' \dontrun{
#' store_time_series(con = connection, zrh_airport, schema = "schema")
#' store_time_series(con = connection, kof_ts, schema = "schema")
#'
#' db_ts_add_to_collection(
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
#' db_collection_delete(
#'   con = connection,
#'   collection_name = "barometer and departures zurich",
#'   schema = "schema"
#' )
#' }
db_collection_delete <- function(con,
                                 collection_name,
                                 user = Sys.info()["user"],
                                 schema = "timeseries") {
  db_return <- fromJSON(db_call_function(con,
    "collection_delete",
    list(collection_name, user),
    schema = schema
  ))

  if (db_return$status == "warning") {
    warning(db_return$message)
  }

  db_return
}

#' list all collection
#'
#' @inheritParams param_defs
#' @family collections functions
#'
#' @importFrom jsonlite fromJSON
#' @export
#'
#' @examples
#'
#' \dontrun{
#' ts1 <- list(ts(rnorm(100), start = c(1990, 1), frequency = 4))
#' names(ts1) <- c("ts1")
#' store_time_series(con = connection, ts1, schema = "schema")
#' store_time_series(con = connection, zrh_airport, schema = "schema")
#' store_time_series(con = connection, kof_ts, schema = "schema")
#'
#' db_ts_add_to_collection(
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
#' db_ts_add_to_collection(
#'   con = connection,
#'   collection_name = "ts1 and departures zurich",
#'   ts_keys = c(
#'     "ch.zrh_airport.departure.total",
#'     "ts1"
#'   ),
#'   schema = "schema"
#' )
#'
#' db_collection_list(
#'   con = connection,
#'   schema = "schema"
#' )
#' }
db_collection_list <- function(con,
                               user = Sys.info()["user"],
                               schema = "timeseries") {
  db_call_function(
    con,
    "list_collections",
    list(
      user
    ),
    schema = schema
  )
}

