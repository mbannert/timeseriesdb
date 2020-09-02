#' Bundles Keys into an Existing Collection or Adds a New Collection
#'
#' Collections are user specific compilations of time series keys. Similar to
#' a playlist in a music app, collections help to come back to a previously stored
#' selection of time series. This functions adds more time series to existing bundles (collections).
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
#' }
db_collection_add_ts <- function(con,
                              collection_name,
                              ts_keys,
                              description = NULL,
                              user = Sys.info()['user'],
                              schema = "timeseries"){
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
      "collection_insert",
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
#' Removes a vector of time series keys from a user specific
#' compilation.
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
#' db_collection_remove_ts(
#'   con = connection,
#'   collection_name = "barometer and departures zurich",
#'   ts_keys = "ch.zrh_airport.departure.total",
#'   schema = "schema"
#' )
#' }
db_collection_remove_ts <- function(con,
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
      "collection_remove_keys",
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


#' Get All Keys in a User Collection
#'
#' Reads all keys in the given collection and returns them in a vector
#'
#' @inheritParams param_defs
#' @family collections functions
#'
#' @export
db_collection_get_keys <- function(con,
                                   collection_name,
                                   user = Sys.info()["user"],
                                   schema = "timeseriesdb") {
  db_call_function(con,
                   "collection_get_keys",
                   list(
                     collection_name,
                     user
                   ),
                   schema = schema)$ts_key
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

#' List All Available Collections for a Specific User
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
#' db_ts_store(con = connection, ts1, schema = "schema")
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
#' db_collection_add_ts(
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
    "collection_list",
    list(
      user
    ),
    schema = schema
  )
}

#' Get the last update of a collection for a specific User
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
#' db_collection_get_last_update(
#'   con = connection,
#'   collection_name = "barometer and departures zurich",
#'   schema = "schema"
#' )
#' }
db_collection_get_last_update <- function(con,
                                          collection_name,
                                          user = Sys.info()["user"],
                                          schema = "timeseries") {
  db_call_function(con,
                   "collection_get_last_update",
                   list(
                     collection_name,
                     user
                   ),
                   schema = schema)
}
