#' Create a New Dataset
#'
#' A dataset is a family of time series that belong to the same topic. By default all series stored with `db_store_ts` belong to a default set. In order to assign them a different set, it must first be created with `db_dataset_create` after which the series may be moved with \code{\link{db_ts_assign_dataset}}.
#'
#' @param set_description \strong{character} description about the set. Default to NA.
#' @param set_md meta information data about the set. Default to NA.
#'
#' @inheritParams param_defs
#' @family datasets functions
#'
#' @importFrom RPostgres dbGetQuery dbQuoteIdentifier
#' @importFrom DBI Id
#' @importFrom jsonlite fromJSON
#'
#' @return character name of the created set
#' @export
#'
#' @examples
#'
#' \dontrun{
#'
#' db_dataset_create(
#'   con = connection,
#'   set_name = "zrh_airport_data",
#'   set_description = "Zurich airport arrivals and departures ",
#'   schema = "schema"
#' )
#' }
db_dataset_create <- function(con,
                              set_name,
                              set_description = NULL,
                              set_md = NULL,
                              schema = "timeseries") {
  set_md <- as.meta(set_md)

  # we want to keep NAs as pure NAs, not JSON nulls that would override the DEFAULT
  set_md <- ifelse(is.null(set_md),
                   NA,
                   toJSON(set_md, auto_unbox = TRUE, null = "null"))


  out <- db_call_function(
    con,
    "dataset_create",
    list(
      set_name,
      set_description,
      set_md
    ),
    schema
  )

  out_parsed <- fromJSON(out)

  if (out_parsed$status == "error") {
    stop(out_parsed$message)
  }

  out_parsed
}


#' Get All Time Series Keys in a Given Set
#'
#'
#' @inheritParams param_defs
#' @family datasets functions
#'
#' @return character A vector of ts keys contained in the set
#' @export
#'
#' @examples
#'
#' \dontrun{
#'
#' db_dataset_get_keys(
#'   con = connection,
#'   set_name = "zrh_airport_data",
#'   set_description = "Zurich airport arrivals and departures ",
#'   schema = "schema"
#' )
#' }
db_dataset_get_keys <- function(con,
                                set_name = "default",
                                schema = "timeseries") {
  db_call_function(
    con,
    "dataset_get_keys",
    list(
      set_name
    ),
    schema
  )$ts_key
}

#' Find Datasets Given a Set
#'
#' Return set identifiers associated with a vector of keys. If a ts key does not exist in the catalog, set_id will be NA.
#'
#' @inheritParams param_defs
#' @family datasets functions
#'
#' @return data.frame with columns `ts_key` and `set_id`
#' @export
#'
#' @examples
#'
#' \dontrun{
#'
#' # one key
#' db_ts_get_dataset(
#'   con = connection,
#'   ts_keys = "ch.zrh_airport.departure.total",
#'   schema = "schema"
#' )
#'
#' # multiple keys
#' db_ts_get_dataset(
#'   con = connection,
#'   ts_keys = c(
#'     "ch.zrh_airport.departure.total",
#'     "ch.zrh_airport.arrival.total"
#'   ),
#'   schema = "schema"
#' )
#' }
db_ts_get_dataset <- function(con,
                              ts_keys,
                              schema = "timeseries") {
  db_with_temp_table(con,
    "tmp_get_set",
    data.frame(ts_key = ts_keys),
    field.types = c(
      ts_key = "text"
    ),
    db_call_function(con,
      "keys_get_dataset",
      schema = schema
    ),
    schema = schema
  )
}

#' Assign Time Series Identifiers to a Dataset
#'
#' `db_ts_assign_dataset` returns a list with status information.
#' status `"ok"` means all went well.
#' status `"warning"` means some keys are not in the catalog. The vector of
#' those keys is in the `offending_keys` field.
#'
#' Trying to assign keys to a non-existent dataset is an error.
#'
#' @inheritParams param_defs
#' @family datasets functions
#'
#' @return list A status list
#' @export
#'
#' @examples
#'
#' \dontrun{
#'
#' db_dataset_create(
#'   con = connection,
#'   set_name = "zrh_airport_data",
#'   set_description = "Zurich airport arrivals and departures ",
#'   schema = "schema"
#' )
#'
#' db_ts_assign_dataset(
#'   con = connection,
#'   ts_keys = c(
#'     "ch.zrh_airport.departure.total",
#'     "ch.zrh_airport.arrival.total"
#'   ),
#'   set_name = "zrh_airport_data",
#'   schema = "schema"
#' )
#' }
db_ts_assign_dataset <- function(con,
                                 ts_keys,
                                 set_name,
                                 schema = "timeseries") {

  # Error case: Set does not exist
  # Warning case: Only some keys found in catalog
  # Success case: you know what that means...

  out <- db_with_temp_table(con,
    "tmp_set_assign",
    data.frame(ts_key = ts_keys),
    field.types = c(ts_key = "text"),
    db_call_function(
      con,
      "dataset_add_keys",
      list(
        set_name
      ),
      schema
    ),
    schema = schema
  )

  out_parsed <- jsonlite::fromJSON(out)

  if (out_parsed$status == "error") {
    stop(out_parsed$reason)
  } else if (out_parsed$status == "warning") {
    warning(sprintf("%s\n%s", out_parsed$reason, paste(out_parsed$offending_keys, collapse = ",\n")))
  }

  # Why not both (well, one and a half)?
  out_parsed
}

#' Update Description and/or Metadata of a Dataset
#'
#' @param description character New description. If set to NA (default) the description is left untouched
#' @param metadata \strong{list} Metadata update (see metadata_update_mode)
#' @param metadata_update_mode character one of "update" or "overwrite". If set to "update",
#'  new fields in the list are added to the existing metadata and existing fields overwritten.
#'  If NA nothing happens in update mode. If set to "overwrite" ALL existing metadata is replaced.
#'
#' @inheritParams param_defs
#' @family datasets functions
#'
#' @importFrom jsonlite toJSON fromJSON
#' @export
#'
#' @examples
#'
#' \dontrun{
#'
#' db_dataset_update_metadata(
#'   con = connection,
#'   set_name = "zrh_airport_data",
#'   description = "updating description Zurich airport arrivals and departures",
#'   schema = "schema"
#' )
#' }
db_dataset_update_metadata <- function(con,
                              set_name,
                              description = NULL,
                              metadata = NULL,
                              metadata_update_mode = "update",
                              schema = "timeseries") {

  if(!is.null(metadata)) {
    metadata <- toJSON(metadata, auto_unbox = TRUE, digits = NA)
  }

  out <- db_call_function(con,
    "dataset_update",
    list(
      set_name,
      description,
      metadata,
      metadata_update_mode
    ),
    schema = schema
  )

  out_parsed <- fromJSON(out)

  if (out_parsed$status == "error") {
    stop(out_parsed$message)
  }

  out_parsed
}

#' Get All Available Datasets and Their Description
#'
#' @inheritParams param_defs
#' @family datasets functions
#'
#' @return data.frame with columns `set_id` and `set_description`
#' @export
#'
#' @examples
#'
#' \dontrun{
#'
#' db_dataset_create(
#'   con = connection,
#'   set_name = "zrh_airport_data",
#'   set_description = "Zurich airport arrivals and departures ",
#'   schema = "schema"
#' )
#'
#' db_dataset_list(
#'   con = connection,
#'   schema = "schema"
#' )
#' }
db_dataset_list <- function(con,
                            schema = "timeseries") {
  db_call_function(con,
    "dataset_list",
    schema = schema
  )
}


#' Irrevocably Delete All Time Series in a Set and the Set Itself
#'
#' This function cannot be used in batch mode as it needs user interaction.
#' It asks the user to manually input confirmation to prevent
#' unintentional deletion of datasets.
#'
#' @inheritParams param_defs
#' @family datasets functions
#'
#' @return character name of the deleted set, NA in case of an error.
#' @export
#'
#' @examples
#'
#' \dontrun{
#'
#' db_dataset_create(
#'   con = connection,
#'   set_name = "zrh_airport_data",
#'   set_description = "Zurich airport arrivals and departures ",
#'   schema = "schema"
#' )
#'
#' db_dataset_delete(
#'   con = connection,
#'   set_name = "zrh_airport_data",
#'   schema = "schema"
#' )
#' }
db_dataset_delete <- function(con,
                              set_name,
                              schema = "timeseries") {
  message("This will permanently delete ALL time series associated with that set,\n**including their histories**.")
  confirmation <- readline("Retype dataset name to confirm: ")

  if (confirmation != set_name) {
    stop("Confirmation failed!")
  }

  out <- fromJSON(db_call_function(con,
    "dataset_delete",
    list(
      set_name,
      confirmation
    ),
    schema = schema
  ))

  if (out$status == "warning") {
    warning(out$reason)
  } else if (out$status == "error") {
    stop(out$message)
  }

  out
}

#' Remove Vintages from the Beginning of Dataset
#'
#' Removes any vintages of the given dataset that are older than a specified date.
#'
#' In some cases only the last few versions of time series are of interest. This
#' function can be used to trim off old vintages that are no longer relevant. It may
#' be helpful to use this function with high frequency data to save disk space
#' of versions are not needed.
#'
#' @param set_id character Name of the set to trim
#' @param older_than Date cut off point
#'
#' @inheritParams param_defs
#' @family datasets functions
#'
#' @export
#' @importFrom jsonlite fromJSON
#'
#' @examples
#'
#' \dontrun{
#'
#' # Storing different versions of the data, use parameter valid_from
#' # different versions are stored with the same key
#' ch.kof.barometer <- kof_ts["baro_2019m11"]
#' names(ch.kof.barometer) <- c("ch.kof.barometer")
#' db_ts_store(
#'   con = connection,
#'   ch.kof.barometer,
#'   valid_from = "2019-12-01",
#'   schema = "schema"
#' )
#'
#' ch.kof.barometer <- kof_ts["baro_2019m12"]
#' names(ch.kof.barometer) <- c("ch.kof.barometer")
#' db_ts_store(
#'   con = connection,
#'   ch.kof.barometer,
#'   valid_from = "2020-01-01",
#'   schema = "schema"
#' )
#'
#' db_dataset_create(
#'   con = connection,
#'   set_name = "barometer",
#'   set_description = "KOF Barometer",
#'   schema = "schema"
#' )
#'
#' db_ts_assign_dataset(
#'   con = connection,
#'   ts_keys = "ch.kof.barometer",
#'   set_name = "barometer",
#'   schema = "schema"
#' )
#'
#' db_dataset_trim_history(
#'   con = connection,
#'   set_id = "barometer",
#'   older_than = "2019-12-31",
#'   schema = "schema"
#' )
#' }
db_dataset_trim_history <- function(con,
                                    set_id,
                                    older_than,
                                    schema = "timeseries") {
  fromJSON(db_call_function(con,
    "dataset_trim_history",
    list(
      set_id,
      older_than
    ),
    schema = schema
  ))
}

#' Get the dataset last update
#'
#' @param set_id \strong{character} name of the set to get the last update
#'
#' @inheritParams param_defs
#' @family datasets functions
#'
#' @export
#'
#' @examples
#'
#' \dontrun{
#'
#' # Storing different versions of the data, use parameter valid_from
#' # different versions are stored with the same key
#' ch.kof.barometer <- kof_ts["baro_2019m11"]
#' names(ch.kof.barometer) <- c("ch.kof.barometer")
#' db_ts_store(
#'   con = connection,
#'   ch.kof.barometer,
#'   valid_from = "2019-12-01",
#'   schema = "schema"
#' )
#'
#' ch.kof.barometer <- kof_ts["baro_2019m12"]
#' names(ch.kof.barometer) <- c("ch.kof.barometer")
#' db_ts_store(
#'   con = connection,
#'   ch.kof.barometer,
#'   valid_from = "2020-01-01",
#'   schema = "schema"
#' )
#'
#' db_dataset_create(
#'   con = connection,
#'   set_name = "barometer",
#'   set_description = "KOF Barometer",
#'   schema = "schema"
#' )
#'
#' db_ts_assign_dataset(
#'   con = connection,
#'   ts_keys = "ch.kof.barometer",
#'   set_name = "barometer",
#'   schema = "schema"
#' )
#'
#' db_dataset_get_last_update(
#'   con = connection,
#'   set_id = "barometer",
#'   schema = "schema"
#' )
#' }
db_dataset_get_last_update <- function(con,
                                       set_id,
                                       schema = "timeseries") {
  db_call_function(con,
                  "dataset_get_last_update",
                  list(
                    set_id
                  ),
                  schema = schema)
}
