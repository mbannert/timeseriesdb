#' Create a New Dataset
#'
#' A dataset is a family of time series that belong to the same topic. By default all series stored with `db_store_ts` belong to a default set. In order to assign them a different set, it must first be created with `db_dataset_create` after which the series may be moved with `tbd`
#'
#' For arbitrary collections of time series see [how do you reference a doc topic?]
#'
#' @param con RPostgres connection object
#' @param set_name character The name of the set to be created
#' @param set_md meta information data about the set.
#' @param schema character Name of timeseries schema
#'
#' @importFrom RPostgres dbGetQuery dbQuoteIdentifier
#' @importFrom DBI Id
#' @importFrom jsonlite fromJSON
#'
#' @return character name of the created set
#' @export
db_dataset_create <- function(con,
                              set_name,
                              set_description = NA,
                              set_md = NA,
                              schema = "timeseries") {
  # TODO: catch as.metas error and throw a more informative one?
  set_md <- as.meta(set_md)

  # we want to keep NAs as pure NAs, not JSON nulls that would override the DEFAULT
  set_md <- ifelse(is.na(set_md),
                   set_md,
                   jsonlite::toJSON(set_md, auto_unbox = TRUE, null = "null"))


  out <- db_call_function(con,
                         "create_dataset",
                         list(
                           set_name,
                           set_description,
                           set_md
                         ),
                         schema)

  out_parsed <- fromJSON(out)

  if(out_parsed$status == "error") {
    stop(out_parsed$message)
  }

  out_parsed
}


#' Get All Time Series Keys in a Given Set
#'
#' @param con RPostgres connection object
#' @param set_name character Name of the set to get keys for
#' @param schema character Name of timeseries schema
#'
#' @return character A vector of ts keys contained in the set
#' @export
db_dataset_get_keys <- function(con,
                               set_name = 'default',
                               schema = "timeseries") {
  db_call_function(con,
                   "keys_in_dataset",
                   list(
                     set_name
                   ),
                   schema)$ts_key
}

#' Find Datasets Given a Set
#'
#' Return set identifiers associated with a vector of keys. If a ts key does not exist in the catalog, set_id will be NA.
#'
#' @param con PostgreSQL connection
#' @param ts_keys character
#' @param schema character Name of timeseries schema
#'
#' @return data.frame with columns `ts_key` and `set_id`
#' @export
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
                                      "get_set_of_keys",
                                      schema = schema),
                     schema = schema)
}

#' Assign Time Series Identifiers to a Dataset
#'
#' `db_ts_assign_dataset` returns a list with status information.
#' status `"ok"` means all went well.
#' status `"warning"` means some keys are not in the catalog. The vector of
#' those keys is in the `offending_keys` field.
#'
#' Trying to assign keys to a nonexistent dataset is an error.
#'
#' @param con RPostgres connection object
#' @param ts_keys character Vector of ts keys to assign to dataset
#' @param set_name character Id of the set to assign `ts_keys` to
#' @param schema character Name of timeseries schema
#'
#' @return list A status list
#' @export
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
                           db_call_function(con,
                            "assign_dataset",
                            list(
                              set_name
                            ),
                            schema),
                           schema = schema)

  out_parsed <- jsonlite::fromJSON(out)

  if(out_parsed$status == "error") {
    stop(out_parsed$reason)
  } else if(out_parsed$status == "warning") {
    warning(sprintf("%s\n%s", out_parsed$reason, paste(out_parsed$offending_keys, collapse = ",\n")))
  }

  # Why not both (well, one and a half)?
  out_parsed
}

#' Update Description and/or Metadata of a Dataset
#'
#' @param con RPostgres connection object
#' @param set_name character Name of the set do update
#' @param description character New description. If set to NA (default) the description is left untouched
#' @param metadata list Metadata update (see metadata_update_mode)
#' @param metadata_update_mode character One of "update" or "overwrite". If set to "update",
#'  new fields in the list are added to the existing metadata and existing fields overwritten.
#'  If NA nothing happens in update mode. If set to "overwrite" ALL existing metadata is replaced.
#' @param schema Timeseries Schema name
#'
#' @importFrom jsonlite toJSON fromJSON
#' @export
db_dataset_update <- function(con,
                              set_name,
                              description = NA,
                              metadata = NA,
                              metadata_update_mode = "update",
                              schema = "timeseries") {
  if(!is.na(metadata)) {
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
                          schema = schema)

  out_parsed <- fromJSON(out)

  if(out_parsed$status == "error") {
    stop(out_parsed$message)
  }

  out_parsed
}

#' Get All available datasets and their description
#'
#' @param con RPostgres connection object
#' @param schema character Name of timeseries schema
#'
#' @return data.frame with columns `set_id` and `set_description`
#' @export
db_dataset_list <- function(con,
                                 schema = "timeseries") {

  db_call_function(con,
                   "list_datasets",
                   schema = schema)
}


#' Irrevocably delete all time series in a set and the set itself
#'
#' This function can only be used manually.
#' It asks the user to manually input confirmation to prevent accidental
#' unintentional deletion of datasets.
#'
#' @param con PostgreSQL connection
#' @param set_name character Name of the set to delete
#' @param schema character Name of timeseries schema
#'
#' @return character name of the deleted set, NA in case of an error.
#' @export
#'
db_dataset_delete <- function(con,
                              set_name,
                              schema = "timeseries") {
  message("This will permanently delete ALL time series associated with that set,\n**including their histories**.")
  confirmation <- readline("Retype dataset name to confirm: ")

  if(confirmation != set_name) {
    stop("Confirmation failed!")
  }

  out <- fromJSON(db_call_function(con,
                                   "dataset_delete",
                                   list(
                                     set_name,
                                     confirmation
                                   ),
                                   schema = schema))

  if(out$status == "warning") {
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
#' function can be used to trim off old vintages that are no longer relevant.
#'
#' @param con RPostgres connection object
#' @param set_id character Name of the set to trim
#' @param older_than Date cut off point
#' @param schema character Time series schema name
#'
#' @export
#' @importFrom jsonlite fromJSON
db_dataset_trim_history <- function(con,
                            set_id,
                            older_than,
                            schema = "timeseries") {
  fromJSON(db_call_function(con,
                            "dataset_trim",
                            list(
                              set_id,
                              older_than
                            ),
                            schema = schema))
}
