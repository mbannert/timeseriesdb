#' Create a New Dataset
#'
#' A dataset is a family of time series that belong to the same topic. By default all series stored with `db_store_ts` belong to a default set. In order to assign them a different set, it must first be created with `db_create_dataset` after which the series may be moved with `tbd`
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
#'
#' @return character name of the created set
#' @export
db_create_dataset <- function(con,
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

  # TODO: use JSON return
  tryCatch(
    db_call_function(con,
                   "create_dataset",
                   list(
                     set_name,
                     set_description,
                     set_md
                   ),
                   schema),
    error = function(e) {
      if(grepl("violates unique constraint \"datasets_pkey\"", e)) {
        stop("A dataset by that name already exists.")
      } else {
        stop(e)
      }
    })
}


#' Get All Time Series Keys in a Given Set
#'
#' @param con RPostgres connection object
#' @param set_name character Name of the set to get keys for
#' @param schema character Name of timeseries schema
#'
#' @return character A vector of ts keys contained in the set
#' @export
db_get_dataset_keys <- function(con,
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
db_get_dataset_id <- function(con,
                               ts_keys,
                               schema = "timeseries") {
  dbWriteTable(con,
               "tmp_get_set",
               data.frame(ts_key = ts_keys),
               temporary = TRUE,
               overwrite = TRUE,
               field.types = c(
                 ts_key = "text"
               ))

  db_grant_to_admin(con, "tmp_get_set", schema)

  db_call_function(con,
                   "get_set_of_keys",
                   schema = schema)
}

#' Assign Time Series Identifiers to a Dataset
#'
#' `db_assign_dataset` returns a list with status information.
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
db_assign_dataset <- function(con,
                              ts_keys,
                              set_name,
                              schema = "timeseries") {

  dbWriteTable(con,
               "tmp_set_assign",
               data.frame(ts_key = ts_keys),
               temporary = TRUE,
               overwrite = TRUE,
               field.types = c(
                 ts_key = "text"
               ))

  db_grant_to_admin(con, "tmp_set_assign", schema)

  # Error case: Set does not exist
  # Warning case: Only some keys found in catalog
  # Success case: you know what that means...
  out <- db_call_function(con,
                           "assign_dataset",
                           list(
                             set_name
                           ),
                           schema)

  out_parsed <- jsonlite::fromJSON(out)

  if(out_parsed$status == "failure") {
    stop(out_parsed$reason)
  } else if(out_parsed$status == "warning") {
    warning(sprintf("%s\n%s", out_parsed$reason, paste(out_parsed$offending_keys, collapse = ",\n")))
  }

  # Why not both (well, one and a half)?
  out_parsed
}

#' Get All available datasets and their description
#'
#' @param con RPostgres connection object
#' @param schema character Name of timeseries schema
#'
#' @return data.frame with columns `set_id` and `set_description`
#' @export
db_list_datasets <- function(con,
                                 schema = "timeseries") {

  db_call_function(con,
                   "list_datasets",
                   schema = schema)
}


