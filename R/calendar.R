#' Create an Entry in the Release Calendar
#'
#' The idea of the release calendar is to set a release date for some time series that might be in the database already but should not be publicly available before a specific date, e.g., a press release. Since publishing is simply a matter of changing the access level, an update of the access levels could be triggered based on the release information in a release table. Only timeseries admins may create and modify releases.
#'
#' @param id Identifier for the release e.g. 'gdb_may_2020'
#' @param title Display title for the release
#' @param release_date Timestamp when the release is to occur
#' @param target_year Year observed in the data
#' @param target_period Period observed in the data (e.g. month, quarter)
#' @param target_frequency Frequency of the data (e.g. 4 for quarterly)
#' @param note Additional remarks about the release.
#'
#' @details
#' \code{target_period} changes meaning depending on the frequency of the release.
#' e.g. period 2 for quarterly data (reference_frequency = 4) means Q2 whereas
#' period 2 for monthly data (frequency 12) means February
#' In other words: \code{target_year} and \code{target_period} mark the end of the time series
#' in the release.
#'
#' @return a status list
#'
#' @inheritParams param_defs
#' @family calendar functions
#'
#' @import data.table
#' @importFrom RPostgres dbWriteTable
#' @importFrom jsonlite fromJSON
#' @export
db_release_create <- function(con,
                           id,
                           title,
                           release_date,
                           datasets,
                           target_year = year(release_date),
                           target_period = month(release_date),
                           target_frequency = 12,
                           note = NULL,
                           schema = "timeseries") {
  out <- db_with_temp_table(con,
                            "tmp_release_insert",
                            data.table(
                              set_id = datasets
                            ),
                            field.types = c(
                              set_id = "text"
                            ),
                            {
                              tryCatch(
                                db_call_function(con,
                                                 "release_create",
                                                 list(
                                                   id,
                                                   title,
                                                   note,
                                                   release_date,
                                                   target_year,
                                                   target_period,
                                                   target_frequency
                                                 ),
                                                 schema = schema
                                ),
                                error = function(e) {
                                  if(grepl("unique constraint \"release_calendar_pkey\"", e)) {
                                    stop("A release with that ID already exists. To update it use update_release.")
                                  } else if(grepl("permission denied for function create_release", e)) {
                                    stop("Only timeseries admin may create new releases.")
                                  } else {
                                    stop(e)
                                  }
                                })
                            },
                            schema = schema)

  parsed <- fromJSON(out)
  if(parsed$status != "ok") {
    if("missing_datasets" %in% names(parsed)) {
      stop(sprintf("Some datasets do not exist: %s", paste(parsed$missing_datasets, collapse = ", ")))
    }
  }

  parsed
}

#' Update an Existing Release Record
#'
#' Any parameters provided to this function will overwrite the corresponding
#' fields in the database. Parameters set to NA (default) will leave the
#' corresponding fields untouched.
#' For details see \code{\link{db_release_create}}.
#'
#' @param id Identifier for the release e.g. 'gdb_may_2020'
#' @param title Display title for the release
#' @param release_date Timestamp when the release is to occur
#' @param target_year Year observed in the data
#' @param target_period Period observed in the data (e.g. month, quarter)
#' @param target_frequency Frequency of the data (e.g. 4 for quarterly)
#' @param note Additional remarks about the release.
#'
#' @inheritParams param_defs
#' @family calendar functions
#'
#' @return a status list
#' @export
db_release_update <- function(con,
                              id,
                              title = NULL,
                              release_date = NULL,
                              datasets = NULL,
                              target_year = NULL,
                              target_period = NULL,
                              target_frequency = NULL,
                              note = NULL,
                              schema = "timeseries") {
  # so data.table contains something
  if(is.null(datasets)) {
    datasets <- NA
  }

  out <- db_with_temp_table(con,
                            "tmp_release_update",
                            data.table(
                              set_id = datasets
                            ),
                            field.types = c(
                              set_id = "text"
                            ),
                            {
                              tryCatch(
                                db_call_function(con,
                                                 "release_update",
                                                 list(
                                                   id,
                                                   title,
                                                   note,
                                                   release_date,
                                                   target_year,
                                                   target_period,
                                                   target_frequency,
                                                   !all(is.na(datasets))
                                                 ),
                                                 schema = schema
                                ),
                                error = function(e) {
                                  if(grepl("permission denied for function update_release", e)) {
                                    stop("Only timeseries admin may update releases.")
                                  } else {
                                    stop(e)
                                  }
                                })
                            },
                            schema = schema)

  parsed <- fromJSON(out)
  if(parsed$status != "ok") {
    if("missing_datasets" %in% names(parsed)) {
      stop(sprintf("Some datasets do not exist: %s", paste(parsed$missing_datasets, collapse = ", ")))
    } else {
      stop(parsed$message)
    }
  }

  parsed
}

#' Cancel a Scheduled Release
#'
#' Attempts to cancel a release that has already passed will result in an error.
#'
#' @param release_id character ID of the release to cancel
#'
#' @inheritParams param_defs
#' @family calendar functions
#'
#' @export
#' @importFrom jsonlite fromJSON
db_release_cancel <- function(con,
                              release_id,
                              schema = "timeseries") {
  # TODO: We shoulda try-caught all failures in db_call_function
  out <- fromJSON(db_call_function(con,
                                   "release_cancel",
                                   list(
                                     release_id
                                   ),
                                   schema = schema))

  if(out$status == "error") {
    stop(out$message)
  }

  out
}

#' List Data on Registered Releases
#'
#' @param include_past Should past releases be included? Defaults to FALSE
#'
#' @inheritParams param_defs
#' @family calendar functions
#'
#' @return data.frame with columns `id`, `title`, `note`, `release_date`, `reference_year`, `reference_period`, `reference_frequency`
#' @export
db_release_list <- function(con,
                             include_past = FALSE,
                             schema = "timeseries") {
  db_call_function(con,
                   "release_list",
                   list(
                     include_past
                   ),
                   schema = schema)
}

#' Get Next Release Date for Given Datasets
#'
#' @param set_ids Sets to get release dates for
#'
#' @inheritParams param_defs
#' @family calendar functions
#'
#' @return data.frame with columns `set_id`, `release_id`, `release_date`
#'
#' @export
db_dataset_get_next_release <- function(con,
                                        set_ids,
                                        schema = "timeseries") {
  db_with_temp_table(con,
                     "tmp_get_release",
                     data.table(
                       set_id = set_ids
                     ),
                     field.types = c(
                       set_id = "text"
                     ),
                     {
                       db_call_function(con,
                                        "release_get_next",
                                        schema = schema)
                     },
                     schema = schema)
}

#' Get the latest Release for Given Datasets
#'
#' @param set_ids Sets to get release dates for
#'
#' @inheritParams param_defs
#' @family calendar functions
#'
#' @return data.frame with columns `set_id`, `release_id`, `release_date`
#' @export
db_dataset_get_latest_release <- function(con,
                                          set_ids,
                                          schema = "timeseries") {
  db_with_temp_table(con,
                     "tmp_get_release",
                     data.table(
                       set_id = set_ids
                     ),
                     field.types = c(
                       set_id = "text"
                     ),
                     {
                       db_call_function(con,
                                        "release_get_latest",
                                        schema = schema)
                     },
                     schema = schema)
}


#' Get the latest Release for Given Datasets
#'
#' @param set_ids Sets to get release dates for
#' @param target_year Year of the desired release
#' @param target_period Period of the desired release
#'
#' @inheritParams param_defs
#' @family calendar functions
#'
#' @import data.table
#' @return data.frame with columns `set_id`, `release_id`, `release_date`
#' @export
db_dataset_get_release <- function(con,
                                      set_ids,
                                      target_year = year(Sys.Date()),
                                      target_period = month(Sys.Date()),
                                      schema = "timeseries") {
  db_with_temp_table(con,
                     "tmp_get_release",
                     data.table(
                       set_id = set_ids
                     ),
                     field.types = c(
                       set_id = "text"
                     ),
                     {
                       db_call_function(con,
                                        "release_get",
                                        list(
                                          target_year,
                                          target_period
                                        ),
                                        schema = schema)
                     },
                     schema = schema)
}
