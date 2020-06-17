#' Create an Entry in the Release Calendar
#'
#' Only timeseries admins may create and modify releases
#'
#' @param con RPostgres connection
#' @param id Identifier for the release e.g. 'gdb_may_2020'
#' @param title Display title for the release
#' @param release_date Timestamp when the release is to occur
#' @param reference_year Year observed in the data
#' @param reference_period Period observed in the data (e.g. month, quarter)
#' @param reference_frequency Frequency of the data (e.g. 4 for quarterly)
#' @param note Additional remarks about the release.
#' @param schema timeseries schema name
#'
#' @details
#' reference_period changes meaning depending on the frequency of the release.
#' e.g. period 2 for quarterly data (reference_frequency = 4) means Q2 whereas
#' period 2 for monthly data (frequency 12) means February
#'
#' @return a status list
#'
#' @import data.table
#' @importFrom RPostgres dbWriteTable
#' @importFrom jsonlite fromJSON
#' @export
db_create_release <- function(con,
                           id,
                           title,
                           release_date,
                           datasets,
                           reference_year = year(release_date),
                           reference_period = month(release_date),
                           reference_frequency = 12,
                           note = NA,
                           schema = "timeseries") {
  dbWriteTable(con,
               "tmp_release_insert",
               data.table(
                 set_id = datasets
               ),
               temporary = TRUE,
               overwrite = TRUE,
               field.types = c(
                 set_id = "text")
  )

  out <- tryCatch(
    db_call_function(con,
                     "create_release",
                     list(
                       id,
                       title,
                       note,
                       release_date,
                       reference_year,
                       reference_period,
                       reference_frequency
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
#' For details see db_create_release
#'
#' @param con RPostgres connection
#' @param id Identifier for the release e.g. 'gdb_may_2020'
#' @param title Display title for the release
#' @param release_date Timestamp when the release is to occur
#' @param reference_year Year observed in the data
#' @param reference_period Period observed in the data (e.g. month, quarter)
#' @param reference_frequency Frequency of the data (e.g. 4 for quarterly)
#' @param note Additional remarks about the release.
#' @param schema timeseries schema name
#'
#' @return a status list
#' @export
db_update_release <- function(con,
                              id,
                              title = NA,
                              release_date = NA,
                              datasets = NA,
                              reference_year = NA,
                              reference_period = NA,
                              reference_frequency = NA,
                              note = NA,
                              schema = "timeseries") {

  if(!is.na(datasets)) {
    dbWriteTable(con,
                 "tmp_release_update",
                 data.table(
                   set_id = datasets
                 ),
                 temporary = TRUE,
                 overwrite = TRUE,
                 field.types = c(
                   set_id = "text"))
  }

  out <- tryCatch(
    db_call_function(con,
                     "update_release",
                     list(
                       id,
                       title,
                       note,
                       release_date,
                       reference_year,
                       reference_period,
                       reference_frequency,
                       !is.na(datasets)
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

  parsed <- fromJSON(out)
  if(parsed$status != "ok") {
    if("missing_datasets" %in% names(parsed)) {
      stop(sprintf("Some datasets do not exist: %s", paste(parsed$missing_datasets, collapse = ", ")))
    }
  }

  parsed
}

#' List Data on Registered Releases
#'
#' @param con RPostgres connection
#' @param include_past Should past releases be included? Defaults to FALSE
#' @param schema Timeseries schema name
#'
#' @return data.frame with columns `id`, `title`, `note`, `release_date`, `reference_year`, `reference_period`, `reference_frequency`
#' @export
db_list_releases <- function(con,
                             include_past = FALSE,
                             schema = "timeseries") {
  db_call_function(con,
                   "list_releases",
                   list(
                     include_past
                   ),
                   schema = schema)
}

#' Get Next Release Date for Given Datasets
#'
#' @param con RPostgres connection
#' @param set_ids Sets to get release dates for
#' @param schema Timeseries schema name
#'
#' @return data.frame with columns `set_id`, `release_id`, `release_date`
#' @export
db_get_next_release_for_set <- function(con,
                                        set_ids,
                                        schema = "timeseries") {
  dbWriteTable(con,
               "tmp_get_release",
               data.table(
                 set_id = set_ids
               ),
               temporary = TRUE,
               overwrite = TRUE,
               field.types = c(
                 set_id = "text"
               ))

  db_grant_to_admin(con, "tmp_get_release", schema)

  db_call_function(con,
                   "get_next_release_for_sets",
                   schema = schema)
}
