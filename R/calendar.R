#' Title
#'
#' @param con
#' @param id
#' @param title
#' @param release_date
#' @param reference_year
#' @param reference_period
#' @param reference_frequency
#' @param note
#' @param schema
#'
#' @return
#' @export
#'
#' @import data.table
#' @importFrom RPostgres dbWriteTable
#' @importFrom jsonlite fromJSON
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

  fromJSON(out)
}

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

  fromJSON(out)
}
