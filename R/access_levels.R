#' Change the Access Level for Time Series
#'
#' @param con RPostgres connection
#' @param ts_keys Time Series Keys for which to change access
#' @param new_access_level Access level to set to
#' @param validity If provided only change the access level for vintages with the given validity.
#'                 By default the access level for all vintages is updated.
#' @param schema Time Series schema name
#'
#' @export
#' @importFrom jsonlite fromJSON
db_change_access_level <- function(con,
                                   ts_keys,
                                   new_access_level,
                                   validity = NA,
                                   schema = "timeseries") {
  out <- db_with_temp_table(con,
                            "tmp_ts_access_keys",
                            data.frame(
                              ts_key = ts_keys
                            ),
                            field.types = c(
                              ts_key = "text"
                            ),
                            {
                              db_call_function(con,
                                               "change_access_level",
                                               list(
                                                 new_access_level,
                                                 validity
                                               ),
                                               schema = schema)
                            },
                            schema = schema)
  parsed <- fromJSON(out)

  if(parsed$status == "error") {
    stop(parsed$message)
  }

  parsed
}

#' Get All access levels and their description
#'
#' @param con RPostgres connection object
#' @param schema character Name of timeseries schema
#'
#' @return data.frame with columns `role` and `description` and `is_default`
#' @export
db_list_access_levels <- function(con,
                                  schema = "timeseries") {

  db_call_function(con,
                   "list_access_levels",
                   schema = schema)
}


#' Delete a role in access levels table
#'
#' @param con RPostgres connection object
#' @param del_role name of the role to remove
#' @param schema character Name of timeseries schema
#'
#' @return data.frame with columns `role` and `description` and `is_default`
#' @export
db_delete_role <- function(con,
                           del_role,
                           schema = "timeseries") {

  out <- db_call_function(con,
                   "access_levels_delete",
                   list(
                     del_role
                   ),
                   schema = schema)

  out_parsed <- jsonlite::fromJSON(out)

  if(out_parsed$status == "warning") {
    warning(del_role,
            " ",
            out_parsed$message)
  } else if(out_parsed$status == "error") {
    stop(del_role,
         " ",
         out_parsed$reason)
  } else if(out_parsed$status == "ok") {
    cat(del_role,
        out_parsed$message)
  }
}
