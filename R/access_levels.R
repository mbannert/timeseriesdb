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
#' @param schema character Name of timeseries schema
#'
#' @return data.frame with columns `role` and `description` and `is_default`
#' @export
db_delete_role <- function(con,
                                  schema = "timeseries") {
  
  db_call_function(con,
                   "access_levels_deletes",
                   schema = schema)
}
