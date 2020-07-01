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
