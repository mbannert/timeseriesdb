#' Get All access levels and their description
#'
#' @param con RPostgres connection object
#' @param schema character Name of timeseries schema
#'
#' @return access levels data.frame with columns `role` and `description` and `is_default`
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
#' @param access_level_name character name of the access level
#' @param schema character name of the schema. Defaults to 'timeseries'.
#'
#' @importFrom jsonlite fromJSON
#' @export
db_delete_access_levels <- function(con,
                                    access_level_name,
                           schema = "timeseries") {

  out <- db_call_function(con,
                   "access_levels_delete",
                   list(
                     access_level_name
                   ),
                   schema = schema)
  
  out_parsed <- fromJSON(out)

  if(out_parsed$status == "warning") {
    warning(access_level_name,
            " ",
            out_parsed$message)
  } else if(out_parsed$status == "error") {
    stop(out_parsed$message)
  }

  out_parsed

}


#' Insert a role in access levels table
#'
#' @param con RPostgres connection object
#' @param access_level_name character name of the access level
#' @param access_level_description character description of the access level. Defaults to NA.
#' @param access_level_default should the new access level be a default. Defaults to NA.
#' @param schema character name of the schema. Defaults to 'timeseries'.
#'
#' @importFrom jsonlite fromJSON
#' @export
db_insert_access_levels <- function(con,
                                    access_level_name,
                                    access_level_description = NA,
                                    access_level_default = NA,
                                    schema = "timeseries") {

  out <- db_call_function(con,
                          "access_levels_insert",
                          list(
                            access_level_name,
                            access_level_description,
                            access_level_default
                          ),
                          schema = schema)
  
  out_parsed <- fromJSON(out)
  
  if(out_parsed$status == "warning") {
    warning(out_parsed$message)
  } else if(out_parsed$status == "error") {
    stop(out_parsed$message)
  }

  out_parsed

}

#' Set the Default Access Level
#'
#' @param con Postgres  connection object
#' @param access_level character Name of the access level to set as the default
#' @param schema character Timeseries schema name
#'
#' @export
#' @importFrom jsonlite fromJSON
db_set_default_access_level <- function(con,
                                        access_level,
                                        schema = "timeseries") {
  out <- db_call_function(con,
                          "set_access_level_default",
                          list(
                            access_level
                          ),
                          schema = schema)
  out_parsed <- fromJSON(out)

  if(out_parsed$status == "error") {
    stop(out_parsed$message)
  }

  out_parsed
}
