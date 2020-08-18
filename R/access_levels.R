#' Change the Access Level for Time Series
#'
#' @name change_access_level
#' @rdname change_access_level
#' @inheritParams param_defs
#' @family access levels functions
#' 
#' @export
#' @importFrom jsonlite fromJSON
#' 
db_ts_change_access <- function(con,
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

#' @rdname change_access_level
#' @inheritParams param_defs
#'
#' @return
#' @export
#' @importFrom jsonlite fromJSON
db_ts_change_access_dataset <- function(con,
                                        dataset,
                                        new_access_level,
                                        validity = NA,
                                        schema = "timeseries") {
  out <- db_call_function(con,
                          "change_access_level_dataset",
                          list(
                            dataset,
                            new_access_level,
                            validity
                          ),
                          schema = schema)
  parsed <- fromJSON(out)

  if(parsed$status == "error") {
    stop(parsed$message)
  } else if(parsed$status == "warning") {
    warning(parsed$message)
  }

  parsed
}

#' Get All access levels and their description
#'
#' @inheritParams param_defs
#' @family access levels functions
#'
#' @return access levels data.frame with columns `role` and `description` and `is_default`
#' @export
#' 
db_access_list_levels <- function(con,
                                  schema = "timeseries") {

  db_call_function(con,
                   "list_access_levels",
                   schema = schema)
}


#' Delete a role in access levels table
#'
#' @inheritParams param_defs
#' @family access levels functions
#'
#' @importFrom jsonlite fromJSON
#' @export
db_access_delete_level <- function(con,
                                   access_level,
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
#' @inheritParams param_defs
#' @param access_level_name \strong{character} name of the access level to insert.
#' @param access_level_description \strong{character} description of the access level. Defaults to NA.
#' @param access_level_default set if the new access level should be the default. Defaults to NA.
#' @family access levels functions
#'
#' @importFrom jsonlite fromJSON
#' @export
db_access_create_level <- function(con,
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
#' @inheritParams param_defs
#' @family access levels functions
#'
#' @export
#' @importFrom jsonlite fromJSON
db_access_set_default <- function(con,
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
