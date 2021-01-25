#' Change the Access Level of a Time Series
#'
#' @name change_access_level
#' @rdname change_access_level
#' @inheritParams param_defs
#' @family access levels functions
#'
#' @export
#' @importFrom jsonlite fromJSON
db_ts_change_access <- function(con,
                                ts_keys,
                                access_level,
                                valid_from = NULL,
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
                                               "ts_change_access_level",
                                               list(
                                                 access_level,
                                                 valid_from
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
db_dataset_change_access <- function(con,
                                        dataset,
                                        access_level,
                                        valid_from = NULL,
                                        schema = "timeseries") {

  out <- db_call_function(con,
                          "dataset_change_access_level",
                          list(
                            dataset,
                            access_level,
                            valid_from
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

#' Get All Access Levels and Their Description
#'
#' Gets an overview of roles and shows whether a role (aka access level) is
#' the default level for series stored without an explicitly specified access level.
#'
#'
#' @inheritParams param_defs
#' @family access levels functions
#'
#' @return access levels data.frame with columns `role` and `description` and `is_default`
#' @export
#'
db_access_level_list <- function(con,
                                  schema = "timeseries") {

  db_call_function(con,
                   "access_level_list",
                   schema = schema)
}


#' Delete a role in access levels table
#'
#' @inheritParams param_defs
#' @family access levels functions
#'
#' @importFrom jsonlite fromJSON
#' @export
db_access_level_delete <- function(con,
                                   access_level,
                                   schema = "timeseries") {

  out <- db_call_function(con,
                          "access_level_delete",
                          list(
                            access_level
                          ),
                          schema = schema)

  out_parsed <- fromJSON(out)

  if(out_parsed$status == "warning") {
    warning(access_level,
            " ",
            out_parsed$message)
  } else if(out_parsed$status == "error") {
    stop(out_parsed$message)
  }

  out_parsed

}


#' Create a New Role (Access Level)
#'
#' Creates a new role in the database. Roles represent access levels and together
#' with the assignment of roles to time series, versions of time series or datasets
#' define who is allowed to access a particular series.
#'
#' @inheritParams param_defs
#' @param access_level_name \strong{character} name of the access level to insert.
#' @param access_level_description \strong{character} description of the access level. Defaults to NA.
#' @param access_level_default set if the new access level should be the default. Defaults to NA.
#' @family access levels functions
#'
#' @importFrom jsonlite fromJSON
#' @export
db_access_level_create <- function(con,
                                   access_level_name,
                                   access_level_description = NULL,
                                   access_level_default = NULL,
                                   schema = "timeseries") {


  out <- db_call_function(con,
                          "access_level_insert",
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
#' Changes the default access level. Apparently only one access level can be
#' the default level at a time.
#'
#' @inheritParams param_defs
#' @family access levels functions
#'
#' @export
#' @importFrom jsonlite fromJSON
db_access_level_set_default <- function(con,
                                  access_level,
                                  schema = "timeseries") {
  out <- db_call_function(con,
                          "access_level_set_default",
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

#' Find Out About the Access Level of a Vintage
#'
#' Provide the function with vector of time series keys and find out which access level is necessary to access the supplied keys.
#'
#' @inheritParams param_defs
#' @export
db_ts_get_access_level <- function(con,
                                   ts_keys,
                                   valid_on = NULL,
                                   schema = "timeseries") {
  db_with_temp_table(con,
                     "tmp_get_access",
                     data.frame(ts_key = ts_keys),
                     field.types = c(
                       ts_key = "text"
                     ),
                     {
                       db_call_function(con,
                                        "ts_get_access_level",
                                        list(valid_on),
                                        schema = schema
                       )
                     },
                     schema = schema
  )
}
