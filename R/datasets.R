#' Create a new dataset
#' 
#' A dataset is a family of time series that belong to the same topic.
#' By default all series stored with `db_store_ts` belong to a default set. In order to 
#' assign them a different set, it must first be created with `db_create_dataset` after which
#' the series may be moved with `tbd`
#' 
#' For arbitrary collections of time series see [how do you reference a doc topic?]
#'
#' @param con PostgreSQL connection
#' @param set_name character The name of the set to be created
#' @param set_md meta Metadata about the set
#'
#' @importFrom RPostgres dbGetQuery dbQuoteIdentifier
#' @importFrom DBI Id
#' 
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
  
  dbGetQuery(con,
             sprintf("SELECT * FROM %screate_dataset($1, $2, $3)",
                     dbQuoteIdentifier(con, Id(schema = schema))),
             list(
               set_name,
               set_description,
               set_md
             ))$create_dataset
}


#' Title
#'
#' @param con 
#' @param set_name 
#' @param schema 
#'
#' @return
#' @export
#'
#' @examples
db_keys_in_dataset <- function(con,
                               set_name,
                               schema = "timeseries") {
  dbGetQuery(con,
             sprintf("SELECT * FROM %skeys_in_dataset($1)",
                     dbQuoteIdentifier(con, Id(schema = schema))),
             list(
               set_name
             ))$ts_key
}

#' Title
#'
#' @param con 
#' @param ts_keys 
#' @param schema 
#'
#' @return
#' @export
#'
#' @examples
db_dataset_of_keys <- function(con,
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
  
  dbGetQuery(con,
              sprintf("SELECT * FROM %sget_set_of_keys()",
                dbQuoteIdentifier(con, Id(schema = schema))))
}

## TODO: Name of function up for discussion.
#' Title
#'
#' @param con 
#' @param ts_keys 
#' @param set_name 
#' @param schema 
#'
#' @return
#' @export
#'
#' @examples
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
  
  # Error case: Set does not exist
  # Warning case: Only some keys found in catalog
  # Success case: you know what that means...
  out <- dbGetQuery(con,
             sprintf("SELECT * FROM %sassign_dataset($1)",
                     dbQuoteIdentifier(con, Id(schema = schema))),
             list(
               set_name
             ))
  
  # TODO: error/warn here or just pass status on?
  jsonlite::fromJSON(out$assign_dataset)
}
