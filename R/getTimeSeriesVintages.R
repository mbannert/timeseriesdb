
#' Get all available vintages for the time series identified by \code{series}
#'
#' @param series character Names of the time series for which to get the vintages
#' @param con PostgreSQL connection object.
#' @param tbl_vintages character string denoting the name of the vintages time series table in the PostgreSQL database.
#' @param schema SQL schema name. Defaults to timeseries.
#'
#' @export
#'
getTimeSeriesVintages <- function(series,
                                  con,
                                  tbl_vintages = "timeseries_vintages",
                                  schema = "timeseries") {
  
  in_clause <- paste0("('", paste(series, collapse = "','"), "')")
  
  query <- sprintf("SELECT
                      ts_key, lower(ts_validity) as lower_bound, upper(ts_validity) as upper_bound
                    FROM %s.%s 
                    WHERE ts_key in %s
                    ORDER BY lower(ts_validity)",
                   schema,
                   tbl_vintages,
                   in_clause)
  class(query) <- "SQL"
  
  # Get the vintage validity ranges
  db_return <- runDbQuery(con, query)
  
  # Extract the keys so we can produce a list of data frames of 
  # vintage ranges with series keys as names
  db_keys <- db_return$ts_key
  db_return$ts_key <- NULL
  
  # Set all NAs to Inf so they can be conveniently compared to other Dates (only upper bounds can be NA)
  db_return[is.na(db_return)] <- structure(Inf, class = "Date")
  
  # Split the data.frame into a list by series key
  vintages <- split(db_return, db_keys)
  
  # Set the vintage ranges of series not found in the db to NA
  vintages[setdiff(series, db_keys)] <- NA
  
  vintages
}
