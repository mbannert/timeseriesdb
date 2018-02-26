#' Store a List of Time Series Chunk Wise to Avoid Memory Problem
#' 
#' This function is a wrapper around \code{\link{storeTimeSeries}}. It is used to split large lists of time series
#' according to memory limitations. This function uses INSERT INTO instead of the more convenient dbWritetable for performance reasons. DO NOT USE THIS FUNCTIONS IN LOOPS OR LAPPLY! This function can handle a set of time series on its own and is much faster than looping over a list. Non-unique primary keys are overwritten !
#' 
#' @author Matthias Bannert, Gabriel Bucur
#' @param series character name of a time series, S3 class ts. When used with lists it is convenient to set series to names(li). Note that the series name needs to be unique in the database!
#' @param con a PostgreSQL connection object.
#' @param li list of time series. Defaults to NULL to no break legacy calls that use lookup environments.
#' @param tbl character string denoting the name of the main time series table in the PostgreSQL database.
#' @param md_unlocal character string denoting the name of the table that holds unlocalized meta information.
#' @param overwrite logical should existing records (same primary key) be overwritten? Defaults to TRUE.
#' @param chunksize integer number of chunks. Defaults to NULL, invoking automatic chunk determination based on C Stack size.
#' @param schema SQL schema name. Defaults to timeseries.
#' @importFrom DBI dbGetQuery
#' @export
storeListChunkWise <- function(series,
                               con,
                               li = NULL,
                               tbl="timeseries_main",
                               md_unlocal = "meta_data_unlocalized",
                               overwrite = T,
                               chunksize = 10000,
                               schema = "timeseries"){
 
  
  name_chunks <- split(series,ceiling(seq_along(names(li))/chunksize))
  
  # loop over the chunks in order to store it chunk wise 
  # otherwise we run into stack limit on the server
  for(i in seq_along(name_chunks)){
    storeTimeSeries(name_chunks[[i]],con,li = li[name_chunks[[i]]],
                    overwrite = overwrite,
                    tbl = tbl,
                    schema = schema)  
  }
}
  
