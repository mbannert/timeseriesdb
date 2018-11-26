#' Store a List of Time Series Chunk Wise to Avoid Memory Problem
#' 
#' This function is a wrapper around \code{\link{storeTimeSeries}}. It is used to split large lists of time series
#' according to memory limitations. This function uses INSERT INTO instead of the more convenient dbWritetable for performance reasons. DO NOT USE THIS FUNCTIONS IN LOOPS OR LAPPLY! This function can handle a set of time series on its own and is much faster than looping over a list. Non-unique primary keys are overwritten !
#' 
#' @author Matthias Bannert, Gabriel Bucur
#' @param con a PostgreSQL connection object.
#' @param li list of time series. Defaults to NULL to no break legacy calls that use lookup environments.
#' @param series character name of a time series, S3 class ts. When used with lists it is convenient to set series to names(li). Note that the series name needs to be unique in the database!
#' @param tbl character string denoting the name of the main time series table in the PostgreSQL database.
#' @param md_unlocal character string denoting the name of the table that holds unlocalized meta information.
#' @param overwrite logical should existing records (same primary key) be overwritten? Defaults to TRUE.
#' @param chunksize integer number of chunks. Defaults to chunks of 10K. 
#' @param schema SQL schema name. Defaults to timeseries.
#' @param show_progress If TRUE, storeListChunkWise will print a progress indicator to the console. Default FALSE.
#' @importFrom DBI dbGetQuery
#' @export
storeListChunkWise <- function(con,
                               li,
                               series = names(li),
                               tbl="timeseries_main",
                               md_unlocal = "meta_data_unlocalized",
                               overwrite = T,
                               chunksize = 10000,
                               schema = "timeseries",
                               show_progress = FALSE){
 
  if(is.character(con)) {
    warning("You are using this function in a deprecated manner. Please use storeListChunkWise(con, li, series, ...) in the future.");
    tx <- con
    con <- series
    series <- li
    li <- con
  }
  
  name_chunks <- split(series,ceiling(seq_along(names(li))/chunksize))
  
  n_series <- length(series)
  n_chunks <- length(name_chunks)
  bar_width <- getOption("width") - 6
  
  if(show_progress) {  
    cat(sprintf("\r|%s| %d%%",
                paste(rep(" ", ceiling(bar_width)), collapse = ""),
                0))
  }
  
  # loop over the chunks in order to store it chunk wise 
  # otherwise we run into stack limit on the server
  for(i in seq_along(name_chunks)){
    storeTimeSeries(con, li = li[name_chunks[[i]]],
                    overwrite = overwrite,
                    tbl = tbl,
                    schema = schema)  
    
    if(show_progress) {  
      progress <- (i*chunksize)/n_series    
      cat(sprintf("\r|%s%s| %d%%",
                  paste(rep("=", floor(bar_width * min(progress, 1))), collapse = ""),
                  paste(rep(" ", ceiling(bar_width * max(0, (1 - progress)))), collapse = ""),
                  min(floor(100*progress), 100)))
    }
  }
}
