#' Select Results in a Result Set for Fetching from Database
#' 
#' This function lets the user select multiple time series from 
#' a result set that can be fetched from the database subsequently. 
#' To do so use \code{\link{fetchSelectedResults}}.
#' 
#' @author Matthias Bannert
#' @param result_set object of class rs
#' @param selection numeric vector indicating line numbers of selected time series.
#' @export
selectResults <- function(result_set,selection){
  # sanity check 
  if (!(class(result_set) == "rs") ) stop("Input is not of class rs!")  
  result_set$selection <- selection
  assign(deparse(substitute(result_set)),value = result_set,envir = .GlobalEnv)
  print("Selection updated.")
}