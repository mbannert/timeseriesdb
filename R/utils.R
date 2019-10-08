#' 
#' Helper function to convert time series indices of the form 2005.75
#' to a date representation like 2005-07-01.
#' Does not currently support sub-monthly frequencies.
#' 
#' @param x numeric A vector of time series time indices (e.g. from stats::time)
#' @param as.string logical If as.string is TRUE the string representation of the 
#' Date is returned, otherwise a Date object.
#' 
#' @export
index_to_date <- function (x, as.string = FALSE) 
{
  years <- floor(x + 1/24)
  months <- floor(12*(x - years + 1/24)) + 1
  # No support for days currently
  # datestr <- paste(years, months, 1, sep = "-")
  datestr <- sprintf("%d-%02d-01", years, months)
  
  if(!as.string) {
    return(as.Date(datestr))
  } else {
    return(datestr)
  }
}


#' @export
`[.tslist` <- function(x, i) {
  x <- unclass(x)
  out <- x[i]
  class(out) <- c("tslist", "list")
  out
}
