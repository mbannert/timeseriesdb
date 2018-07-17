#' Convert ts style time index Date representation
#' 
#' Helper function to convert time series indices of the form 2005.75
#' to a date representation like 2005-07-01.
#' Does not currently support sub-monthly frequencies.
#' 
#' @param x numeric A vector of time series time indices (e.g. from stats::time)
#' @param as.string logical If as.string is TRUE the string representation of the 
#' Date is returned, otherwise a Date object.
#' 
#' @author Severin Thöni
#' @export
indexToDate <- function (x, as.string = FALSE) 
{
  years <- floor(x)
  months <- floor(12*(x - years + 1/24)) + 1
  # No support for days currently
  # datestr <- paste(years, months, 1, sep = "-")
  datestr <- sprintf("%d-%02d-1", years, months)
  
  if(!as.string) {
    date <- as.Date(datestr)
  } else {
    date <- datestr
  }
  date
}
