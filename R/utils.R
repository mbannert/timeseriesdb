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

#' Convert date-likes to time index
#'
#' @param x The Date or Y-m-d string to convert 
#'
#' @return The numeric representation of the date that can be used with ts
#' @export
date_to_index <- function(x) {
  x <- as.character(x)
  components <- as.numeric(unlist(strsplit(x, "-")))
  components[1] + (components[2] - 1)/12
}

#' @export
`[.tslist` <- function(x, i) {
  x <- unclass(x)
  out <- x[i]
  class(out) <- c("tslist", "list")
  out
}


#' Get internal use case number
#' 
#' This is a little helper to calculate a use case number
#' from whether vintage and/or release_date are NULL.
#' This helps by establishing a common frame of reference and
#' enhances readability of db_ and query_ functions
#'
#' @details 
#' The use cases are numbered thusly:
#' 1 - Keep vintages but no release date specifiec
#' 2 - Keep vintages and withold latest vintage until release date
#' 3 - Neither vintages nor release date
#' 4 - No vintages but withold newer version until release date
#'
#' @param vintage 
#' @param release_date 
get_use_case <- function(vintage, release_date) {
  1 + 2*is.null(vintage) + !is.null(release_date) # Because coding fun
}