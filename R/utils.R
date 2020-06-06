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
  # If called as index_to_date(time(a_ts))
  # x is a ts. Unclass it so we can work with the faster basic operators
  x <- c(x)

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

# recursive function to check depth of list. hat tip flodel
# at stackoverflow: http://stackoverflow.com/questions/13432863/determine-level-of-nesting-in-r
#' Determine depth of a list
#'
#' This function recursively checks the depth of a list and returns an integer value of depth
#'
#' @param this an object of class list
#' @details Hat tip to flodel at stackoverflow for suggesting this light weight way analyze depth of a nested list. Further complexity needs to be added to cover the fact that data.frame are lists, too. A more sophisticated recursive function can be found in the gatveys2 package.
#' @references http://stackoverflow.com/questions/13432863/determine-level-of-nesting-in-r
#' @export
get_list_depth <- function(this) {
  ifelse(
    is.list(this),
    ifelse(
      length(this) > 0,
      1L + max(sapply(this, get_list_depth)),
      1L
    ),
    0L
  )
}


#' Helper to construct SQL function calls
#'
#' Calls function `schema`.`fname` with the given `args`, returning
#' the result.
#'
#' @param con RPostgres connection object
#' @param fname character Name of the function to be called
#' @param schema character Name of the timeseries schema
#' @param args list of function arguments. A single, unnested list. 
#'
#' @return value of `dbGetQuery(con, "SELECT * FROM schema.fname($args)")$fname`
db_call_function <- function(con,
                             fname,
                             args = NULL,
                             schema = "timeseries") {
  query <- sprintf("SELECT * FROM %s.%s(%s)",
                   dbQuoteIdentifier(con, schema),
                   dbQuoteIdentifier(con, fname),
                   ifelse(length(args) > 0,
                          paste(sprintf("$%d", 1:length(args)), collapse = ", "),
                          ""))


  res <- dbGetQuery(con, query, args)

  if(fname %in% names(res)) {
    res[[fname]] # query returns value (e.g. JSON) -> unwrap the value
  } else {
    res # query returns table -> just return the DF as it comes
  }
}
