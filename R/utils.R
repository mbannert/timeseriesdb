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
  datestr <- sprintf("%d-%02d-01", years, months)
  
  if(!as.string) {
    date <- as.Date(datestr)
  } else {
    date <- datestr
  }
  date
}

#' Check Validity of a PostgreSQL connection
#' 
#' Is the PostgreSQL connection expired?
#' 
#' @param dbObj PostgreSQL connection object.
#' @importFrom DBI dbIsValid
#' @importFrom DBI dbGetInfo
#' @import RPostgreSQL
#' @import methods
#' @docType methods
#' @aliases dbIsValid
#' @rdname dbIsValid-methods
#' @export
setMethod("dbIsValid", "PostgreSQLConnection", function(dbObj) {
  isValid <- tryCatch({DBI::dbGetInfo(dbObj)},
                      error = function(e) NULL)
  !is.null(isValid)  
})

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
getListDepth <- function(this) {
  ifelse(
    is.list(this), 
    ifelse(
      length(this) > 0,
      1L + max(sapply(this, getListDepth)),
      1L
    ),
    0L
  )
}

.createValues <- function(li, validity = NULL, store_freq, release_date = NULL){
  # CREATE ELEMENTS AND RECORDS ##########################
  # use the form (..record1..),(..record2..),(..recordN..)
  # to be able to store everything in one big query
  hstores <- unlist(lapply(li,createHstore))
  series <- names(li)
  freqs <- sapply(li,function(x) {
    ifelse(inherits(x,"zoo"),'NULL',stats::frequency(x))
  })
  
  if(is.null(release_date)) {
    release_date <- "DEFAULT"
  } else {
    tryCatch(
      release_date <- strftime(release_date, format = "%F %T %z"),
      error = function(e) {
        msg <- sprintf("Failed to parse release_date \"%s\". Please make sure it is an object which can be converted to \"POSIXlt\" for strftime!", release_date)
        stop(msg)
      }
    );
  }
  
  if(is.null(validity)){
    if(!store_freq){
      values <- paste(paste0("('",
                             paste(series,
                                   hstores,
                                   sep="','"),
                             "', '", release_date, "')"),
                      collapse = ",")
    } else {
      values <- paste(paste0("('",
                             paste(series,
                                   hstores,
                                   freqs,
                                   sep="','"),
                             "', '", release_date, "')"),
                      collapse = ",")
    }
  } else {
    if(!store_freq){
      values <- paste(paste0("('",
                             paste(series,
                                   validity,
                                   hstores,
                                   sep="','"),
                             "', '", release_date, "')"),
                      collapse = ",")
    } else {
      values <- paste(paste0("('",
                             paste(series,
                                   validity,
                                   hstores,
                                   freqs,
                                   sep="','"),
                             "', '", release_date, "')"),
                      collapse = ",")
    }
  }
  values <- gsub("''","'",values)
  values <- gsub("::hstore'","::hstore",values)
  values <- gsub("'NULL'","NULL",values)
  values <- gsub("'DEFAULT'", "DEFAULT", values)
  values
}


#'@importFrom stats tsp time
.createValuesMeta <- function(li){
  # CREATE META INFORMATION -------------------------------------------------
  # automatically generated meta information
  md_generated_by <- Sys.info()["user"]
  md_resource_last_update <- Sys.time()
  md_coverages <- unlist(lapply(li,function(x){
    
    if(inherits(x, "zoo")) {
      idx <- time(x)
      t0 <- min(idx)
      t1 <- max(idx)
      if(class(t0) == "Date") {
        time_range <- as.character(c(t0, t1))
      } else if(class(t0) == "character") {
        time_range <- c(t0, t1)
      } else {
        time_range <- indexToDate(c(t0, t1), as.string = TRUE)
      }
    } else {
      tsp.x <- tsp(x)
      time_range <- indexToDate(tsp.x[c(1, 2)], as.string = TRUE)
    }
    
    sprintf('%s to %s',
            time_range[1],
            time_range[2]
    )}
  ))
  
  series <- names(li)
  
  # same trick as for data itself, one query
  md_values <- paste(paste0("('",
                            paste(series,
                                  md_generated_by,
                                  md_resource_last_update,
                                  md_coverages,
                                  sep="','"),
                            "')"),
                     collapse = ",")
  md_values
}

stringSafeAsNumeric <- function(x) {
  y <- suppressWarnings(as.numeric(x))
  if(any(is.na(x) != is.na(y))) {
    x
  } else {
    y
  }
}
