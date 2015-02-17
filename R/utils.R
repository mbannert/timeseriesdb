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
getListDepth <- function(this) ifelse(is.list(this), 1L + max(sapply(this, depth)), 0L)





#' Check whether an PostgreSQL object is valid or not.
#' 
#' Support function that verifies that the holding a reference to a
#' foreign object is still valid for communicating with the RDBMS
#' 
#' @param dbObj a PostgreSQLConnection connection object. Check if the connection has expired.
#' @return A logical scalar.
#' @name dbIsValid
#' @rdname dbIsValid 
#' @export
setMethod("dbIsValid", "PostgreSQLConnection", function(dbObj) {
  isValid <- tryCatch({dbGetInfo(dbObj)},
                      error = function(e) NULL)
  !is.null(isValid)  
})


