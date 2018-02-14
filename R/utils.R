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
getListDepth <- function(this) ifelse(is.list(this), 1L + max(sapply(this, getListDepth)), 0L)

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



#' Delete all objects except for specific objects
#' 
#' Run rm(list=ls()) but sparing some objects from being deleted. 
#' This function is particularly handy when you want to clear the memory but want to keep the 
#' the database connection object.
#' 
#' @param but character vector of variables that should not be deleted. 
#' @param env environment to clean up. Defaults to .Globalenv
#' @param quiet logical should functions print output? Defaults to falase.
#' @rdname rmAllBut
#' @export
rmAllBut <- function(but,env = .GlobalEnv, quiet = F){
  vars <- ls(envir = env)
  del <- vars[!vars %in% but]
  rm(list = del,envir = env)
  if(!quiet){
    cat(sprintf("All objects but '%s' deleted.",
                paste(but,collapse = ", ")))
  }
  
}

#' @export 
print.SQL <- function(x,...){
  cat(gsub("\n[ \t]+","\n",x))
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
    # TODO: Make sure release_date is in Postgres compatible format
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

.createValuesMeta <- function(li){
  # CREATE META INFORMATION -------------------------------------------------
  # automatically generated meta information
  md_generated_by <- Sys.info()["user"]
  md_resource_last_update <- Sys.time()
  md_coverages <- unlist(lapply(li,function(x){
    sprintf('%s to %s',
            min(zooLikeDateConvert(x)),
            max(zooLikeDateConvert(x))
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


