#' Search Database for keys based on string wildcards
#' 
#' This function searches for keys that match a particular 
#' chunk of the key and returns all matches in a result set.
#' 
#' @author Matthias Bannert
#' @param ... character string patterns. 
#' @param connect character name of the PostgreSQL connection object.
#' @param tbl character representation of the name of the main timeseries
#' table. Defaults to 'timeseries_main'
searchDbForKeys <- function(...,connect = "con",tbl = "timeseries_main"){
  # Because we cannot really use a global binding to 
  # the postgreSQL connection object which does not exist at the time
  # of compilation, we use the character name of the object here. 
  connect <- get(connect)
  # private add_tag function, might want to add it as a 
  # public function
  add_tag <- function(x,open,close){
    paste(open,x,close,sep="")
  }
  
  like <- unlist(list(...))
  stopifnot(is.character(like))
  like <- add_tag(like,"'%","%'")
  like <- paste(like,collapse=" AND ts_key LIKE ")
  statement <- paste("SELECT ts_key FROM ",tbl,
                     " WHERE ts_key LIKE ",like,sep="")
  res <- dbGetQuery(connect,statement)
  
  # return an object of class result set
  result_set <- list()
  class(result_set) <- "rs"
  result_set$keys <- as.matrix(res)
  result_set$selection <- NULL
  result_set
  
}

#' @export
print.rs <- function(x, ...) {
  cat("The following keys fit the search pattern: \n")
  print(x$keys)
  cat("Select for fetching from Database: \n")
  print(x$select)
}


