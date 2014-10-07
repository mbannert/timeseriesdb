#' Function to Create Hstore Key Value Pair Mapping 
#' 
#' This function creates a key value pair mapping from a time series object. 
#' It returns an hstore object that can be inserted to a PostgreSQL database relation field of type hstore. 
#' @author Matthias Bannert
#' @title Create Hstore
#' @param x a time series object or two column data frame.
#' @param ... optional arguments, e.g. position of the key col and
#' pasition of the value col in a data.frame.
#' @examples
#' ts1 <- ts(rnorm(100),start = c(1990,1),frequency = 4)
#' createHstore(ts1)
#' 
#' @export
createHstore <- function(x,...) UseMethod("createHstore")

#' @rdname createHstore
#' @export
createHstore.ts <- function(x,...){
  tm <- time(x)
  paste(sprintf('"%s"=>"%s"',
                zooLikeDateConvert(tm),
                as.character(x)),
        collapse=",")
}

#' @rdname createHstore
#' @export
createHstore.data.frame <- function(x,key = 1, value = 2){
  # only allow to cols because its KEY => VALUE
  stopifnot(ncol(x) == 2)
  
  paste(sprintf('"%s"=>"%s"',
                as.character(x[,key]),
                as.character(x[,value]),
        collapse=",")
        )
}



