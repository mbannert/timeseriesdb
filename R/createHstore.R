#' Function to Create Hstore Key Value Pair Mapping 
#' 
#' This function creates a key value pair mapping from a time series object. 
#' It returns an hstore object that can be inserted to a PostgreSQL database relation field of type hstore. 
#' @author Matthias Bannert
#' @param x a time series object
#' @examples
#' ts1 <- ts(rnorm(100),start = c(1990,1),frequency = 4)
#' createHstore(ts1)
#' 
#' @export
createHstore <- function(x) UseMethod("createHstore")

#' @return \code{NULL}
#'
#' @method createHstore ts
#' @S3method createHstore ts
createHstore.ts <- function(x){
  tm <- time(x)
  paste(sprintf('"%s"=>"%s"',
                zooLikeDateConvert(tm),
                as.character(x)),
        collapse=",")
}
