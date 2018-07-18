#' Function to Create Hstore Key Value Pair Mapping 
#' 
#' This function creates a key value pair mapping from a time series object. 
#' It returns an hstore object that can be inserted to a PostgreSQL database relation field of type hstore. 
#' @author Matthias Bannert
#' @title Create Hstore
#' @param x a time series object, a two column data frame or object of S3 class
#' miro (meta information for R objects).
#' @param ... optional arguments, fct = TRUE create text expressions of hstore function calls.
#' also for data.frames key_pos and value_pos could be given if they are different from 1 and 2. 
#'  e.g. position of the key col and
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
  # '1900-01-01 => -0.395131869823009, 1900-01-02 => -0.395131869823009, ...'::hstore
  tm <- zoo::index(x)
  
  # TODO: 1) This also has 16 digits for whole numbers
  #       2) How many digits are really necessary?
  paste0("'", 
         paste(sprintf("%s => %.16f", indexToDate(tm, as.string = TRUE), x),
               collapse=", "),
         "'::hstore")
}

#' @rdname createHstore
#' @export
createHstore.zoo <- function(x,...){
  tm <- zoo::index(x)
  
  if(class(tm) == "Date") {
    tm <- as.character(tm)
  } else {
    tm <- indexToDate(as.numeric(tm), as.string = TRUE)
  }
  paste0("'", 
         paste(sprintf("%s => %.16f", tm, x),
               collapse=", "),
         "'::hstore")
}




#' @rdname createHstore
#' @export
createHstore.data.frame <- function(x, key_col_index = 1){

  # only allow to cols because its KEY => VALUE
  stopifnot(ncol(x) == 2)
  
  # figure out the value column
  # since we only have two cols it must be 1 or 2 depending on the key col
  val_col_index <- `if`(key_col_index[1] == 1, 2, 1)
  
  paste(sprintf('"%s"=>"%s"',
                as.character(x[,key_col_index]),
                as.character(x[,val_col_index])),
        collapse=",")
}



#' @rdname createHstore
#' @export
createHstore.list <- function(x,...){
  dot_args <- list(...)
  # check if list is more than 2 dim
  if(getListDepth(x) != 1) stop('Only key-value pairs are accepted,
                         this list has too many dimensions!') 
  
  if(is.null(names(x))) stop('Only named lists are accepted.')
  
  # the => operator is deprecated in 
  # Postgres so if you want to use the new version function
  # based version use fct = T
  # the operator will be kept alive as long as postgres does 
  # the same 
  
  # 2017 Edit: deprecation was a misunderstanding
  # calling the hstore function everytime is not good. better use => !!!
  # being able to use hstore might be useful for some cases too, 
  # naming 'deprecated is unfortunate' might change naming at some point. 
  deprecated_hstore_operator <- paste(sprintf('"%s"=>"%s"',
                                              names(x),
                                              as.character(unlist(x))),
                                      collapse=",")
  
  # note the double ampersand here !!
  # it denotes a short-circuit logical operator.
  if(length(dot_args) != 0 && exists("fct",dot_args)){
    if(dot_args$fct){
      paste(sprintf("hstore('%s','%s')",
                    names(x),
                    as.character(unlist(x))),
            collapse="||")  
    } else{
      deprecated_hstore_operator
    }
    
  } else {
    deprecated_hstore_operator
  }
  
}

