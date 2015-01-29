#' Add Meta Information to R Environments 
#' 
#' This function adds meta information to environments that 
#' are explicitly meant to store Meta Information. This function 
#' can be used separately in interactive R Session or to facilitate
#' mapping database information to R. 
#' 
#' @param series character name key of 
#' @param map_list list to represent key value mapping. Could also be of class miro. 
#' @param meta_env an environment that already holds meta information and should be extended. 
#' Defaults to NULL in which case it creates and returns a new environment.
#' @param overwrite logical should existing meta information be overwritten inside
#' the environment?
#' @export
addMetaInformation <- function(series,map_list,
                   meta_env = NULL,
                   overwrite = T){
  # sanity check
  stopifnot(is.list(map_list))
  # check if environment exists, 
  # if not create it and put 
  # the meta information in there, stored
  # under the series' name.
  
  # general adjustment, add class
  # meta informaiton for R objects.
  class(map_list) <- c('miro','list')
  
  # remove empty elements from a list 
  # this can be important for generically
  # created meta information
  map_list[map_list == ''] <- NULL
    
  if(is.null(meta_env)){
    meta_env <- new.env()
    meta_env[[series]] <- map_list
  } else {
    # if environment exists we need to check
    # whether the object exists and if so
    # whether it needs be overwritten
    if(overwrite || is.null(meta_env)){
      meta_env[[series]] <- map_list
    } else {
      stop('Meta Information unchanged. \n
          Set overwrite to T or choose other meta environment.')
    }
  }
  
  class(meta_env) <- c('meta_env','environment')
  meta_env
}

