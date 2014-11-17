#' Add Meta Information to R Environments 
#' 
#' This function adds meta information to environments that 
#' are explicitly meant to store Meta Information. This function 
#' can be used separately in interactive R Session or to facilitate
#' mapping database information to R. 
#' 
#' @param series character name key of 
#' @param map_list list to represent key value mapping. Could also be of class miro. 
#' @param meta_env name of the environment that holds the meta
#' environment. Defaults to meta_localized.
#' @param overwrite logical should existing meta information be overwritten inside
#' the environment?
#' @export
add_mi <- function(series,map_list,
                   meta_env = 'meta_localized',
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
  
  if(!exists(meta_env,envir = .GlobalEnv)) {
    meta <- new.env()
    meta[[series]] <- map_list
    assign(meta_env,meta,envir = environment())
  } else {
    # if environment exists we need to check
    # whether the object exists and if so
    # whether it needs be overwritten
    if(overwrite){
      meta <- get(meta_env)
      meta[[series]] <- map_list
    } else {
      stop('Meta Information unchanged. \n
          Set overwrite to T or choose other meta name.')
    }
  }
  
  
  out <- get(meta_env,environment())
  class(out) <- c('meta_env','environment')
  out
}

