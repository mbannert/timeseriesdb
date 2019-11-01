#' Convert json representation into time series
#'
#' @param jsn JSON string to convert
#' @param as.dt Should the result be returned as a data.table?
#' 
#' @import data.table
#' @importFrom xts xts
#' @importFrom stats ts
#' @importFrom jsonlite fromJSON
#' 
#' @export
json_to_ts <- function(jsn, as.dt = FALSE) {
  dta <- fromJSON(jsn)
  
  if(as.dt) {
    if(is.null(dta$frequency)) {
      dta$frequency <- NA
    }
    
    return(
      as.data.table(dta)
    )
  }
  
  if(is.null(dta$frequency)) {
    return(
      xts(
        dta$value,
        order.by = as.Date(dta$time)
      )
    )
  } else {
    return(
      ts(
        dta$value,
        start = date_to_index(dta$time[1]),
        frequency = as.numeric(dta$frequency[1])
      )
      
    )
  }
}