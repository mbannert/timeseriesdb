#' Concatenate Time Series and Resolve Overlap Automatically
#' 
#' Resolve overlap determines which of two ts class time series is
#' reaching further and arranges the two series into first and second 
#' series accordingly. Both time series are concatenated to one
#' if both series had the same frequency. 
#'
#' @param ts1 ts time series, typically the older series
#' @param ts2 ts time series, typically the younger series
#' @export
#' @examples
#' ts1 <- ts(rnorm(100),start = c(1990,1),frequency = 4)
#' ts2 <- ts(1:48,start = c(2000,1),frequency = 4)  
#' resolveOverlap(ts1,ts2)   
#' 
#' # automatical detection of correction sequence!
#' ts2 <- ts(rnorm(100),start = c(1990,1),frequency = 4)            
#' ts1 <- ts(1:48,start = c(2000,1),frequency = 4)
#' resolveOverlap(ts1,ts2)   
#' 
#' # both series are of the same length use sequence of arguments.
#' ts1 <- ts(rnorm(100),start = c(1990,1),frequency = 4)
#' ts2 <- ts(1:48,start = c(2003,1),frequency = 4)  
#' resolveOverlap(ts1,ts2)
resolveOverlap <- function(ts1,ts2){
  stopifnot(is.ts(ts1))
  stopifnot(is.ts(ts2))
  # check which time series is supposed to be 
  # first and second
  if(max(time(ts2)) > max(time(ts1))){
    first <- ts1
    second <- ts2
  } else if(max(time(ts1)) > max(time(ts2))){
    first <- ts2
    second <- ts1
  } else{
    warning('Cannot resolve overlap automatically, both time series reach until the same period. Using sequency of functions arguments.')
    first <- ts1
    second <- ts2
  }
  
  stopifnot(frequency(first) == frequency(second))
  # stop if there is no overlap
  if(max(time(first)) < min(time(second))){
    stop('no overlap in time series.')
  }
  # check overlap
  min_second <- min(time(second))
  pos_first <- which(time(first) == min_second)
  first_chunk <- first[1:(pos_first-1)]
  ts(c(first_chunk,second),
     frequency = frequency(first),
     start = c(1990,1))
  
}

