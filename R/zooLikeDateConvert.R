#' Zoo like Date Conversion
#' 
#' This function is taken from the zoo package. It is basically the
#' S3 method as.Date.numeric of the package zoo. It is used to turn
#' 2005.75 (3rd quarter of 2005) like date formats into dates
#' like 2005-07-01.
#'
#' @param x object of class ts or zoo (experimental)
#' @param offset numeric defaults to 0. See the zoo package for more information.
#' @rdname zooLikeDateConvert
#' @name zooLikeDateconvert
#' 
#' @author Achim Zeileis, Gabor Grothendieck, Jeffrey A. Ryan,
#' Felix Andrews
#' @export
zooLikeDateConvert <- function (x, offset = 0, as.string = FALSE) 
{
  if(class(x) == "zoo"){
    stats::time(x)
  } else {
    time.x <- unclass(stats::time(x)) + offset
    if (stats::frequency(x) == 1) 
      datestr <- paste(time.x, 1, 1, sep = "-")
    else if (stats::frequency(x) == 2)
      datestr <- paste((time.x + 0.001)%/%1, 6 * (stats::cycle(x) - 1) + 
                      1, 1, sep = "-")
    else if (stats::frequency(x) == 4) 
      datestr <- paste((time.x + 0.001)%/%1, 3 * (stats::cycle(x) - 1) + 
                      1, 1, sep = "-")
    else if (stats::frequency(x) == 12) 
      datestr <- paste((time.x + 0.001)%/%1, stats::cycle(x), 1, sep = "-")
    else stop("unable to convert ts time to Date class")  
    
    if(!as.string) {
      date <- as.Date(datestr)
    } else {
      date <- datestr
    }
    date
    
  }
}













