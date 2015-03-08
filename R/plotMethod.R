#' Plot a list of Time Series
#
#' Directly plot time series of mixed frequency into one plot. 
#' \code{\link{readTimeSeries}} returns a list of time series. The returned list is 
#' also of class tslist which makes it very convenient to plot timeseries, 
#' directly out of the database. This function gets automatically the right ranges 
#' for the axes to plot all values and dates in contained in the list. 
#' 
#' @param x a list of time series. Object should be of class tslist. 
#' @param ... parameters than can simply by passed on tot the plot function
#' @param use_legend logical. Should legend be used. Defaults to TRUE. Useful to switch of if so many time series are drawn that they are hard to distinguish anyway. 
#' @export plot.tslist
#' @rdname plotMethods
plot.tslist <- function(x,...,use_legend = T){
  stopifnot(is.list(x))
  if(!(length(x) > 1)){
    return(plot(x[[1]]))
  } 
  
  # Get the min and max date from all the series ---------------
  # this needs to be done to figure out the plot dimensions... 
  # the same holds for the value itself
  # min dates
  min_date <- unlist(lapply(x,function(series) min(time(series))))
  min_date_nm <- names(min_date)[which.min(min_date)]
  min_date_value <- min_date[which.min(min_date)]
  # min values
  min_value <- unlist(lapply(x,function(series) min(series,na.rm = T)))
  min_value_nm <- names(min_value)[which.min(min_value)]
  min_value_value <- min_value[which.min(min_value)]
  
  # max dates
  max_date <- unlist(lapply(x,function(series) max(time(series))))
  max_date_nm <- names(max_date)[which.max(max_date)]
  max_date_value <- max_date[which.max(max_date)]
  # min values
  max_value <- unlist(lapply(x,function(series) max(series,na.rm = T)))
  max_value_nm <- names(max_value)[which.max(max_value)]
  max_value_value <- max_value[which.max(max_value)]
  
  # length of the list, plot the first elements
  l <- length(x)
  li_m_first <- x[-1]
  
  # configure canvas, to have sufficient space for the legend here... 
  if(use_legend){
    par(mar=c(3.1, 3.1, 3.1, 15.1), xpd=TRUE)  
  }
  
  
  
  
  # create a canvas by plotting the first element 
  plot(x[[1]],xlim = c(min_date_value,max_date_value),
       ylim = c(min_value_value,max_value_value),
       xlab = "",
       ylab = "")
  
  # create expression (which is technically a string)
  lines_expr <- sprintf("lines(x$%s,col=%s,lwd=%s)",
                        names(x),
                        seq_along(x),lwd=3)
  
  # run the expression to add lines to the existing plot
  eval(parse(text = lines_expr))  
  # Add legend to top right, outside plot region
  if(use_legend){
    legend("topright", legend = names(x),
           fill = seq_along(x),cex = 0.6,inset=c(-.32,0),xpd = T,bty = "n")
  }
  
}
