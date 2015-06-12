#' Export A List of Time Series to CSV
#' 
#' Export a List of time series to semi-colon separated csv file. Typically multiple time series are exported as long format (melted format) files. 
#' 
#' @param tl list of time series
#' @param fname character file name. If set to NULL a standard file name chunk + Sys.Date is used.
#' @param cast logical. Should the resulting data.frame be cast to wide format? Defaults to TRUE
#' @param sep character that separates columns in .csv
#' @param dec character that separates decimals in .csv
#' @param LC_TIME_LOCALE character time locale that differs from the standard locale. e.g. en_US.UTF-8. Defaults to NULL and uses the standard locale then. 
#' @param date_format character denotes the date format. Defaults to NULL. If set to null the default is used: Jan 2010. In combination with LC\_TIME\_Locale various international date formats can be produced. 
#' @importFrom reshape2 dcast
#' @examples 
#' tslist <- list()
#' tslist$ts1 <- ts(rnorm(50),start = c(1990,1),frequency = 12)
#' Sys.getlocale() # gets all locale categories
#' Sys.getlocale("LC_TIME") # gets the time category only
#' exportTsList(tslist,LC_TIME_LOCALE = "de_DE.UTF-8",date_format="%Y %b")
#' # returns .csv file with a 2010-Mai style dates. 
#' exportTsList(tslist)
#' # returns 2010-05 style dates
#' # Quarterly data does also work with format
#' tslist_q <- list()
#' tslist_q$ts2 <- ts(rnorm(50),start = c(1990,4),frequency = 4)
#' exportTsList(tslist_q,date_format="%Y-0%q")
#' @export
exportTsList <- function(tl,fname = NULL,cast = T,sep = ";",dec=".",
                         LC_TIME_LOCALE = NULL,
                         date_format = NULL){
 
  # check if all series got some frequencies 
  # other we can't export
  
  frq <- unique(sapply(tl,frequency))
  
  if(length(frq) != 1) stop("All time series of within a list need to have the same frequency for proper export.")
  
   
  tl <- lapply(tl,as.xts)
  out_list <- lapply(names(tl),function(x){
    dframe <- data.frame(time = time(tl[[x]]),
                         value = tl[[x]],row.names = NULL)
    dframe$series <- x
    dframe
  })
  
  tsdf <- do.call("rbind",out_list)
  if(is.null(fname)){
    fname <- "timeseriesdb_export_"
  } 
  fname <- paste0(fname,gsub("-","_",Sys.Date()))
  #write.csv2(tsdf,file = fname,row.names = F)
  #  reshape2::dcast(tsdf)
  if(cast){
    tsdf <- reshape2::dcast(tsdf,time ~ series)
  }
  
  # format the date
  if(!is.null(date_format)){
    PREV_LC_TIME_LOCALE <- Sys.getlocale("LC_TIME")
    Sys.setlocale("LC_TIME",ifelse(is.null(LC_TIME_LOCALE),
                                   PREV_LC_TIME_LOCALE,LC_TIME_LOCALE))
    tsdf$time <- format(tsdf$time,date_format)
    Sys.setlocale("LC_TIME",PREV_LC_TIME_LOCALE)
  } else{
    tsdf$time <- ifelse(frq == 12,format(tsdf$time,"%Y-%m"),stop("need to provide a date_format argument if frequency is not monthly"))
  }
  
  
  
  write.table(tsdf,file = paste0(fname,".csv"), row.names = F, dec = dec, sep = sep)
}
