#' Export A List of Time Series to CSV
#' 
#' Export a List of time series to semi-colon separated csv file. Typically multiple time series are exported as long format (melted format) files. 
#' 
#' @param tl list of time series
#' @param fname character file name. If set to NULL a standard file name chunk + Sys.Date is used.
#' @param auto_date logical should date automatically be appended to file name? Defaults to TRUE.
#' @param cast logical. Should the resulting data.frame be cast to wide format? Defaults to TRUE.
#' @param xlsx logical. Should data be exported to .xlsx? Defaults to FALSE.
#' @param sep character that separates columns in .csv
#' @param dec character that separates decimals in .csv
#' @param LC_TIME_LOCALE character time locale that differs from the standard locale. e.g. en_US.UTF-8. Defaults to NULL and uses the standard locale then. 
#' @param date_format character denotes the date format. Defaults to NULL. If set to null the default is used: Jan 2010. In combination with LC\_TIME\_Locale various international date formats can be produced. 
#' @param timeAsX logical should time be put to the x-axis of the spreadsheet?
#' Defaults to FALSE.
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
exportTsList <- function(tl,fname = NULL,
                         auto_date = T,
                         cast = T, xlsx = F,
                         sep = ";",dec=".",
                         LC_TIME_LOCALE = NULL,
                         date_format = NULL,
                         timeAsX = FALSE){
  
  # set file name chunk for the export file
  if(is.null(fname)){
    fname <- "timeseriesdb_export"
  } 
  fname <- paste0(fname,"_",
                  ifelse(auto_date,
                         gsub("-","_",Sys.Date()),
                         NULL))
  
  # check if all series got some frequencies 
  # other we can't export, maybe we don't even need this anymore... 
  # double check with next revision... 
  frq <- unique(sapply(tl,frequency))
  if(length(frq) != 1) stop("All time series of within a list need to have the same frequency for proper export.")

  tl <- lapply(tl,function(x) {
    xtsout <- xts::as.xts(x)
    names(xtsout) <- "value"
    xtsout
    })
  out_list <- lapply(names(tl),function(x){
    dframe <- data.frame(time = time(tl[[x]]),
                         value = tl[[x]],row.names = NULL)
    dframe$series <- x
    dframe
  })
  
  tsdf <- do.call("rbind",out_list)

  
  # re format Date now before reshaping... 
  # cause reshaping uses dates in col names etc. 
  # format the date
  if(!is.null(date_format)){
    PREV_LC_TIME_LOCALE <- Sys.getlocale("LC_TIME")
    Sys.setlocale("LC_TIME",ifelse(is.null(LC_TIME_LOCALE),
                                   PREV_LC_TIME_LOCALE,LC_TIME_LOCALE))
    tsdf$time <- format(tsdf$time,date_format)
    Sys.setlocale("LC_TIME",PREV_LC_TIME_LOCALE)
  } else{
    
    # DO NOT USE ifelse() here because it returns a 'scalar' 
    # that will blow reformatting up... 
    # wanna experience an R WTF try sdf$time <- ifelse(frq == 12, format(tsdf$time, "%Y-%m"),stop("some message")) 
    if(frq == 12){
      tsdf$time <- format(tsdf$time,"%Y-%m")
    } else {
      stop("need to provide a date_format argument if frequency is not monthly")
    }
  }
  
  
  # Now shape the data... 
  # want to have time on the X axis??
  if(timeAsX){
    # time on the X axis
    tsdf <- reshape(tsdf,timevar = "time",
                    idvar = "series",
                    direction="wide")
    names(tsdf) <- gsub("value.","",names(tsdf))
    
    if(xlsx){
      openxlsx::write.xlsx(tsdf,paste0(fname,".xlsx"))
    } else{
      write.table(tsdf,file = paste0(fname,".csv"), row.names = F, dec = dec, sep = sep)  
    }
  } else {
    if(cast){
      tsdf <- reshape(tsdf,
                      idvar="time",
                      timevar = "series",
                      direction="wide")
      names(tsdf) <- gsub("value.","",names(tsdf))
    }  
  }
  
  if(xlsx){
    openxlsx::write.xlsx(tsdf,paste0(fname,".xlsx"))
  } else{
    write.table(tsdf,file = paste0(fname,".csv"), row.names = F, dec = dec, sep = sep)  
  }
  
  
  
  
}
