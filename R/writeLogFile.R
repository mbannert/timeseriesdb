#' Simple Log File Writer
#' 
#' Most simple log file writer just write steps of
#' a script to a text file. 
#'
#' @param msg log file message
#' @param filename character name of a textfile. Defaults to NULL.
#' @param line_end line end character 
#' @export
writeLogFile <- function(msg,filename=NULL,line_end = "\n"){
    if(is.null(filename)){
        filename <- paste0(Sys.Date(),".log")
    }
    sink(filename,append=T)
    msg <- paste0(Sys.time(),":",msg,line_end)
    cat(msg)
    sink()
}
