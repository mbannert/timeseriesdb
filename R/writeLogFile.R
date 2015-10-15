#' Simple Log File Writer
#' 
#' Most simple log file writer just write steps of
#' a script to a text file. 
#'
#' @param filename character name of a textfile. Defaults to NULL.
#' @param msg log file message
#' @param line_end line end character 
#' @export
writeLogFile <- function(filename=NULL,msg,line_end = "\n"){
    if(is.null(filename)){
        filename <- paste0(Sys.Date,".log")
    }
    sink(filename,append=T)
    msg <- paste0(Sys.time(),":",msg,line_end)
    cat(msg)
    sink()
}