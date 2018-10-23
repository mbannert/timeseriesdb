#' Export Content of Meta Information Environment to Various File Formats
#' 
#' The idea of this function is to create a standalone meta information catalog. 
#' The catalog file can be used as a companion to illustrate time series exports from 
#' timeseriesdb. Note that this function imports functionality from other packages such as 
#' data.table and openxlsx.
#' 
#' @param meta_env meta\_env environment object. 
#' @param fname character file name including file extension. If set to NULL no file 
#' is export. The resulting data.frame is just displayed on the console in this case. Defaults to NULL.
#' @param export_type character indication which file format should be exported.
#' "pdf","tex","csv" are the eligible.
#' @param flexcols which columns shoukd be kept in the data.frame. Defaults to NULL, using all columns. 
#' @param row.names logical should row.names be displayed in csv. 
#' @param sep character seperator
#' @param overwrite should existing files be overwritten? Defaults to TRUE.
#' @importFrom data.table rbindlist
#' @importFrom openxlsx write.xlsx
#' @export
exportMetaEnv <- function(meta_env,fname = NULL,export_type = "pdf",flexcols = NULL,
                          row.names = F, sep=";",overwrite = T){
  if(!inherits(meta_env,"meta_env")) stop("object not a timeseriesdb meta information environment.")
  
  flex <- lapply(meta_env,"[[","flexible") 
  # address fixed meta information later on 
  # do not need it right now, fixed part should also contain frequency then !?
  # fix <- lapply(meta_env,"[[","fixed") 
  
  nms <- names(flex)
  all_names <- unique(unlist(lapply(flex,"[[","key")))
  
  
  flex_out_list <- lapply(nms,function(x){
    dframe <- flex[[x]]
    if(ncol(dframe) != 0){
      tdf <- data.frame(t(dframe[,2]))
      colnames(tdf) <- dframe[,1]
      outdf <- cbind(key = x,tdf)
    } else{
      edf <- data.frame(t(rep(NA,length(all_names))))
      names(edf) <- all_names
      outdf <- cbind(key = x,edf)
      outdf
    }
  })
  
  if(is.null(flexcols)){
    wtf <- data.table::rbindlist(flex_out_list,fill = TRUE)  
  } else {
    wtf <- data.table::rbindlist(flex_out_list,fill = TRUE)  
    wtf <- as.data.frame(wtf)
    wtf <- wtf[,flexcols]
  }
  
  if(is.null(fname)){
    wtf
  } else {
    
    message("Timeseriesdb itself does not depend on openxlsx. The xlsx export does. Make sure the package is installed \n
            in order to export .xlsx files though. You don't need to use library / require, having installed the package will be \n
            sufficient to export xlsx files.")
    
    if(export_type == "xlsx"){
      
      openxlsx::write.xlsx(as.data.frame(wtf),fname,overwrite = overwrite)
    }
    
    if(export_type == "csv") {
      message("Writing .csv file...")
      utils::write.table(wtf,file=fname,sep=sep,row.names = row.names)  
    } 
    
    if(export_type == "pdf"){
      # might want to add pdf rendering within 
      xtable::xtable(as.data.frame(wtf))
      
    }
  }
  
}
