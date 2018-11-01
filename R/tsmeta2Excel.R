# TODO: Different formats?
# JSON:
# jsonlite::toJSON(as.tsmeta.list(x), pretty = TRUE, auto_unbox = TRUE)
# 
# CSV:
# write.csv(as.tsmeta.dt(x), row.names = FALSE, file = stdout())

#' @importFrom openxlsx write.xlsx
#' @export
writeTsmetaToExcel <- function(..., path) {
  li <- list(...)
  if(is.null(names(li))) {
    names(li) <- "meta_data"
  }
  li <- lapply(li, as.tsmeta.dt)
  
  write.xlsx(li, path)
  
  invisible(TRUE)
}

#' @importFrom openxlsx loadWorkbook read.xlsx
#' @export
readTsmetaFromExcel <- function(path) {
  wb <- loadWorkbook(path)
  
  sheetNames <- wb$sheet_names
  
  out <- lapply(sheetNames, function(x) {
    as.tsmeta.dt(read.xlsx(path, sheet = x))
  })
  names(out) <- sheetNames
  out
}
