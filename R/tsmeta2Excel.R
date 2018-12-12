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
  li_names <- names(li)
  n <- length(li)
  if(is.null(li_names)) {
    names(li) <- sprintf("meta_data%d", 1:n)
  } else if(any(li_names_empty <- sapply(li_names, nchar) == 0)) {
    li_names[li_names_empty] <- sprintf("meta_data%d", (1:n)[li_names_empty])
    names(li) <- li_names
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
