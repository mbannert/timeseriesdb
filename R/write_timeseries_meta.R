write_timeseries_meta <- function(..., path) {
  li <- list(...)
  li <- lapply(li, as.tsmeta.dt)
  
  write.xlsx(li, path)
  
  invisible(TRUE)
}

read_timeseries_meta <- function(path) {
  wb <- loadWorkbook(path)
  
  sheetNames <- wb$sheet_names
  
  out <- lapply(sheetNames, function(x) {
    as.tsmeta.dt(read.xlsx(path, sheet = x))
  })
  names(out) <- sheetNames
  out
}
