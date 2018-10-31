#' @export 
print.SQL <- function(x, ...){
  cat(gsub("\n[ \t]+","\n",x))
}

#' @export 
print.tsmeta <- function(x, ...) {
  cat("That be a tsmeta object.")
}

#' @export 
print.tsmeta.list <- function(x, ...) {
  cat("A tsmeta.list object\n")
  print(as.data.table(as.tsmeta.dt(x)))
}

#' @export 
print.tsmeta.dt <- function(x, ...) {
  cat("tsmeta DT! tsmeta DT!")
}