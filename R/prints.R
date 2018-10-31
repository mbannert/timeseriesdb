#' @export 
print.SQL <- function(x, ...){
  cat(gsub("\n[ \t]+","\n",x))
}

#' @export 
print.tsmeta <- function(x, ...) {
  cat("Time series metadata\n")
  n <- names(x)
  name_lengths <- sapply(n, nchar)
  max_name_length <- max(name_lengths)
  for(i in n) {
    cat(sprintf("%s%s: %s\n", i, paste(rep(" ", max_name_length - name_lengths[i]), collapse = ""), x[[i]]))
  }
}

#' @export 
print.tsmeta.list <- function(x, ...) {
  cat("A tsmeta.list object\n")
  print(as.data.table(as.tsmeta.dt(x)))
}

#' @export 
print.tsmeta.dt <- function(x, ...) {
  cat("A tsmeta.dt object\n")
  print(as.data.table(x))
}
