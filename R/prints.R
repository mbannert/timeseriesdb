#' @export 
print.SQL <- function(x, ...){
  cat(gsub("\n[ \t]+","\n",x))
}

#' @export 
print.tsmeta <- function(x, ...) {
  atts <- attributes(x)
  cat(sprintf("Time series metadata%s\n", ifelse(!is.null(atts$locale), sprintf(" (%s)", atts$locale), "")))
  n <- names(x)
  name_lengths <- sapply(n, nchar)
  max_name_length <- max(name_lengths)
  for(i in n) {
    cat(sprintf("%s%s: %s\n", i, paste(rep(" ", max_name_length - name_lengths[i]), collapse = ""), x[[i]]))
  }
}

#' @export 
print.tsmeta.list <- function(x, ...) {
  atts <- attributes(x)
  cat(sprintf("A tsmeta.list object%s\n", ifelse(!is.null(atts$locale), sprintf(" (%s)", atts$locale), "")))
  print(as.data.table(as.tsmeta.dt(x)))
}

#' @export 
print.tsmeta.dt <- function(x, ...) {
  atts <- attributes(x)
  cat(sprintf("A tsmeta.dt object%s\n", ifelse(!is.null(atts$locale), sprintf(" (%s)", atts$locale), "")))
  print(as.data.table(x))
}
