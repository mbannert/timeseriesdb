#' @export
createJSON <- function(data) {
  UseMethod("createJSON")
}

#' @export
createJSON.data.frame <- function(data) {
  # Check if we already have a proper DF
  if(length(setdiff(names(data), c("ts_key", "meta_data"))) == 0) {
    return(data)
  }
  
  dims <- setdiff(names(data), "ts_key")
  
  meta_fmt <- create_meta_format(dims)
<<<<<<< Updated upstream
  out <- data[, .(meta_data = do.call(sprintf, c(meta_fmt, .SD))), by = ts_key]
=======
  #out <- data[, .(meta_data = do.call(sprintf, c(meta_fmt, .SD))), by = ts_key]
  
  out <- data[, .(ts_key, meta_data = do.call(sprintf, c(meta_fmt, .SD[, -"ts_key"])))]
>>>>>>> Stashed changes
  
  # Strip out some chars causing problems
  out[, meta_data := gsub("\n", "", meta_data)]
  out[, meta_data := gsub("\"\"", "", meta_data)]
  out
}

#' @export
createJSON.list <- function(data) {
  do.call(sprintf, c(list(create_meta_format(names(data))), gsub("[\"\n]", "", as.character(unlist(data)))))
}

create_meta_format <- function(dims) {
  paste0("{", paste(sprintf('"%s": "%%s"', dims), collapse = ", "), "}")
}