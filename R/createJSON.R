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
  
  out <- data[, .(ts_key, meta_data = do.call(sprintf, c(meta_fmt, .SD[, -"ts_key"])))]
  
  out[, meta_data := pgEscape(meta_data)] 
  
  out
}

#' @export
createJSON.list <- function(data) {
  do.call(sprintf, c(list(create_meta_format(names(data))), pgEscape(as.character(unlist(data)))))
}

create_meta_format <- function(dims) {
  paste0("{", paste(sprintf('"%s": "%%s"', dims), collapse = ", "), "}")
}

# Feel free to add to your heart's content
pgEscape <- function(x) {
  out <- gsub("\n", "\\\\n", x)
  out <- gsub("\r", "\\\\r", out)
  out <- gsub("\"", "\\\"", out)
  out
}
