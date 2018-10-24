#' @export
print.meta_env <- function(x,...){
  env_name <- deparse(substitute(x))
  obj_count <- length(ls(envir = x))
  out <- sprintf('%s contains %s meta information object(s).',
                 env_name,obj_count)
  cat(out)
  
}

#' @export
print.tsmeta.dt <- function(x) {
  message("thems a tsmeta.dt!")
  class(x) <- class(x)[2:length(class(x))]
  print(x)
}

#' @export
print.tsmeta.list <- function(x) {
  message("smells like a tsmeta.list")
  class(x) <- "list"
  print(x)
}

#' @export
print.tsmeta <- function(x) {
  nam <- names(x)
  for(i in seq_along(x)) {
    cat(nam[i])
    cat("\n")
    cat(x[[i]])
    cat("\n\n")
  }
  NULL
}