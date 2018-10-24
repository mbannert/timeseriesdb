#' @export
as.tsmeta.dt <- function(meta) {
  UseMethod("as.tsmeta.dt")
}

#' @export
as.tsmeta.dt.tsmeta.list <- function(meta_list) {
  out <- rbindlist(meta_list, fill = TRUE)
  minlength <- min(sapply(meta_list, length))
  if(ncol(out) != minlength) {
    warning("Fill-in occurred, not all fields were present in all meta data items!")
  }
  out[, ts_key := names(meta_list)]
  class(out) <- c("tsmeta.dt", class(out))
  out
}

#' @export
as.tsmeta.dt.list <- function(meta) {
  as.tsmeta.dt(as.tsmeta.list(meta))
}

#' @export
as.tsmeta.dt.data.frame <- function(meta) {
  meta <- as.data.table(meta)
  class(meta) <- c("tsmeta.dt", class(meta))
  meta
}

#' @export
as.tsmeta.dt.meta_env <- function(meta) {
  as.tsmeta.dt(as.list(meta))
}

#' @export
as.tsmeta.dt.tsmeta.dt <- identity

#' @export
as.tsmeta.list <- function(meta) {
  UseMethod("as.tsmeta.list")
}

#' @export
as.tsmeta.list.tsmeta.dt <- function(meta) {
  out <- lapply(split(meta, by = "ts_key"), function(x) {
    as.list(x[, -"ts_key"])
  })
  # Remove NA elements from list
  out <- lapply(out, function(x){x[!is.na(x)]})
  as.tsmeta.list.list(out)
}

#' @export
as.tsmeta.list.list <- function(meta) {
  if(getListDepth(meta) != 2) {
    stop("A meta list must have exactly depth 2!")
  }
  meta <- lapply(meta, function(x) {
    class(x) <- c("tsmeta", class(x))
    x
  })
  class(meta) <- c("tsmeta.list", class(meta))
  meta
}

#' @export
as.tsmeta.list.data.frame <- function(meta) {
  as.tsmeta.list(as.tsmeta.dt(meta))
}

#' @export
as.tsmeta.list.meta_env <- function(meta) {
  meta <- as.list(meta)
  meta <- lapply(meta, `class<-`, "list")
  as.tsmeta.list(meta)
}

#' @export
as.tsmeta.list.tsmeta.list <- identity