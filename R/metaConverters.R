#' @export
tsmeta.dt <- function(...) {
  as.tsmeta.dt(data.table(...))
}

#' @export
as.tsmeta.dt <- function(meta) {
  UseMethod("as.tsmeta.dt")
}

#' @export
as.tsmeta.dt.tsmeta.list <- function(meta_list) {
  if(length(meta_list) > 0) {
    meta_lengths <- sapply(meta_list, length)
    empty_metas <- meta_lengths == 0
    
    out <- rbindlist(meta_list, fill = TRUE, idcol = TRUE)[.(names(meta_list)), on = .(.id)]
    minlength <- min(meta_lengths[!empty_metas])
    if(ncol(out) != (minlength + 1)) {
      warning("Fill-in occurred, not all fields were present in all meta data items!")
    }
    setnames(out, ".id", "ts_key")
    class(out) <- c("tsmeta.dt", class(out))
    out
  } else {
    tsmeta.dt()
  }
}

#' @export
as.tsmeta.dt.list <- function(meta) {
  as.tsmeta.dt(as.tsmeta.list(meta))
}

#' @export
as.tsmeta.dt.data.frame <- function(meta) {
  meta <- as.data.table(meta)
  if(nrow(meta)) {
    setcolorder(meta,"ts_key")
  }
  class(meta) <- c("tsmeta.dt", class(meta))
  meta
}

#' @export
as.tsmeta.dt.tsmeta.dt <- identity

#' @export
tsmeta.list <- function(...) {
  as.tsmeta.list(list(...))
}

#' @export
as.tsmeta.list <- function(meta) {
  UseMethod("as.tsmeta.list")
}

#' @export
as.tsmeta.list.tsmeta.dt <- function(meta) {
  if(nrow(meta) > 0) {
    out <- lapply(split(meta, by = "ts_key"), function(x) {
      as.list(x[, -"ts_key"])
    })
    # Remove NA elements from list
    out <- lapply(out, function(x){x[!is.na(x)]})
    as.tsmeta.list.list(out)
  } else {
    tsmeta.list()
  }
}

#' @export
as.tsmeta.list.list <- function(meta) {
  if(getListDepth(meta) != 2 && length(meta) > 0) {
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
as.tsmeta.list.tsmeta.list <- identity
