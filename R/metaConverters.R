as.meta.dt <- function(meta_list) {
  UseMethod("as.meta.dt")
}

as.meta.dt.meta.list <- function(meta_list) {
  out <- rbindlist(meta_list, fill = TRUE)
  if(ncol(out) != length(meta_list[[1]])) {
    warning("Fill-in occurred, not all fields were present in all meta data items!")
  }
  out[, ts_key := names(meta_list)]
  class(out) <- c("meta.dt", class(out))
  out
}

as.meta.dt.list <- function(meta) {
  as.meta.dt(as.meta.list(meta))
}

as.meta.dt.data.frame <- function(meta) {
  class(meta) <- c("meta.dt", class(meta))
  meta
}

as.meta.dt.meta.dt <- identity

as.meta.list <- function(meta) {
  UseMethod("as.meta.list")
}

as.meta.list.meta.dt <- function(meta) {
  out <- lapply(split(meta, by = "ts_key"), as.list)
  # Remove NA elements from list
  out <- lapply(out, function(x){x[!is.na(x)]})
  class(out) <- c("meta.list", class(out))
  out
}

as.meta.list.list <- function(meta) {
  if(getListDepth(meta) != 2) {
    stop("A meta list must have exactly depth 2!")
  }
  class(meta) <- c("meta.list", class(meta))
  meta
}

as.meta.list.data.frame <- function(meta) {
  as.meta.list(as.meta.dt(meta))
}

as.meta.list.meta.list <- identity