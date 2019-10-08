#' @importFrom jsonlite toJSON
to_ts_json <- function(x, ...){
  UseMethod("to_ts_json")
}

to_ts_json.tslist <- function(x, ...){
  l <- lapply(x, function(xx) {
    toJSON(
      list(
        time = index_to_date(time(xx), as.string = TRUE), 
        value = xx
      )
    )
  })
  class(l) <- "ts_json"
  l
}

to_ts_json.data.table <- function(x, ...){
  dt <- x[, .(json = toJSON(list(time = time, value = value))), by = "id"]
  out <- list(dt[, json])
  class(out) <- "ts_json"
  names(out) <- dt[, id]
  out
}