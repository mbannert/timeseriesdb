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

#' @import data.table
to_ts_json.data.table <- function(x, ...){
  dt <- x[, .(json = list(toJSON(list(time = time, value = value)))), by = "id"]
  out <- dt$json
  class(out) <- "ts_json"
  names(out) <- dt[, id]
  out
}
