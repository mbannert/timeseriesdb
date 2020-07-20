#' @importFrom jsonlite toJSON unbox
#' @importFrom stats frequency time is.ts
to_ts_json <- function(x, ...){
  UseMethod("to_ts_json")
}

to_ts_json.tslist <- function(x, ...){
  l <- lapply(x, function(xx) {
    toJSON(
      list(
        frequency = if(is.ts(xx)) { unbox(frequency(xx)) } else { unbox(NA) },
        time = index_to_date(time(xx), as.string = TRUE),
        value = c(unclass(xx))
      ),
      digits = NA
    )
  })
  class(l) <- "ts_json"
  l
}

#' @import data.table
to_ts_json.data.table <- function(x, ...){
  if(!"freq" %in% names(x)) {
    # Syntactically correct: it is NA. Also jsonlite translates NA into null. Neat!
    x[, freq := NA]
  }

  dt <- x[, .(
    json = list(
      toJSON(
        list(
          frequency = unbox(freq[1]),
          time = time,
          value = value
        ),
        digits = NA
      )
    )
  ), by = "id"]
  out <- dt$json
  class(out) <- "ts_json"
  names(out) <- dt[, id]
  out
}
