######################################################
# Metadata
######################################################

library(microbenchmark)

meta_file <- "C:/dev/R_repos/swissdata/wd/ch.fso.besta.mjr/ch.fso.besta.mjr.yaml"

microbenchmark({
  meta_list <- meta_dt <- tstools::read_swissdata_meta(meta_file, as_list = TRUE)
  lapply(meta_list, jsonlite::toJSON, auto_unbox = TRUE)
  }, times = 50)

meta_to_json <- function(m) {
  dims <- setdiff(names(m), "ts_key")
  
  meta_fmt <- paste0("{", paste(sprintf('"%s": "%%s"', dims), collapse = ", "), "}")
  m[, .(ts_key, meta_data = do.call(sprintf, c(meta_fmt, .SD)))]
}

# DEFO the faster one!
microbenchmark({
  meta_dt <- tstools::read_swissdata_meta(meta_file)
  meta_to_json(meta_dt)
}, times  = 50)


######################################################
# Time series
######################################################

tsl <- tstools::generate_random_ts(lengths = 100)

# This one wins
microbenchmark({
  dates <- timeseriesdb:::indexToDate(stats::time(tsl$ts1))
  values <- tsl$ts1
  paste0('{"dates":', jsonlite::toJSON(dates), '}, "values":', jsonlite::toJSON(values))
})

microbenchmark({
  dates <- timeseriesdb:::indexToDate(stats::time(tsl$ts1))
  values <- as.numeric(tsl$ts1)
  vl <- as.list(values)
  names(vl) <- dates
  jsonlite::toJSON(vl, auto_unbox = TRUE)
})
