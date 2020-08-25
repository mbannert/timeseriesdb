library(stringr)
library(microbenchmark)

schema <- "timeseries_1_0"
tbl <- "timeseries_main"

microbenchmark(
  sprintf("INSERT INTO %s.%s", schema, tbl),
  str_interp("INSERT INTO ${schema}.${tbl}")
)

# And the winner is... sprintf!
