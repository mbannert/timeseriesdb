connect_to_test_db <- function() {
  dbConnect(Postgres(), "postgres", "localhost", 1111, "", "postgres", bigint = "integer")
}

# TODO: see ?dbCanConnect
is_test_db_reachable <- function(){
  tryCatch({
    con <- connect_to_test_db()
    dbDisconnect(con)
    TRUE
  },
  error = function(e) { FALSE }
  )
}

reset_db <- function(con, remove_default_set = FALSE) {
  dbExecute(con, "DELETE FROM timeseries.timeseries_main")
  dbExecute(con, "DELETE FROM timeseries.catalog")
  dbExecute(con, "DELETE FROM timeseries.datasets")
  if(!remove_default_set) {
    dbExecute(con, "INSERT INTO timeseries.datasets VALUES ('default', 'A set that is used if no other set is specified. Every time series needs to be part of a dataset', NULL);")
  }
}