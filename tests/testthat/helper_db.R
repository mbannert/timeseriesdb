connect_to_test_db <- function() {
  dbConnect(Postgres(), "postgres", "localhost", 1111, "pgpass", "postgres")
}

is_test_db_reachable <- function(){
  tryCatch({
    con <- connect_to_test_db()
    dbDisconnect(con)
    TRUE
  },
  error = function(e) { FALSE }
  )
}

reset_db <- function(con) {
  dbExecute(con, "DELETE FROM timeseries.timeseries_main")
  dbExecute(con, "DELETE FROM timeseries.releases")
}