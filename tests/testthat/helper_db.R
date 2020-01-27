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

reset_db <- function(con) {
  dbExecute(con, "DELETE FROM timeseries.timeseries_main")
  dbExecute(con, "DELETE FROM timeseries.catalog")
  dbExecute(con, "DELETE FROM timeseries.datasets")
}

prepare_db <- function(con,
                       init_datasets = FALSE,
                       init_catalog = FALSE) {

  datasets <- data.frame(
    set_id = c(
      "set1",
      "set2"
    ),
    set_description = c(
      "test set 1",
      "test set 2"
    ),
    set_md = c(
      '{"testno": 1}',
      '{"testno": 2}'
    )
  )

  catalog <- data.frame(
    ts_key = c(
      "ts1",
      "ts2",
      "ts3",
      "ts4",
      "ts5"
    ),
    set_id = c(
      "set1",
      "set1",
      "set2",
      "set2",
      "default"
    )
  )

  reset_db(con)
  if(init_datasets) {
    dbWriteTable(con,
                 DBI::Id(schema = "timeseries", table = "datasets"),
                 datasets,
                 append = TRUE)

    if(init_catalog) {
      dbWriteTable(con,
                   DBI::Id(schema = "timeseries", table = "catalog"),
                   catalog,
                   append = TRUE)
    }
  }
}

test_with_fresh_db <- function(description, code, hard_reset = FALSE) {
  skip_on_cran()
  skip_if_not(is_test_db_reachable())

  prepare_db(con, !hard_reset, !hard_reset)

  test_that(description, code)
}
