context("store_records general tests")

tsl <- list(
  ts1 = ts(1:4, 2019, frequency = 12),
  ts2 = ts(1:5, 2019, frequency = 4)
)
class(tsl) <- c("tslist", "list")

con <- NULL
if(is_test_db_reachable()) {
  con <- connect_to_test_db()
}

test_that("vintage id is a uuid", {
  skip_on_cran()
  skip_if_not(is_test_db_reachable())

  reset_db(con)

  store_time_series(con, tsl, access = "timeseries_access_public", valid_from = "2019-01-01")

  out <- dbGetQuery(con, "SELECT * FROM timeseries.timeseries_main")

  expect_match(out$id, "[0-9a-fA-F]{8}\\-[0-9a-fA-F]{4}\\-[0-9a-fA-F]{4}\\-[0-9a-fA-F]{4}\\-[0-9a-fA-F]{12}")
})

test_that("it can deal with string valid_from", {
  skip_on_cran()
  skip_if_not(is_test_db_reachable())

  reset_db(con)

  expect_error(store_time_series(con, tsl, access = "timeseries_access_public", valid_from = "2019-01-01"),
               NA)
})

test_that("it can deal with string release_date", {
  skip_on_cran()
  skip_if_not(is_test_db_reachable())

  reset_db(con)

  expect_error(store_time_series(con, tsl, access = "timeseries_access_public", release_date = "2019-01-01"),
               NA)
})
