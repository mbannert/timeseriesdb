context("read_time_series, case 1")

tsl_state_1 <- list(
  ts1 = ts(rep(1, 10), 2019, frequency = 4)
)
class(tsl_state_1) <- c("tslist", "list")

tsl_state_2 <- list(
  ts1 = ts(rep(1.1, 10), 2019, frequency = 4)
)
class(tsl_state_2) <- c("tslist", "list")

con <- NULL
if(is_test_db_reachable()) {
  con <- connect_to_test_db()
  reset_db(con)
  store_time_series(con, tsl_state_1, "ts for testing read case 1", "public", valid_from = "1945-09-02")
  store_time_series(con, tsl_state_2, "second version of ts for testing read case 1", "public", valid_from = "1988-08-11")
}

test_that("by default it reads the most recend vintage", {
  skip_on_cran()
  skip_if_not(is_test_db_reachable())

  tsl_read <- read_time_series(con, "ts1")
  expect_equal(tsl_read, tsl_state_2)
})

test_that("reading desired vintages works", {
  skip_on_cran()
  skip_if_not(is_test_db_reachable())

  tsl_read_1 <- read_time_series(con, "ts1", valid_on = "1960-01-01")
  expect_equal(tsl_read_1, tsl_state_1)

  tsl_read_2 <- read_time_series(con, "ts1", valid_on = "2019-10-22")
  expect_equal(tsl_read_2, tsl_state_2)
})