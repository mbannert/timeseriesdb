context("read_time_series, case 4")

con <- NULL
if(is_test_db_reachable()) {
  con <- connect_to_test_db()
}

reset_db(con)

tsl_state_1 <- list(
  ts1 = ts(rep(4, 10), 2019, frequency = 4)
)
class(tsl_state_1) <- c("tslist", "list")

tsl_state_2 <- list(
  ts1 = ts(rep(4.1, 10), 2019, frequency = 4)
)
class(tsl_state_2) <- c("tslist", "list")

store_time_series(con, tsl_state_1, "ts for testing read case 4", "public", release_date = "1900-01-01")
store_time_series(con, tsl_state_2, "second version of ts for testing read case 4", "public", release_date = Sys.Date() + 1)

test_that("it does not respect release date by default", {
  skip_on_cran()
  skip_if_not(is_test_db_reachable())
  
  tsl_read <- read_time_series(con, "ts1")
  expect_equal(tsl_read, tsl_state_2)
})

test_that("it returns the proper version when respecting release date", {
  skip_on_cran()
  skip_if_not(is_test_db_reachable())
  
  tsl_read <- read_time_series(con, "ts1", respect_release_date = TRUE)
  expect_equal(tsl_read, tsl_state_1)
})