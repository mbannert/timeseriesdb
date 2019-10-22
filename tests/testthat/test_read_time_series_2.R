# And now for something completely tricky... *liberty bell*

context("read_time_series, case 2")

con <- NULL
if(is_test_db_reachable()) {
  con <- connect_to_test_db()
}

reset_db(con)

tsl_state_0 <- list(
  ts1 = ts(rep(1.9, 10), 2019, frequency = 4)
)
class(tsl_state_0) <- c("tslist", "list")

tsl_state_1 <- list(
  ts1 = ts(rep(2, 10), 2019, frequency = 4)
)
class(tsl_state_1) <- c("tslist", "list")

tsl_state_2 <- list(
  ts1 = ts(rep(2.1, 10), 2019, frequency = 4)
)
class(tsl_state_2) <- c("tslist", "list")

tsl_state_2_v2 <- list(
  ts1 = ts(rep(2.1415926, 10), 2019, frequency = 4)
)
class(tsl_state_2_v2) <- c("tslist", "list")

# TODO: This would be more robust if we took charge of what time it is entirely (on db and here)
store_time_series(con,
                  tsl_state_0,
                  "ts for testing read case 2", "public",
                  valid_from = Sys.Date() - 4,
                  release_date = Sys.Date() - 4)
store_time_series(con,
                  tsl_state_1,
                  "second version of ts for testing read case 2", "public",
                  valid_from = Sys.Date() - 3,
                  release_date = Sys.Date() - 1)
store_time_series(con,
                  tsl_state_2,
                  "second revision of second version ot ts for testing read case 2",
                  "public",
                  valid_from = Sys.Date() - 1,
                  release_date = Sys.Date() + 2)
store_time_series(con,
                  tsl_state_2_v2,
                  "second revision of second version ot ts for testing read case 2",
                  "public",
                  valid_from = Sys.Date() + 1,
                  release_date = Sys.Date() + 2)

test_that("by default it reads the most recend valid vintage", {
  skip_on_cran()
  skip_if_not(is_test_db_reachable())
  
  tsl_read <- read_time_series(con, "ts1")
  expect_equal(tsl_read, tsl_state_2)
})

test_that("by default it reads the most recend valid vintage but with respecting rls date", {
  skip_on_cran()
  skip_if_not(is_test_db_reachable())
  
  tsl_read <- read_time_series(con, "ts1", respect_release_date = TRUE)
  expect_equal(tsl_read, tsl_state_1)
})

test_that("reading desired vintages works", {
  skip_on_cran()
  skip_if_not(is_test_db_reachable())

  tsl_read_1 <- read_time_series(con, "ts1", valid_on = Sys.Date() - 4)
  expect_equal(tsl_read_1, tsl_state_0)

  tsl_read_2 <- read_time_series(con, "ts1", valid_on = Sys.Date() - 2)
  expect_equal(tsl_read_2, tsl_state_1)
})

test_that("reading vintages, respecting release date", {
  skip_on_cran()
  skip_if_not(is_test_db_reachable())
  
  tsl_read <- read_time_series(con, "ts1", valid_on = Sys.Date() - 2, respect_release_date = TRUE)
  expect_equal(tsl_read, tsl_state_0)
})