# And now for something completely tricky... *liberty bell*

context("read_time_series")

# TODO: move this into helper_db as a fixture
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

con <- NULL
if(is_test_db_reachable()) {
  con <- connect_to_test_db()
  reset_db(con)

  current_date <- dbGetQuery(con, "SELECT CURRENT_DATE")$current_date

  # TODO: This would be more robust if we took charge of what time it is entirely (on db and here)
  store_time_series(con,
                    tsl_state_0,
                    "public",
                    valid_from = current_date - 4,
                    release_date = current_date - 4)
  store_time_series(con,
                    tsl_state_1,
                    "public",
                    valid_from = current_date - 3,
                    release_date = current_date - 1)
  store_time_series(con,
                    tsl_state_2,
                    "public",
                    valid_from = current_date - 1,
                    release_date = current_date + 2)
  store_time_series(con,
                    tsl_state_2_v2,
                    "public",
                    valid_from = current_date + 1,
                    release_date = current_date + 2)
}

test_that("by default it reads the most recent valid vintage", {
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

  tsl_read_1 <- read_time_series(con, "ts1", valid_on = current_date - 4)
  expect_equal(tsl_read_1, tsl_state_0)

  tsl_read_2 <- read_time_series(con, "ts1", valid_on = current_date - 2)
  expect_equal(tsl_read_2, tsl_state_1)
})

test_that("reading vintages, respecting release date", {
  skip_on_cran()
  skip_if_not(is_test_db_reachable())

  tsl_read <- read_time_series(con, "ts1", valid_on = current_date - 2, respect_release_date = TRUE)
  expect_equal(tsl_read, tsl_state_1)
})

test_that("reading via regex works", {
  skip_on_cran()
  skip_if_not(is_test_db_reachable())

  tsl_read <- read_time_series(con, "^ts", regex = TRUE)
  expect_equal(names(tsl_read), "ts1")
})
