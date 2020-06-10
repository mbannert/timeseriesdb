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

tsl_pblc <- list(
  tsp = ts(rep(3, 10), 2019, frequency = 4)
)
class(tsl_pblc) <- c("tslist", "list")

if(is_test_db_reachable()) {
  con_admin <- connect_to_test_db()
  con_reader_public <- connect_to_test_db("dev_reader_public")
  con_reader_main <- connect_to_test_db("dev_reader_main")

  reset_db(con_admin)

  current_date <- dbGetQuery(con_admin, "SELECT CURRENT_DATE")$current_date

  # TODO: This would be more robust if we took charge of what time it is entirely (on db and here)
  store_time_series(con_admin,
                    tsl_state_0,
                    "timeseries_access_main",
                    valid_from = current_date - 4,
                    release_date = current_date - 4)
  store_time_series(con_admin,
                    tsl_state_1,
                    "timeseries_access_main",
                    valid_from = current_date - 3,
                    release_date = current_date - 1)
  store_time_series(con_admin,
                    tsl_state_2,
                    "timeseries_access_main",
                    valid_from = current_date - 1,
                    release_date = current_date + 2)
  store_time_series(con_admin,
                    tsl_state_2,
                    "timeseries_access_main",
                    valid_from = current_date - 1,
                    release_date = current_date + 2)
  store_time_series(con_admin,
                    tsl_state_2_v2,
                    "timeseries_access_main",
                    valid_from = current_date + 1,
                    release_date = current_date + 2)

  store_time_series(con_admin,
                    tsl_pblc,
                    "timeseries_access_public")
}

test_that("public reader may not read main series", {
  skip_on_cran()
  skip_if_not(is_test_db_reachable())

  tsl_read <- read_time_series(con_reader_public, "ts1")
  expect_length(tsl_read, 0)
})

test_that("series with no access get skipped", {
  skip_on_cran()
  skip_if_not(is_test_db_reachable())

  tsl_read <- read_time_series(con_reader_public, c("ts1", "tsp"))
  expect_equal(tsl_read, tsl_pblc)
})

test_that("by default it reads the most recent valid vintage", {
  skip_on_cran()
  skip_if_not(is_test_db_reachable())

  tsl_read <- read_time_series(con_reader_main, "ts1")
  expect_equal(tsl_read, tsl_state_2)
})

test_that("by default it reads the most recent valid vintage but with respecting rls date", {
  skip_on_cran()
  skip_if_not(is_test_db_reachable())

  tsl_read <- read_time_series(con_reader_main, "ts1", respect_release_date = TRUE)
  expect_equal(tsl_read, tsl_state_1)
})

test_that("reading desired vintages works", {
  skip_on_cran()
  skip_if_not(is_test_db_reachable())

  tsl_read_1 <- read_time_series(con_reader_main, "ts1", valid_on = current_date - 4)
  expect_equal(tsl_read_1, tsl_state_0)

  tsl_read_2 <- read_time_series(con_reader_main, "ts1", valid_on = current_date - 2)
  expect_equal(tsl_read_2, tsl_state_1)
})

test_that("reading vintages, respecting release date", {
  skip_on_cran()
  skip_if_not(is_test_db_reachable())

  tsl_read <- read_time_series(con_reader_main, "ts1", valid_on = current_date - 2, respect_release_date = TRUE)
  expect_equal(tsl_read, tsl_state_1)
})

test_that("reading via regex works", {
  skip_on_cran()
  skip_if_not(is_test_db_reachable())

  tsl_read <- read_time_series(con_reader_main, "^ts", regex = TRUE)
  expect_setequal(names(tsl_read), c("ts1", "tsp"))
})
