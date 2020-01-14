# TODO: tests for default args

tsl <- list(
  ts1 = ts(1:2, 2019, frequency = 12),
  ts2 = ts(3:4, 2019, frequency = 12)
)
class(tsl) <- c("tslist", "list")

dt <- data.table(
  id = c("ts1", "ts2"),
  time = seq(as.Date("2019-01-01"), length.out = 2, by = "1 month"),
  value = 1:4
)


# store time series from lists  ##########################
context("store_time_series.tslist")

test_that("it calls through to store_records", {
  store_recs <- mock()
  with_mock(
    # https://github.com/r-lib/testthat/issues/734#issuecomment-377367516
    "timeseriesdb:::store_records" = store_recs,
    "timeseriesdb:::to_ts_json" = mock("ts_json"), # Oh how I love isolating units under test. You are going to die all alone, little function...
    {
      store_time_series(
        "con",
        tsl,
        "access",
        "valid_from",
        "release_date",
        "schema",
        "chunksize"
      )
      
      expect_called(store_recs, 1)
      
      expect_args(
        store_recs,
        1,
        "con",
        "ts_json",
        "access",
        "timeseries_main",
        "valid_from",
        "release_date",
        "schema",
        "chunksize"
      )
    }
  )
})

test_that("it handles empty lists", {
  tsl <- list()
  class(tsl) <- c("tslist", "list")
  expect_warning(xx <- store_time_series("con", tsl), "no-op")
  expect_equal(xx, list())
})

test_that("it handles non-ts-likes", {
  local_tsl <- tsl
  local_tsl$not_a_ts <- "Mwahahaha!"
  store_recs <- mock()
  with_mock(
    "timeseriesdb:::store_records"= store_recs,
    {
      expect_message(
        store_time_series("con", local_tsl, "release", "access"),
        "no valid time series objects.*not_a_ts"
      )
    })
})


# # store time series from data.table  ##########################

context("store_time_series.data.table")

test_that("it calls through to store_records", {
  store_recs <- mock()
  with_mock(
    "timeseriesdb:::store_records" = store_recs,
    "timeseriesdb:::to_ts_json" = mock("ts_json"),
    {
      store_time_series(
        "con",
        dt,
        "access",
        "valid_from",
        "release_date",
        "schema",
        "chunksize"
      )

      expect_called(store_recs, 1)

      expect_args(
        store_recs,
        1,
        "con",
        "ts_json",
        "access",
        "timeseries_main",
        "valid_from",
        "release_date",
        "schema",
        "chunksize"
      )
    }
  )
})

test_that("it handles empty lists", {
  dt <- data.table(id = numeric(), time = numeric(), value = numeric())
  expect_warning(xx <- store_time_series("con", dt, "release", "access"), "no-op")
  expect_equal(xx, list())
})

test_that("it complains when it gets a non-ts_dt", {
  dt <- data.table(gronkh = numeric(), knight_in_shining_armour = numeric())
  expect_error(store_time_series("con", dt, "release", "access"))
})