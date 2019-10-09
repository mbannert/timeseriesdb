suppressWarnings(library(mockery))


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
  store_ts_json <- mock()
  with_mock(
    store_records = store_ts_json,
    {
      store_time_series("con", tsl)
      expect_called(store_ts_json, 1)
      expect_args(store_ts_json, 1, "con", to_ts_json(tsl), NULL, NULL, "timeseries_main", TRUE, "timeseries")
    }
  )
})

test_that("it subsets the list", {
  store_ts_json <- mock()
  with_mock(
    store_records = store_ts_json,
    {
      xx <- store_time_series("con", tsl, "ts2")
      only_ts2 <- tsl[2]
      expect_args(store_ts_json, 1, "con", to_ts_json(only_ts2), NULL, NULL, "timeseries_main", TRUE, "timeseries")
    }
  )
})

test_that("it handles empty lists", {
  tsl <- list()
  class(tsl) <- c("tslist", "list")
  expect_message(xx <- store_time_series("con", tsl), "No time series in subset")
  expect_equal(xx, list())
})

test_that("it handles non-ts-likes", {
  local_tsl <- tsl
  local_tsl$not_a_ts <- "Mwahahaha!"
  store_ts_json <- mock()
  with_mock(
    store_records = store_ts_json,
    {
      expect_message(store_time_series("con", local_tsl), "no valid time series objects.*not_a_ts")
    }
  )
})


# store time series from data.table  ##########################

context("store_time_series.data.table")

test_that("it calls through to store_records", {
  store_ts_json <- mock()
  with_mock(
    store_records = store_ts_json,
    {
      store_time_series("con", dt)
      expect_called(store_ts_json, 1)
      expect_args(store_ts_json, 1, "con", to_ts_json(dt), NULL, NULL, "timeseries_main", TRUE, "timeseries")
    }
  )
})
