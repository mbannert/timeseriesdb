# TODO: tests for default args

tsl <- list(
  ts1 = ts(1:2, 2019, frequency = 12),
  ts2 = ts(3:4, 2019, frequency = 12)
)
class(tsl) <- c("tslist", "list")

dt <- data.table(
  id = rep(c("ts1", "ts2"), each = 2),
  time = seq(as.Date("2019-01-01"), length.out = 2, by = "1 month"),
  value = 1:4
)

ts <- tsl$ts1

xts <- xts::as.xts(ts)

# store time series from lists  ##########################
test_that("it calls through to store_records", {
  
  store_recs <- mock()
  with_mock(
    # https://github.com/r-lib/testthat/issues/734#issuecomment-377367516
    "timeseriesdb:::store_records" = store_recs,
    "timeseriesdb:::to_ts_json" = mock("ts_json"), # Oh how I love isolating units under test. You are going to die all alone, little function...
    {
      db_ts_store(
        "con",
        tsl,
        "access",
        "valid_from",
        "release_date",
        "pre_release_access",
        "schema"
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
        "pre_release_access",
        "schema"
      )
    }
  )
})


test_that("defaults are passed on correctly", {
  store_recs <- mock()
  with_mock(
    "timeseriesdb:::store_records" = store_recs,
    "timeseriesdb:::to_ts_json" = mock("ts_json"), # Oh how I love isolating units under test. You are going to die all alone, little function...
    {
      db_ts_store(
        "con",
        tsl
      )

      expect_called(store_recs, 1)

      expect_args(
        store_recs,
        1,
        "con",
        "ts_json",
        NULL,
        "timeseries_main",
        NULL,
        NULL,
        NULL,
        "timeseries"
      )
    }
  )
})

test_that("it handles empty lists", {
  tsl <- list()
  class(tsl) <- c("tslist", "list")
  expect_warning(xx <- db_ts_store("con", tsl), "no-op")
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
        db_ts_store("con", local_tsl, "release", "access"),
        "no valid time series objects.*not_a_ts"
      )
    })
})

test_that("It handles duplicate names", {
  tsl_local <- tsl
  names(tsl_local) <- c("tsa", "tsa")
  expect_error(
    db_ts_store("con", tsl_local, "release", "access"),
    "duplicate keys"
  )
})

# # store time series from data.table  ##########################

context("db_ts_store.data.table")

test_that("it calls through to store_records", {
  store_recs <- mock()
  with_mock(
    "timeseriesdb:::store_records" = store_recs,
    "timeseriesdb:::to_ts_json" = mock("ts_json"),
    {
      db_ts_store(
        "con",
        dt,
        "access",
        "valid_from",
        "release_date",
        "pre_release_access",
        "schema"
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
        "pre_release_access",
        "schema"
      )
    }
  )
})


test_that("defaults are passed on correctly", {
  store_recs <- mock()
  with_mock(
    "timeseriesdb:::store_records" = store_recs,
    "timeseriesdb:::to_ts_json" = mock("ts_json"),
    {
      db_ts_store(
        "con",
        dt
      )

      expect_called(store_recs, 1)

      expect_args(
        store_recs,
        1,
        "con",
        "ts_json",
        NULL,
        "timeseries_main",
        NULL,
        NULL,
        NULL,
        "timeseries"
      )
    }
  )
})





test_that("it handles empty lists", {
  dt <- data.table(id = numeric(), time = numeric(), value = numeric())
  expect_warning(xx <- db_ts_store("con", dt, "release", "access"), "no-op")
  expect_equal(xx, list())
})

test_that("it complains when it gets a non-ts_dt", {
  dt <- data.table(gronkh = numeric(), knight_in_shining_armour = numeric())
  expect_error(db_ts_store("con", dt, "release", "access"))
})

test_that("it complains about character-ts in list", {
  char_tsl <- list(
    ts1 = ts(letters, 2020, frequency = 12)
  )

  # Yes I am using my knowledge that the db is not hit in this case. Sue me.
  expect_error(
    db_ts_store("con", char_tsl, "release", "access"),
    "numeric"
  )
})

test_that("it complains about character-ts in dt", {
  char_dt <- data.table(
    id = "tss",
    time = seq(Sys.Date(), length.out = 26, by = "1 months"), # thehe
    value = letters
  )

  expect_error(
    db_ts_store("con", char_dt, "release", "access"),
    "numeric"
  )
})

test_that("id complains about duplicate series in dt", {
  dup_dt <- data.table(
    id = "dupli_mac_dupleton",
    time = rep(seq(Sys.Date(), length.out = 13, by = "3 month"), each = 2),
    value = pi
  )

  expect_error(
    db_ts_store("con", dup_dt, "release", "access"),
    "duplicated"
  )
})


# storing single ts -------------------------------------------------------

context("db_ts_store.ts")

test_that("it calls through to store_records", {
  store_recs <- mock()
  with_mock(
    "timeseriesdb:::store_records" = store_recs,
    "timeseriesdb:::to_ts_json" = mock("ts_json"),
    {
      db_ts_store(
        "con",
        ts,
        "access",
        "valid_from",
        "release_date",
        "pre_release_access",
        "schema"
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
        "pre_release_access",
        "schema"
      )
    }
  )
})

test_that("defaults are passed on correctly", {
  store_recs <- mock()
  with_mock(
    "timeseriesdb:::store_records" = store_recs,
    "timeseriesdb:::to_ts_json" = mock("ts_json"),
    {
      db_ts_store(
        "con",
        ts
      )

      expect_called(store_recs, 1)

      expect_args(
        store_recs,
        1,
        "con",
        "ts_json",
        NULL,
        "timeseries_main",
        NULL,
        NULL,
        NULL,
        "timeseries"
      )
    }
  )
})


# store xts ---------------------------------------------------------------

context("db_ts_store.xts")

test_that("it calls through to store_records", {
  store_recs <- mock()
  with_mock(
    "timeseriesdb:::store_records" = store_recs,
    "timeseriesdb:::to_ts_json" = mock("ts_json"),
    {
      db_ts_store(
        "con",
        xts,
        "access",
        "valid_from",
        "release_date",
        "pre_release_access",
        "schema"
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
        "pre_release_access",
        "schema"
      )
    }
  )
})

test_that("defaults are passed on correctly", {
  store_recs <- mock()
  with_mock(
    "timeseriesdb:::store_records" = store_recs,
    "timeseriesdb:::to_ts_json" = mock("ts_json"),
    {
      db_ts_store(
        "con",
        xts
      )

      expect_called(store_recs, 1)

      expect_args(
        store_recs,
        1,
        "con",
        "ts_json",
        NULL,
        "timeseries_main",
        NULL,
        NULL,
        NULL,
        "timeseries"
      )
    }
  )
})
