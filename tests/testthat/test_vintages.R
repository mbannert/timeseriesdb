context("vintages")

con <- NULL

on_cran <- !identical(Sys.getenv("NOT_CRAN"), "true")

if (!on_cran) {
  con <- createConObj(dbhost = "localhost",
                      dbname = "sandbox",
                      passwd = "")
  dbGetQuery(con, "DELETE from timeseries.timeseries_vintages")
}

# later time series
ts1.2 <- ts(runif(80,0,50),start=c(2000,1),freq=12)

# earlier time series
ts2_tsp <- tsp(ts1.2)
ts1 <- window(ts1.2, start = ts2_tsp[1], end = ts2_tsp[2] - 1/ts2_tsp[3])
ts2 <- ts(runif(80, 0, 50), start = c(2000, 1), freq = 12)

test_that("storing vintages works", {
  skip_on_cran()
  
  storeTimeSeries(c("ts1", "ts2"), con, list(ts1 = ts1, ts2 = ts2), valid_from = "2000-01-01")
  
  cnt <- dbGetQuery(con, "select count(*) from timeseries.timeseries_vintages")$count
  expect_equal(cnt, 2)
})

test_that("storing vintages for existing ts works", {
  skip_on_cran()
  
  storeTimeSeries(c("ts1"), con, list(ts1 = ts1.2), valid_from = "2006-08-01")
  cnt <- dbGetQuery(con, "select count(*) from timeseries.timeseries_vintages")$count
  expect_equal(cnt, 3)
})

test_that("inserting vintage into already covered range fails", {
  skip_on_cran()
  
  expect_warning(err <- storeTimeSeries(c("ts1"), con, list(ts1 = ts1), valid_from = "2004-01-01"))

  rollbackTransaction(con)  # Q: Why does runDbQuery's auto rollback not do here?
                            # A: Because testthat (https://github.com/r-lib/testthat/issues/244)
})

test_that("inserting vintage that causes empty validity range fails", {
  skip_on_cran()
  
  err <- storeTimeSeries(c("ts1"), con, list(ts1 = ts1), valid_from = "2006-08-01")
  
  expect_equal(names(err), "error")
})

test_that("reading earlier versions works", {
  skip_on_cran()
  
  ts_before <- readTimeSeries("ts1", con, valid_on = "2001-01-01")
  ts_after <- readTimeSeries("ts1", con, valid_on = "2007-01-01")
  
  expect_equal(ts_before$ts1, ts1)
  expect_equal(ts_after$ts1, ts1.2)
})

test_that("getTimeSeriesVintages works", {
  skip_on_cran()
  
  series <- c("ts1", "ts2", "ts3")
  
  expected_vintages_ts1 <- data.frame(
    lower_bound = c(as.Date("2000-01-01"), as.Date("2006-08-01")),
    upper_bound = c(as.Date("2006-08-01"), structure(Inf, class="Date"))
  )
  
  vintages <- getTimeSeriesVintages(series, con)
  
  expect_equal(names(vintages), series)
  expect_true(is.na(vintages$ts3))
  expect_equivalent(vintages$ts1, expected_vintages_ts1)
})

if (!on_cran) {
  dbDisconnect(con)
}