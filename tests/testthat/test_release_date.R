context("release date")

con <- NULL

on_cran <- !identical(Sys.getenv("NOT_CRAN"), "true")

if (!on_cran) {
  con <- createConObj(dbhost = "localhost",
                      dbname = "sandbox",
                      passwd = "")
  
  dbGetQuery(con, "DELETE from timeseries.timeseries_main")
  
  db_time <- dbGetQuery(con, "select NOW() as time")$time
  
  release_date <- db_time + 3 # 3 seconds from now
}

set.seed(123)
tslist <- list()
tslist$ts_w_release_date <- ts(rnorm(20),start = c(1990,1), frequency = 4)

# Test that ..... ##################

test_that("Release date gets stored without error", {
  skip_on_cran()
  
  storeTimeSeries("ts_w_release_date", con, tslist, release_date = release_date)
  stored_release_date <- dbGetQuery(con, 
                                    "select ts_release_date from timeseries.timeseries_main where ts_key = 'ts_w_release_date'")$ts_release_date
  expect_equal(release_date, stored_release_date, tolerance = 10)
})

test_that("Release date has no effect by default", {
  skip_on_cran()
  
  ts_read <- readTimeSeries("ts_w_release_date", con)$ts_w_release_date
  expect_equal(length(ts_read), 20)
})

test_that("Respecting release_date in readTimeSeries works", {
  skip_on_cran()
  
  ts_read_before <- readTimeSeries("ts_w_release_date", con, respect_release_date = TRUE)$ts_w_release_date
  expect_equal(length(ts_read_before), 19)
  
  # Make sure we cross the release threshold
  Sys.sleep(5)
  
  ts_read_after <- readTimeSeries("ts_w_release_date", con, respect_release_date = TRUE)$ts_w_release_date
  expect_equal(length(ts_read_after), 20)
})

if(!on_cran) {
  dbDisconnect(con)
}