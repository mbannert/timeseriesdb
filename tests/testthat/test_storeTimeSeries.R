context("deprecated storeTimeSeries")

test_that("it calls .Deprecated", {
  skip("Not yet sure if we are going to actually keep the old syntax")
  
  my_dep <- mock()
  with_mock(
    ".Deprecated" = my_dep,
    "timeseriesdb::store_time_series" = mock(),
    {
    }
  )
})