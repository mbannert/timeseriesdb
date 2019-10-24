context("db_populate_ts_read")

test_that("It warns when using regex with multiple ts_keys", {
  query_populate_ts_read_keys_regex_mock <- mock()
  with_mock(
    dbExecute = mock(),
    dbWriteTable = mock(),
    "timeseriesdb:::query_populate_ts_read_keys_regex" = query_populate_ts_read_keys_regex_mock,
    "timeseriesdb:::query_populate_ts_read" = mock(),
    {
      expect_warning(db_populate_ts_read(
        NULL,
        c("a", "b"),
        TRUE,
        "schema",
        "timeseries_main",
        "2019-01-01",
        FALSE),
        "using only first element")
    }
  )
})

test_that("it only uses the first element of ts_keys when regex == TRUE", {
  query_populate_ts_read_keys_regex_mock <- mock()
  with_mock(
    dbExecute = mock(),
    dbWriteTable = mock(),
    "timeseriesdb:::query_populate_ts_read_keys_regex" = query_populate_ts_read_keys_regex_mock,
    "timeseriesdb:::query_populate_ts_read" = mock(),
    {
      suppressWarnings(db_populate_ts_read(
        NULL,
        c("a", "b"),
        TRUE,
        "schema",
        "timeseries_main",
        "2019-01-01",
        FALSE))
      
      expect_args(query_populate_ts_read_keys_regex_mock,
                  1,
                  con = NULL,
                  schema = "schema",
                  pattern = "a")
    }
  )
})
