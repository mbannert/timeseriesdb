context("json_to_ts")

# const regular_json = 'I seem to be in JS mode again...'
regular_json <-  '
{
  "frequency": 12,
  "time": ["2019-01-01", "2019-02-01", "2019-03-01", "2019-04-01"],
  "value": [1, 2, 3, 4]
}
'

regular_ts <- ts(1:4, 2019, frequency = 12)

irregular_json <- '
{
  "frequency": null,
  "time": ["2019-01-01", "2019-02-02", "2019-03-03", "2019-04-04"],
  "value": [1, 2, 3, 4]
}
'

irregular_xts <- xts::xts(
  1:4,
  order.by = as.Date(c("2019-01-01", "2019-02-02", "2019-03-03", "2019-04-04")))

mock_date_to_index <- mock(2019)

test_that("it converts a regular json to ts", {
  # Is this too pedantic?
  with_mock(
    date_to_index = mock_date_to_index,
    {
      x <- json_to_ts(regular_json)
      expect_is(x, "ts")
      expect_equal(x, regular_ts)
    }
  )
})

test_that("it converts an irregular json to xts", {
  x <- json_to_ts(irregular_json)
  expect_is(x, "xts")
  expect_equal(x, irregular_xts)
})

test_that("it can output a data.table if desired", {
  x <- json_to_ts(regular_json, as.dt = TRUE)
  expect_is(x, "data.table")
  expect_equal(names(x), c("frequency", "time", "value"))
})

test_that("it still returns frequency in dt form", {
  x <- json_to_ts(irregular_json, as.dt = TRUE)
  expect_equal(names(x), c("frequency", "time", "value"))
})
