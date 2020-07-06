context("index_to_date")

test_that("it converts an index to a date", {
  x <- index_to_date(2019.5)
  expect_is(x, "Date")
  expect_equal(x, as.Date("2019-07-01"))
})

test_that("it can also return a string", {
  x <- index_to_date(2019.5, as.string = TRUE)
  expect_is(x, "character")
  expect_equal(x, "2019-07-01")
})

test_that("it currently only does days", {
  x <- index_to_date(2019.54, as.string = TRUE)
  expect_equal(x, "2019-07-01")
})

test_that("it zero-pads the month", {
  x <- index_to_date(2019, as.string = TRUE)
  expect_true(grepl("-01-01", x))
})
