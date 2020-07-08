context("date_to_index")

test_that("it converts a date to an index", {
  x <- date_to_index(as.Date("2019-07-01"))
  expect_is(x, "numeric")
  expect_equal(x, 2019.5)
})

test_that("it can also handle a string", {
  x <- date_to_index("2019-07-01")
  expect_is(x, "numeric")
  expect_equal(x, 2019.5)
})

test_that("it rounds to months", {
  x <- date_to_index("2019-10-31") # spooooky
  expect_equal(x, 2019.75)
})
