context("get_use_case")

test_that("it recognizes use case 1", {
  expect_equal(get_use_case("2019-01-01", NULL), 1)
})

test_that("it recognizes use case 2", {
  expect_equal(get_use_case("2019-01-01", "2019-01-10"), 2)
})

test_that("it recognizes use case 3", {
  expect_equal(get_use_case(NULL, NULL), 3)
})

test_that("it recognizes use case 4", {
  expect_equal(get_use_case(NULL, "2019-01-10"), 4)
})