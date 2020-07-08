context("to_ts_json.tslist")

test_tsj <- function(tsj) {
  expect_is(tsj, "ts_json")
  expect_is(tsj[[1]], "json")
  expect_equal(unclass(tsj[[1]]), '{"frequency":12,"time":["2019-01-01","2019-02-01","2019-03-01","2019-04-01","2019-05-01"],"value":[1,2,3,4,5]}')
}

test_that("it works on tslists", {
  tsl <- list(
    my_ts = ts(1:5, start = 2019, frequency = 12)
  )
  class(tsl) <- c("tslist", "list")

  tsj <- to_ts_json(tsl)
  test_tsj(tsj)
})

test_that("it pretty-unboxes ts of length 1", {
  tsl <- list(
    my_ts = ts(1, start = 2019, frequency = 12)
  )
  class(tsl) <- c("tslist", "list")

  tsj <- to_ts_json(tsl)
  expect_equal(unclass(tsj[[1]]), '{"frequency":12,"time":["2019-01-01"],"value":[1]}')
})

test_that("it does not throw away precision", {
  tsl <- list(
    my_ts = ts(rep(pi, 100), start = 2019, frequency = 12)
  )
  class(tsl) <- c("tslist", "list")

  tsj <- to_ts_json(tsl)
  expect_match(as.character(tsj), as.character(pi))
})

context("to_ts_json.data.table")

test_that("it works on ts_dts", {
  dt <- data.table(
    id = "my_ts",
    time = seq(as.Date("2019-01-01"), length.out = 5, by = "1 month"),
    value = 1:5,
    freq = 12
  )

  tsj <- to_ts_json(dt)
  test_tsj(tsj)
})

test_that("it pretty-unboxes ts of length 1", {
  dt <- data.table(
    id = "my_ts",
    time = seq(as.Date("2019-01-01"), length.out = 1, by = "1 month"),
    value = 1,
    freq = 12
  )

  tsj <- to_ts_json(dt)
  expect_equal(unclass(tsj[[1]]), '{"frequency":12,"time":["2019-01-01"],"value":[1]}')
})

test_that("it sets frequency to null when unknown", {
  dt <- data.table(
    id = "my_ts",
    time = seq(as.Date("2019-01-01"), length.out = 1, by = "1 month"),
    value = 1
  )

  tsj <- to_ts_json(dt)
  expect_equal(unclass(tsj[[1]]), '{"frequency":null,"time":["2019-01-01"],"value":[1]}')
})

test_that("assigning names works with nTs > 1", {
  dt <- data.table(
    id = c("ts1", "ts2"),
    time = seq(as.Date("2019-01-01"), length.out = 2, by = "1 month"),
    value = 1:4
  )

  # This is testthat for "expect to not throw an error"
  expect_error(to_ts_json(dt), NA)
})

test_that("it does not throw away precision", {
  dt <- data.table(
    id = "my_ts",
    time = seq(as.Date("2019-01-01"), length.out = 10, by = "1 month"),
    value = pi
  )

  tsj <- to_ts_json(dt)
  expect_match(as.character(tsj), as.character(pi))
})

test_that("It eats irregular time series", {
  x_ts <- list(
    xxts = xts::xts(
      c(1, 2, 3),
      order.by = c(zoo::as.yearmon("2019-01-01"), zoo::as.yearmon("2020-01-01"), zoo::as.yearmon("2020-03-01"))
  ))
  class(x_ts) <- c("tslist", "list")
  tsj <- to_ts_json(x_ts)

  expect_equal(
    unclass(tsj[[1]]),
    '{"frequency":null,"time":["2019-01-01","2020-01-01","2020-03-01"],"value":[1,2,3]}'
  )
})
