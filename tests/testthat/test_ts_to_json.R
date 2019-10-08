context("ts_to_json")



test_tsj <- function(tsj) {
  expect_is(tsj, "ts_json")
  expect_is(tsj[[1]], "json")
  expect_equal(unclass(tsj[[1]]), '{"time":["2019-01-01","2019-02-01","2019-03-01","2019-04-01","2019-05-01"],"value":[1,2,3,4,5]}')
}

test_that("it works on tslists", {
  tsl <- list(
    my_ts = ts(1:5, start = 2019, frequency = 12)
  )
  class(tsl) <- c("tslist", "list")
  
  tsj <- to_ts_json(tsl)
  test_tsj(tsj)
})

test_that("it works on ts_dts", {
  dt <- data.table(
    id = "my_ts",
    time = seq(as.Date("2019-01-01"), length.out = 5, by = "1 month"),
    value = 1:5
  )
  
  tsj <- to_ts_json(dt)
  test_tsj(tsj)
})
