context("Operators")

tsl <- list(
  ts1 = ts(rnorm(100), start = c(1990,1), frequency = 4),
  ts2 = ts(rnorm(80), start = c(1995,1), frequency = 12)
)

class(tsl) <- c("tslist", "list")

test_that("class is kept", {
  x <- tsl[1:2]
  expect_is(x, "tslist")
})

test_that("subset works",{
  x <- tsl[1]
  tsl_copy <- tsl
  tsl_copy[[2]] <- NULL
  expect_equal(x, tsl_copy)
  
  
})
