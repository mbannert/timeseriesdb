context("metaConverters")

# Define fixtures as functions so we can get fresh ones each time
meta_list <- function() {
  out <- list(
    ts_key1 = list(
      key1 = "value1",
      key2 = "value2"
    ),
    ts_key2 = list(
      key1 = "value3",
      key2 = "value4"
    )
  )
  out <- lapply(out, function(x) {
    class(x) <- c("tsmeta", class(x))
    x
  })
  class(out) <- c("tsmeta.list", class(out))
  out
}

meta_dt <- function() {
  out <- data.table(
    key1 = c("value1", "value3"),
    key2 = c("value2", "value4"),
    ts_key = c("ts_key1", "ts_key2")
  )
  class(out) <- c("tsmeta.dt", class(out))
  out
}

# Tests
#############################################
# To tsmeta.dt
#############################################
test_that("tsmeta.list -> tsmeta.dt", {
  outv <- as.tsmeta.dt(meta_list())
  expect_equal(outv, meta_dt())
})

test_that("list -> tsmeta.dt", {
  inv <- meta_list()
  class(inv) <- "list"
  outv <- as.tsmeta.dt(inv)
  expect_equal(outv, meta_dt())
})

test_that("invalid list -> tsmeta.dt", {
  inv <- list()
  expect_error(as.tsmeta.dt(inv))
})

test_that("list with fillin -> tsmeta.dt", {
  inv <- meta_list()
  inv$ts_key1$key3 <- "valueX"
  expect_warning(outv <- as.tsmeta.dt(inv))
  expected <- meta_dt()
  expected$key3 <- c("valueX", NA)
  setcolorder(expected, c("key1", "key2", "key3", "ts_key"))
  expect_equal(outv, expected)
})

test_that("data.frame -> tsmeta.dt", {
  inv <- meta_dt()
  class(inv) <- "data.frame"
  outv <- as.tsmeta.dt(inv)
  expect_equal(outv, meta_dt())
})

test_that("tsmeta.dt -> tsmeta.dt", {
  expect_equal(as.tsmeta.dt(meta_dt()), meta_dt())
})

########################################
# To tsmeta.list
########################################
test_that("tsmeta.list -> tsmeta.list", {
  outv <- as.tsmeta.list(meta_list())
  expect_equal(outv, meta_list())
  expect_is(outv[[1]], "tsmeta")
})

test_that("list -> tsmeta.list", {
  inv <- meta_list()
  inv <- lapply(inv, `class<-`, "list")
  outv <- as.tsmeta.list(inv)
  expect_equal(outv, meta_list())
  expect_is(outv[[1]], c("tsmeta"))
})

test_that("invalid list -> tsmeta.list", {
  inv <- list()
  expect_error(as.tsmeta.list(inv))
})

test_that("tsmeta.dt -> tsmeta.list", {
  outv <- as.tsmeta.list(meta_dt())
  expect_equal(outv, meta_list())
  expect_is(outv[[1]], c("tsmeta"))
})

test_that("filled tsmeta.dt -> tsmeta.list", {
  inv <- meta_dt()
  inv$key3 <- c("valueX", NA)
  setcolorder(inv, c("key1", "key2", "key3", "ts_key"))
  outv <- as.tsmeta.list(inv)
  expect_equal(length(outv$ts_key2), 2)
  expect_true(!any(is.na(outv$ts_key2)))
  expect_is(outv[[1]], c("tsmeta"))
})
