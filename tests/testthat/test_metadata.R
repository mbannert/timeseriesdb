
# setup -------------------------------------------------------------------


context("metadata")

# Define fixtures as functions so we can get fresh ones each time

meta_object <- function(x = list(
  key1 = "value1",
  key2 = "value2"
)) {
  class(x) <- c("meta", "list")
  x
}

meta_list <- function() {
  out <- list(
    ts_key1 = meta_object(),
    ts_key2 = meta_object(list(
      key1 = "value3",
      key2 = "value4"
    ))
  )
  class(out) <- c("tsmeta.list", class(out))
  out
}

meta_dt <- function() {
  out <- data.table(
    ts_key = c("ts_key1", "ts_key2"),
    key1 = c("value1", "value3"),
    key2 = c("value2", "value4")
  )
  class(out) <- c("tsmeta.dt", class(out))
  out
}


# test constructing meta --------------------------------------------------

test_that("meta.list constructs meta", {
  mt <- meta_object()
  outv <- meta(list(
    key1 = "value1",
    key2 = "value2"
  ))
  
  expect_equal(outv, mt)
})

test_that("meta.default constructs meta", {
  mt <- meta_object()
  outv <- meta(key1 = "value1",
               key2 = "value2")
  
  expect_equal(outv, mt)
})

test_that("meta.list errors when not all fields are named", {
  expect_error(meta(list(a = 1, 2)), "named")
})

test_that("meta.list errors when no fields are named", {
  expect_error(meta(list(1, 2)), "named")
})

test_that("meta.default errors when not all fields are named", {
  expect_error(meta(a = 1, 2), "named")
})

test_that("meta.default errors when no fields are named", {
  expect_error(meta(1, 2), "named")
})

# converting to meta object -----------------------------------------------

test_that("as.meta does NA", {
  expect_true(is.na(as.meta(NA)))
})

test_that("as.meta does NULL", {
  expect_true(is.null(as.meta(NULL)))
})

test_that("as.meta does lists", {
  mt <- meta_object()
  outv <- as.meta(list(
    key1 = "value1",
    key2 = "value2"
  ))
  
  expect_equal(mt, outv)
})

test_that("as.meta does not to non-lists", {
  expect_error(as.meta(1), "lists")
})

# test constructing tsmeta.dt ---------------------------------------------

test_that("tsmeta.dt constructs a tsmeta.dt", {
  dt <- meta_dt()
  outv <- tsmeta.dt(
    ts_key = dt$ts_key,
    key1 = dt$key1,
    key2 = dt$key2
  )
  expect_equal(outv, dt)
})

test_that("empty tsmeta.dt are possible", {
  outv <- tsmeta.dt()
  expect_is(outv, "tsmeta.dt")
  expect_equal(nrow(outv), 0)
})

# test converting to tsmeta.dt --------------------------------------------

test_that("tsmeta.list -> tsmeta.dt", {
  outv <- as.tsmeta.dt(meta_list())
  expect_equal(outv, meta_dt())
})

test_that("empty tsmeta.list -> tsmeta.dt", {
  outv <- as.tsmeta.dt(tsmeta.list())
  expect_is(outv, "tsmeta.dt")
  expect_equal(nrow(outv), 0)
})

test_that("list -> tsmeta.dt", {
  inv <- meta_list()
  class(inv) <- "list"
  outv <- as.tsmeta.dt(inv)
  expect_equal(outv, meta_dt())
})

test_that("empty list -> tsmeta.dt", {
  inv <- list()
  outv <- as.tsmeta.dt(inv)
  expect_is(outv, "tsmeta.dt")
  expect_equal(nrow(outv), 0)
})

test_that("invalid list -> tsmeta.dt", {
  inv <- list(a = list(b = list(too_deep = TRUE)))
  expect_error(as.tsmeta.dt(inv))
})

test_that("list with fillin -> tsmeta.dt", {
  inv <- meta_list()
  inv$ts_key1$key3 <- "valueX"
  expect_warning(outv <- as.tsmeta.dt(inv))
  expected <- meta_dt()
  expected$key3 <- c("valueX", NA)
  setcolorder(expected, c("ts_key", "key1", "key2", "key3"))
  expect_equal(outv, expected)
})

test_that("data.frame -> tsmeta.dt", {
  inv <- meta_dt()
  class(inv) <- "data.frame"
  outv <- as.tsmeta.dt(inv)
  expect_equal(outv, meta_dt())
})

test_that(" empty data.frame -> tsmeta.dt", {
  outv <- as.tsmeta.dt(data.frame())
  expect_is(outv, "tsmeta.dt")
  expect_equal(nrow(outv), 0)
})

test_that("tsmeta.dt -> tsmeta.dt", {
  expect_equal(as.tsmeta.dt(meta_dt()), meta_dt())
})


test_that("list with empty -> tsmeta.dt", {
  inv <- meta_list()
  inv$tskey_3 <- list()
  outv <- as.tsmeta.dt(inv)
  expect_equal(nrow(outv), 3)
  expect_equal(sum(is.na(outv[3, ])), 2)
})


# test constructing tsmeta.list -------------------------------------------

test_that("tsmeta.list constructs a tsmeta.list", {
  inv <- unclass(meta_list())
  inv <- lapply(inv, `class<-`, "list")
  outv <- do.call(tsmeta.list, inv)
  expect_equal(outv, meta_list())
})


test_that("tsmeta.list -> tsmeta.list", {
  outv <- as.tsmeta.list(meta_list())
  expect_equal(outv, meta_list())
  expect_is(outv[[1]], "meta")
})

# test converting to tsmeta.list ------------------------------------------

test_that("list -> tsmeta.list", {
  inv <- meta_list()
  inv <- lapply(inv, `class<-`, "list")
  outv <- as.tsmeta.list(inv)
  expect_equal(outv, meta_list())
  expect_is(outv[[1]], c("meta"))
})

test_that("empty list -> tsmeta.list", {
  outv <- as.tsmeta.list(list())
  expect_is(outv, "tsmeta.list")
  expect_equal(length(outv), 0)
})

test_that("invalid list -> tsmeta.list", {
  inv <- list(a = list(b = list(too_deep = TRUE)))
  expect_error(as.tsmeta.list(inv))
})

test_that("tsmeta.dt -> tsmeta.list", {
  outv <- as.tsmeta.list(meta_dt())
  expect_equal(outv, meta_list())
  expect_is(outv[[1]], c("meta"))
})

test_that("empty tsmeta.dt -> tsmeta.list", {
  outv <- as.tsmeta.list(tsmeta.dt())
  expect_is(outv, "tsmeta.list")
  expect_equal(length(outv), 0)
})

test_that("filled tsmeta.dt -> tsmeta.list", {
  inv <- meta_dt()
  inv$key3 <- c("valueX", NA)
  setcolorder(inv, c("key1", "key2", "key3", "ts_key"))
  outv <- as.tsmeta.list(inv)
  expect_equal(length(outv$ts_key2), 2)
  expect_true(!any(is.na(outv$ts_key2)))
  expect_is(outv[[1]], c("meta"))
})

test_that("tsmeta.dt with empty -> tsmeta.list", {
  tsml <- meta_list()
  tsml$ts_key3 <- list()
  inv <- as.tsmeta.dt(tsml)
  outv <- as.tsmeta.list(inv)
  expect_is(outv$ts_key3, "list")
  expect_equal(length(outv$ts_key3), 0)
})


# test print methods ------------------------------------------------------

# Coming soon to an RStudio near you: lots of expect_output assertions!
