
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

# test print methods ------------------------------------------------------

# Coming soon to an RStudio near you: lots of expect_output assertions!
