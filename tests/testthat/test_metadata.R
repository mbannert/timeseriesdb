
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
  class(out) <- c("tsmeta", class(out))
  out
}

meta_df <- function() {
  data.frame(
    ts_key = c("ts_key1", "ts_key2"),
    key1 = c("value1", "value3"),
    key2 = c("value2", "value4"),
    stringsAsFactors = FALSE # Oh how I can't wait for 4.0...
  )
}

# test constructing meta --------------------------------------------------

test_that("meta.list constructs meta", {
  mt <- meta_object()
  outv <- create_meta(list(
    key1 = "value1",
    key2 = "value2"
  ))

  expect_equal(outv, mt)
})

test_that("meta.default constructs meta", {
  mt <- meta_object()
  outv <- create_meta(key1 = "value1",
               key2 = "value2")

  expect_equal(outv, mt)
})

test_that("meta.list errors when not all fields are named", {
  expect_error(create_meta(list(a = 1, 2)), "named")
})

test_that("meta.list errors when no fields are named", {
  expect_error(create_meta(list(1, 2)), "named")
})

test_that("meta.default errors when not all fields are named", {
  expect_error(create_meta(a = 1, 2), "named")
})

test_that("meta.default errors when no fields are named", {
  expect_error(create_meta(1, 2), "named")
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


# test constructing tsmeta -------------------------------------------

test_that("create_tsmeta constructs a tsmeta", {
  inv <- unclass(meta_list())
  inv <- lapply(inv, `class<-`, "list")
  outv <- do.call(create_tsmeta, inv)
  expect_equal(outv, meta_list())
})


test_that("tsmeta -> tsmeta", {
  outv <- as.tsmeta(meta_list())
  expect_equal(outv, meta_list())
  expect_is(outv[[1]], "meta")
})

# test converting to tsmeta.list ------------------------------------------

test_that("list -> tsmeta", {
  inv <- meta_list()
  inv <- lapply(inv, `class<-`, "list")
  outv <- as.tsmeta(inv)
  expect_equal(outv, meta_list())
  expect_is(outv[[1]], c("meta"))
})

test_that("empty list -> tsmeta", {
  outv <- as.tsmeta.list(list())
  expect_is(outv, "tsmeta")
  expect_equal(length(outv), 0)
})

test_that("invalid list -> tsmeta", {
  inv <- list(a = list(b = list(too_deep = TRUE)))
  expect_error(as.tsmeta(inv))
})

test_that("data.frame -> tsmeta", {
  outv <- as.tsmeta(meta_df())
  expect_equal(outv, meta_list())
})

test_that("data.table -> tsmeta", {
  outv <- as.tsmeta(as.data.table(meta_df()))
  expect_equal(outv, meta_list())
})

test_that("as.tsmeta.data.table skips depth check", {
  fake_as.tsmeta.list <- mock()

  with_mock(
    as.tsmeta.list = fake_as.tsmeta.list,
    {
      as.tsmeta(as.data.table(meta_df()))
    }
  )

  expect_false(mock_args(fake_as.tsmeta.list)[[1]]$check_depth)
})

# test print methods ------------------------------------------------------

# Coming soon to an RStudio near you: lots of expect_output assertions!
