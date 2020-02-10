context("db_call_function")

fake_dbQuoteIdentifier <- function(con, x) {
  sprintf("\"%s\"", x)
}

test_that("db_call_function without args", {
  fake_dbGetQuery = mock(list(my_function = "a result"))

  with_mock(
    dbQuoteIdentifier = fake_dbQuoteIdentifier,
    dbGetQuery = fake_dbGetQuery,
    {
      db_call_function("con", "my_function")

      expect_args(fake_dbGetQuery,
                  1,
                  "con",
                  "SELECT * FROM \"timeseries\".\"my_function\"()",
                  NULL)
    }
  )
})

test_that("db_call_function with args", {
  fake_dbGetQuery = mock(list(my_function = "a result"))

  with_mock(
    dbQuoteIdentifier = fake_dbQuoteIdentifier,
    dbGetQuery = fake_dbGetQuery,
    {
      args <- list(
        a = 1,
        b = "banana"
      )

      db_call_function("con", "my_function", args)

      expect_args(fake_dbGetQuery,
                  1,
                  "con",
                  "SELECT * FROM \"timeseries\".\"my_function\"($1, $2)",
                  args)
    }
  )
})

test_that("db_call_function with schema", {
  fake_dbGetQuery = mock(list(my_function = "a result"))

  with_mock(
    dbQuoteIdentifier = fake_dbQuoteIdentifier,
    dbGetQuery = fake_dbGetQuery,
    {
      db_call_function("con", "my_function", schema = "some_otter_place")

      expect_args(fake_dbGetQuery,
                  1,
                  "con",
                  "SELECT * FROM \"some_otter_place\".\"my_function\"()",
                  NULL)
    }
  )
})

test_that("db_call_function returns the result of SQL function", {
  fake_dbGetQuery = mock(list(my_function = "a result"))

  with_mock(
    dbQuoteIdentifier = fake_dbQuoteIdentifier,
    dbGetQuery = fake_dbGetQuery,
    {
      result <- db_call_function("con", "my_function")

      expect_equal(result, "a result")
    }
  )
})
