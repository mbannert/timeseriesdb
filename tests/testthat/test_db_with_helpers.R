context("db_with_temp_table")

test_that("It creates a temp table and hands it over to admin", {
  fake_dbWriteTable = mock()
  fake_db_grant_to_admin = mock()

  with_mock(
    dbWriteTable = fake_dbWriteTable,
    "timeseriesdb:::db_grant_to_admin" = fake_db_grant_to_admin,
    dbRemoveTable = mock(),
    {
      db_with_temp_table("con", "temp", "content", "field.types", {1+1}, "schema")

      expect_args(
        fake_dbWriteTable,
        1,
        con = "con",
        name = "temp",
        value = "content",
        field.types = "field.types",
        overwrite = TRUE,
        temporary = TRUE
      )

      expect_args(
        fake_db_grant_to_admin,
        1,
        "con",
        "temp",
        "schema"
      )
    }
  )
})

test_that("it evaluates the code", {
  fake_code = mock()
  with_mock(
    dbWriteTable = mock(),
    "timeseriesdb:::db_grant_to_admin" = mock(),
    dbRemoveTable = mock(),
    {
      db_with_temp_table("con", "temp", "content", "field.types", fake_code(), "schema")
      expect_called(fake_code, 1)
    }
  )
})

test_that("it removes the table even in error case", {
  fake_remove = mock()

  with_mock(
    dbWriteTable = mock(),
    "timeseriesdb:::db_grant_to_admin" = mock(),
    dbRemoveTable = fake_remove,
    {
      capture_error(db_with_temp_table("con", "temp", "content", "field.types", stop("oh noes"), "schema"))

      expect_called(fake_remove, 1)
    }
  )
})


context("db_with_tmp_read")

test_that("It warns when using regex with multiple ts_keys", {
  with_mock(
    dbWriteTable = mock(),
    "timeseriesdb:::db_grant_to_admin" = mock(),
    dbRemoveTable = mock(),
    dbExecute = mock(),
    dbQuoteIdentifier = mock(),
    dbQuoteLiteral = mock(),
    {
      expect_warning(
        db_with_tmp_read("con", c("a", "b"), TRUE, 1+1, "schema"),
        "using only first element"
      )
    }
  )
})

test_that("It only uses the first element of ts_keys when regex == TRUE", {
  db_quote_literal_mock = mock()
  with_mock(
    dbWriteTable = mock(),
    "timeseriesdb:::db_grant_to_admin" = mock(),
    dbRemoveTable = mock(),
    dbExecute = mock(),
    dbQuoteIdentifier = mock(),
    dbQuoteLiteral = db_quote_literal_mock,
    {
      suppressWarnings(db_with_tmp_read("con", c("a", "b"), TRUE, 1+1, "schema"))

      expect_equal(mock_args(db_quote_literal_mock)[[1]][[2]], "a")
    }
  )
})

test_that("It evaluates the code", {
  mock_f = mock()

  with_mock(
    dbWriteTable = mock(),
    "timeseriesdb:::db_grant_to_admin" = mock(),
    dbRemoveTable = mock(),
    dbExecute = mock(),
    dbQuoteIdentifier = mock(),
    dbQuoteLiteral = mock(),
    {
      db_with_tmp_read("con", c("a", "b"), FALSE, mock_f(), "schema")

      expect_called(mock_f, 1)
    }
  )
})

test_that("It removes the temp table in error case", {
  fake_dbRemoveTable = mock()

  with_mock(
    dbWriteTable = mock(),
    "timeseriesdb:::db_grant_to_admin" = mock(),
    dbRemoveTable = fake_dbRemoveTable,
    dbExecute = mock(),
    dbQuoteIdentifier = mock(),
    dbQuoteLiteral = mock(),
    {
      capture_error(db_with_tmp_read("con", c("a"), TRUE, stop("blaaagh"), "schema"))

      expect_called(fake_dbRemoveTable, 1)
    }
  )
})



if(is_test_db_reachable()) {
  con_admin <- connect_to_test_db()
}

test_with_fresh_db(con_admin, "temp_ts_read gets cleaned up in failure case", {
  boom_bot <- mock(stop("Kablammy!"))

  with_mock(
    "timeseriesdb:::get_tsl_from_res" = boom_bot,
    {
      e <- capture_error(db_ts_read(con_admin, "rts1", schema = "tsdb_test"))

      expect_equal(
        dbListTables(con_admin),
        character(0)
      )
    }
  )
})
