context("db_populate_ts_read")

test_that("It warns when using regex with multiple ts_keys", {
  with_mock(
    dbExecute = mock(),
    dbWriteTable = mock(),
    dbQuoteIdentifier = mock(),
    dbQuoteLiteral = mock(),
    {
      expect_warning(db_tmp_read(
        NULL,
        c("a", "b"),
        TRUE,
        "schema"),
        "using only first element")
    }
  )
})

test_that("it only uses the first element of ts_keys when regex == TRUE", {
  db_quote_literal_mock = mock()
  with_mock(
    dbExecute = mock(),
    dbWriteTable = mock(),
    dbQuoteIdentifier = mock(),
    dbQuoteLiteral = db_quote_literal_mock,
    {
      suppressWarnings(db_tmp_read(
        NULL,
        c("a", "b"),
        TRUE,
        "schema"))

      args <- mock_args(db_quote_literal_mock)

      expect_match(args[[1]][[2]], "a")
    }
  )
})
