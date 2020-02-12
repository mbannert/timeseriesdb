con <- NULL
if(is_test_db_reachable()) {
  con <- connect_to_test_db()
}

context("read unlocalized metadata")

test_that("is passes correct args to db_call_function unlocalized", {
  fake_db_call_function = mock(data.frame(
    ts_key = "vts1",
    metadata = "{}"
  ))

  with_mock(
    db_tmp_read = mock(),
    db_call_function = fake_db_call_function,
    {
      db_read_ts_metadata("con", "vts1", valid_on = "2020-01-01", schema = "schema")

      expect_args(fake_db_call_function,
                  1,
                  "con",
                  "read_metadata_raw",
                  list(as.Date("2020-01-01")),
                  "schema")
    }
  )
})

test_that("is passes correct args to db_call_function localized", {
  fake_db_call_function = mock(data.frame(
    ts_key = "vts1",
    metadata = "{}"
  ))

  with_mock(
    db_tmp_read = mock(),
    db_call_function = fake_db_call_function,
    {
      db_read_ts_metadata("con", "vts1", valid_on = "2020-01-01", schema = "schema", locale = "de")

      expect_args(fake_db_call_function,
                  1,
                  "con",
                  "read_metadata_localized_raw",
                  list(as.Date("2020-01-01"), "de"),
                  "schema")
    }
  )
})

test_with_fresh_db(con, "by default it reads the most recent valid vintage", {
  result <- db_read_ts_metadata(con, "vts1")
  expect_equal(result,
               as.tsmeta.list(
                 list(
                   vts1 = list(
                     field = "value"
                   )
                 )
               ))
})

test_with_fresh_db(con, "reading desired vintages works", {
  result <- db_read_ts_metadata(con, "vts1", valid_on = Sys.Date() - 1)
  expect_equal(result,
               as.tsmeta.list(
                 list(
                   vts1 = list(
                     field = "old value"
                   )
                 )
               ))
})

test_with_fresh_db(con, "reading via regex works", {
  result <- db_read_ts_metadata(con, "vts", regex = TRUE)
  expect_equal(result,
               as.tsmeta.list(
                 list(
                   vts1 = list(
                     field = "value"
                   ),
                   vts2 = list(
                     field = "value",
                     other_field = 3
                   )
                 )
               ))
})

context("read localized metadata")

test_with_fresh_db(con, "by default it reads the most recent valid vintage", {
  result <- db_read_ts_metadata(con, "vts1", locale = "de")
  expect_equal(result,
               as.tsmeta.list(
                 list(
                   vts1 = list(
                     label = "versionierte zeitreihe 1, version 2"
                   )
                 )
               ),
               check.attributes = FALSE)
})

test_with_fresh_db(con, "reading desired vintages works", {
  result <- db_read_ts_metadata(con, "vts1", valid_on = Sys.Date() - 1, locale = "de")
  expect_equal(result,
               as.tsmeta.list(
                 list(
                   vts1 = list(
                     label = "versionierte zeitreihe 1"
                   )
                 )
               ),
               check.attributes = FALSE)
})

test_with_fresh_db(con, "reading via regex works", {
  result <- db_read_ts_metadata(con, "vts", regex = TRUE, locale = "en")
  expect_equal(result,
               as.tsmeta.list(
                 list(
                   vts1 = list(
                     label = "vintage time series 1"
                   ),
                   vts2 = list(
                     label = "vintage time series 2"
                   )
                 )
               ),
               check.attributes = FALSE)
})
