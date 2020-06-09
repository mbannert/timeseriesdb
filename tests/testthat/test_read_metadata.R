if(is_test_db_reachable()) {
  con_admin <- connect_to_test_db()
  con_reader <- connect_to_test_db("dev_reader_public")
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

test_with_fresh_db(con_admin, "by default it reads the most recent valid vintage", {
  skip_if_not(is_test_db_reachable())

  result <- db_read_ts_metadata(con_reader, "vts1")
  expect_equal(result,
               as.tsmeta.list(
                 list(
                   vts1 = list(
                     field = "value"
                   )
                 )
               ))
})

test_with_fresh_db(con_admin, "reading desired vintages works", {
  skip_if_not(is_test_db_reachable())

  result <- db_read_ts_metadata(con_reader, "vts1", valid_on = Sys.Date() - 1)
  expect_equal(result,
               as.tsmeta.list(
                 list(
                   vts1 = list(
                     field = "old value"
                   )
                 )
               ))
})

test_with_fresh_db(con_admin, "reading via regex works", {
  skip_if_not(is_test_db_reachable())

  result <- db_read_ts_metadata(con_reader, "vts", regex = TRUE)
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

test_with_fresh_db(con_admin, "by default it reads the most recent valid vintage", {
  result <- db_read_ts_metadata(con_reader, "vts1", locale = "de")
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

test_with_fresh_db(con_admin, "reading desired vintages works", {
  result <- db_read_ts_metadata(con_reader, "vts1", valid_on = Sys.Date() - 1, locale = "de")
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

test_with_fresh_db(con_admin, "reading via regex works", {
  result <- db_read_ts_metadata(con_reader, "vts", regex = TRUE, locale = "en")
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


# reading current edge ----------------------------------------------------

test_with_fresh_db(con_admin, "reading unlocalized edge", {
  result <- db_get_metadata_validity(con_reader, c("vts1"))
  expect_equal(result,
               data.table(
                 ts_key = "vts1",
                 validity = Sys.Date() + 1
               ))
})

test_with_fresh_db(con_admin, "reading unlocalized edge via regex", {
  result <- db_get_metadata_validity(con_reader, c("vts"), regex = TRUE)
  expect_equal(result,
               data.table(
                 ts_key = c("vts1", "vts2"),
                 validity = Sys.Date() + 1
               ))
})

test_with_fresh_db(con_admin, "reading localized edge", {
  result <- db_get_metadata_validity(con_reader, c("vts1"), locale = "de")
  expect_equal(result,
               data.table(
                 ts_key = "vts1",
                 validity = Sys.Date()
               ))
})

test_with_fresh_db(con_admin, "reading localized edge via regex", {
  result <- db_get_metadata_validity(con_reader, c("vts"), regex = TRUE, locale = "de")
  expect_equal(result,
               data.table(
                 ts_key = c("vts1", "vts2"),
                 validity = Sys.Date()
               ))
})
