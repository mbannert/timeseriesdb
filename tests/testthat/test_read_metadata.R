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

  fake_db_with_tmp_read <- function(con, keys, regex, code, schema){force(code)}

  with_mock(
    db_call_function = fake_db_call_function,
    db_with_tmp_read = fake_db_with_tmp_read,
    {
      db_meta_read("con", "vts1", valid_on = "2020-01-01", schema = "schema")

      expect_args(fake_db_call_function,
                  1,
                  "con",
                  "read_metadata_raw",
                  list(valid_on = as.Date("2020-01-01")),
                  "schema")
    }
  )
})

test_that("is passes correct args to db_call_function localized", {
  fake_db_call_function = mock(data.frame(
    ts_key = "vts1",
    metadata = "{}"
  ))

  fake_db_with_tmp_read <- function(con, keys, regex, code, schema){force(code)}

  with_mock(
    db_with_tmp_read = fake_db_with_tmp_read,
    db_call_function = fake_db_call_function,
    {
      db_meta_read("con", "vts1", valid_on = "2020-01-01", schema = "schema", locale = "de")

      expect_args(fake_db_call_function,
                  1,
                  "con",
                  "read_metadata_localized_raw",
                  list(valid_on = as.Date("2020-01-01"), loc = "de"),
                  "schema")
    }
  )
})

test_with_fresh_db(con_admin, "by default it reads the most recent valid vintage", {
  result <- db_meta_read(con_reader, "vts1", schema = "tsdb_test")
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
  result <- db_meta_read(con_reader, "vts1", valid_on = Sys.Date() - 1, schema = "tsdb_test")
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
  result <- db_meta_read(con_reader, "vts", regex = TRUE, schema = "tsdb_test")
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
  result <- db_meta_read(con_reader, "vts1", locale = "de", schema = "tsdb_test")
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
  result <- db_meta_read(con_reader,
                                "vts1",
                                valid_on = Sys.Date() - 1,
                                locale = "de",
                                schema = "tsdb_test")
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
  result <- db_meta_read(con_reader,
                                "vts",
                                regex = TRUE,
                                locale = "en",
                                schema = "tsdb_test")
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
  result <- db_meta_get_last_update(con_reader, c("vts1"), schema = "tsdb_test")
  expect_equal(result,
               data.table(
                 ts_key = "vts1",
                 validity = Sys.Date() + 1
               ))
})

test_with_fresh_db(con_admin, "reading unlocalized edge with missing key", {
  result <- db_meta_get_last_update(
    con_reader,
    c("vts1", "blananagram"),
    schema = "tsdb_test")
  expect_equal(result,
               data.table(
                 ts_key = c("blananagram", "vts1"),
                 validity = c(as.Date(NA), Sys.Date() + 1)
               ))
})

test_with_fresh_db(con_admin, "reading unlocalized edge via regex", {
  result <- db_meta_get_last_update(con_reader,
                                     c("vts"),
                                     regex = TRUE,
                                     schema = "tsdb_test")
  expect_equal(result,
               data.table(
                 ts_key = c("vts1", "vts2"),
                 validity = Sys.Date() + 1
               ))
})

test_with_fresh_db(con_admin, "reading localized edge", {
  result <- db_meta_get_last_update(con_reader,
                                     c("vts1"),
                                     locale = "de",
                                     schema = "tsdb_test")
  expect_equal(result,
               data.table(
                 ts_key = "vts1",
                 validity = Sys.Date()
               ))
})

test_with_fresh_db(con_admin, "reading localized edge with missing key", {
  result <- db_meta_get_last_update(con_reader,
                                    c("vts1", "blananagram"),
                                    locale = "de",
                                    schema = "tsdb_test")
  expect_equal(result,
               data.table(
                 ts_key = c("blananagram", "vts1"),
                 validity = c(as.Date(NA), Sys.Date())
               ))
})

test_with_fresh_db(con_admin, "reading localized edge via regex", {
  result <- db_meta_get_last_update(con_reader,
                                     c("vts"),
                                     regex = TRUE,
                                     locale = "de",
                                     schema = "tsdb_test")
  expect_equal(result,
               data.table(
                 ts_key = c("vts1", "vts2"),
                 validity = Sys.Date()
               ))
})

test_with_fresh_db(con_admin, "SQL-only test for array version of read localized metadata", {
  out <- dbGetQuery(con_reader, "SELECT * FROM tsdb_test.read_metadata_localized_raw('{vts1, vts2}'::TEXT[], NULL, 'en')")

  expect_equal(
    out$ts_key,
    c(
      "vts1",
      "vts2"
    )
  )

  expect_match(
    out$metadata[[1]],
    '"label": "vintage time series 1"'
  )

  expect_match(
    out$metadata[[2]],
    '"label": "vintage time series 2"'
  )
})

# reading for collection --------------------------------------------------

context("reading md for collection")

test_with_fresh_db(con_admin, "by default it reads the most recent valid vintage", {
  result <- db_collection_read_meta(con_reader, "mdtest", "test", schema = "tsdb_test")

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

test_with_fresh_db(con_admin, "reading desired vintages works", {
  result <- db_collection_read_meta(con_reader,
                         "mdtest",
                         "test",
                         valid_on = Sys.Date() - 1,
                         schema = "tsdb_test")
  expect_equal(result,
               as.tsmeta.list(
                 list(
                   vts1 = list(
                     field = "old value"
                   ),
                   vts2 = list(
                     field = "value",
                     other_field = -3
                   )
                 )
               ))
})

test_with_fresh_db(con_admin, "localized, by default it reads the most recent valid vintage", {
  result <- db_collection_read_meta(con_reader, "mdtest", "test", locale = "de", schema = "tsdb_test")

  expect_equal(result,
               as.tsmeta.list(
                 list(
                   vts1 = list(
                     label = "versionierte zeitreihe 1, version 2"
                   ),
                   vts2 = list(
                     label = "versionierte zeitreihe 2, version 2"
                   )
                 )
               ),
               check.attributes = FALSE)
})


test_with_fresh_db(con_admin, "localized, by default it reads the most recent english valid vintage", {
  result <- db_collection_read_meta(con_reader, "mdtest", "test", locale = "en", schema = "tsdb_test")

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


test_with_fresh_db(con_admin, "localized, reading older vintage works", {
  result <- db_collection_read_meta(con_reader,
                                    "mdtest",
                                    "test",
                                    locale = "de",
                                    valid_on = Sys.Date() - 1,
                                    schema = "tsdb_test")

  expect_equal(result,
               as.tsmeta.list(
                 list(
                   vts1 = list(
                     label = "versionierte zeitreihe 1"
                   ),
                   vts2 = list(
                     label = "versionierte zeitreihe 2"
                   )
                 )
               ),
               check.attributes = FALSE)
})
