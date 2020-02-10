con <- NULL
if(is_test_db_reachable()) {
  con <- connect_to_test_db()
}

context("read unlocalized metadata")

test_with_fresh_db(con, "it can return md as data.table", {
  result <- db_read_ts_metadata(con, "vts1", as.dt = TRUE)
  expect_equal(result,
               as.tsmeta.dt(
                 data.table(
                   ts_key = "vts1",
                   field = "value"
                 )
               ))
})

test_with_fresh_db(con, "it fills missing fields in dt mode", {
  result <- db_read_ts_metadata(con, c("vts1", "vts2"), as.dt = TRUE)
  expect_true(result[ts_key == "vts1", is.na(other_field)])
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

test_with_fresh_db(con, "it can return md as data.table", {
  result <- db_read_ts_metadata(con, "vts1", as.dt = TRUE, locale = "en")
  expect_equal(result,
               as.tsmeta.dt(
                 data.table(
                   ts_key = "vts1",
                   label = "vintage time series 1"
                 )
               ), check.attributes = FALSE)
})

test_with_fresh_db(con, "it attaches a locale attribute in dt mode", {
  result <- db_read_ts_metadata(con, "vts1", as.dt = TRUE, locale = "en")
  atts <- attributes(result)

  expect_match(names(atts), "locale", all = FALSE)
  expect_equal(atts$locale, "en")
})

test_with_fresh_db(con, "it attaches locale attribute in list mode", {
  result <- db_read_ts_metadata(con, "vts1", locale = "de")
  atts <- attributes(result)

  expect_match(names(atts), "locale", all = FALSE)
  expect_equal(atts$locale, "de")
})

test_with_fresh_db(con, "it attaches locale attribute to elements in list mode", {
  result <- db_read_ts_metadata(con, "vts1", locale = "de")

  atts <- attributes(result$vts1)
  expect_match(names(atts), "locale", all = FALSE)
  expect_equal(atts$locale, "de")
})

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
