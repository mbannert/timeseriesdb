con <- NULL
if(is_test_db_reachable()) {
  con <- connect_to_test_db()
}

meta_fixture_df <- function(ts_key,
                            validity,
                            metadata,
                            locale = NULL) {

  if(is.null(locale)) {
    out <- data.frame(
      ts_key = ts_key,
      validity = validity,
      metadata = metadata,
      stringsAsFactors = FALSE
    )
  } else {
    out <- data.frame(
      ts_key = ts_key,
      validity = validity,
      locale = locale,
      metadata = metadata,
      stringsAsFactors = FALSE
    )
  }

  out$validity <- as.Date(out$validity)
  class(out$metadata) <- "pq_jsonb"

  out
}

# test storing md localized -----------------------------------------------
context("localized metadata")

# return values -----------------------------------------------------------


test_with_fresh_db(con, "db_store_ts_metadata localized returns 'ok'", {
  result <- db_store_ts_metadata(con,
                                 tsmeta.list(ts1 = list(field = "value")),
                                 valid_from = "2020-01-01",
                                 locale = "de")

  expect_equal(
    result,
    list(status = "ok")
  )
})

test_with_fresh_db(con, "db_store_ts_metadata localized warns on missing keys", {
  expect_warning(
    db_store_ts_metadata(con,
                         tsmeta.list(tsx = list(field = "value")),
                         valid_from = "2020-01-01",
                         locale = "de"))
})

test_with_fresh_db(con, "db_store_ts_metadata localized missing key warning contents", {
  result <- suppressWarnings(
    db_store_ts_metadata(con,
                         tsmeta.list(tsx = list(field = "value")),
                         valid_from = "2020-01-01",
                         locale = "de"))

  expect_equal(
    result,
    list(
      status = "warning",
      message = "Some keys not found in catalog",
      offending_keys = "tsx"
    )
  )
})


# db state ----------------------------------------------------------------


test_with_fresh_db(con, "db_store_ts_metadata localized stores metadata", {
  db_store_ts_metadata(con,
                       tsmeta.list(ts1 = list(field = "value")),
                       valid_from = "2020-01-01",
                       locale = "de")

  result <- dbGetQuery(con, "SELECT ts_key, validity, locale, metadata FROM timeseries.metadata_localized")
  expect_equal(
    result,
    meta_fixture_df("ts1", "2020-01-01", '{"field": "value"}', "de")
  )
})

test_with_fresh_db(con, "db_store_ts_metadata localized can add fields", {
  db_store_ts_metadata(con,
                       tsmeta.list(ts1 = list(field = "value")),
                       valid_from = "2020-01-01",
                       locale = "de")
  db_store_ts_metadata(con,
                       tsmeta.list(ts1 = list(field2 = 3)),
                       valid_from = "2020-01-01",
                       locale = "de")

  result <- dbGetQuery(con, "SELECT ts_key, validity, locale, metadata FROM timeseries.metadata_localized")
  expect_equal(
    result,
    meta_fixture_df("ts1", "2020-01-01", '{"field": "value", "field2": 3}', "de")
  )
})

test_with_fresh_db(con, "db_store_ts_metadata localized can override fields", {
  db_store_ts_metadata(con,
                       tsmeta.list(ts1 = list(field = "value")),
                       valid_from = "2020-01-01",
                       locale = "de")
  db_store_ts_metadata(con,
                       tsmeta.list(ts1 = list(field = "new_value")),
                       valid_from = "2020-01-01",
                       locale = "de")

  result <- dbGetQuery(con, "SELECT ts_key, validity, locale, metadata FROM timeseries.metadata_localized")
  expect_equal(
    result,
    meta_fixture_df("ts1", "2020-01-01", '{"field": "new_value"}', "de")
  )
})

# TODO: Test creating of multiple vintages

test_that("db_store_ts_metadata.tsmeta.dt is a simple wrapper", {
  fake_db_store_ts_metadata.tsmeta.list = mock()
  fake_as.tsmeta.list = mock("a nice tsmeta list")

  with_mock(
    db_store_ts_metadata.tsmeta.list = fake_db_store_ts_metadata.tsmeta.list,
    as.tsmeta.list = fake_as.tsmeta.list,
    {
      db_store_ts_metadata("con",
                           tsmeta.dt(data.frame(ts_key = "ts1",
                                                field = "value")),
                           locale = "de",
                           valid_from = "valid_from")

      expect_args(fake_db_store_ts_metadata.tsmeta.list,
                  1,
                  "con",
                  "a nice tsmeta list",
                  "valid_from",
                  "de",
                  "timeseries")
    }
  )
})

# test storing md unlocalized, unversioned --------------------------------
context("unlocalized metadata")

# returns -----------------------------------------------------------------


test_with_fresh_db(con, "db_store_ts_metadata unlocalized returns ok", {
  result <- db_store_ts_metadata(con,
                                 tsmeta.list(ts1 = list(field = "value")),
                                 "2020-01-01")

  expect_equal(
    result,
    list(
      status = "ok"))
})

test_with_fresh_db(con, "db_store_ts_metadata unlocalized warns on missing keys", {
  expect_warning(
    db_store_ts_metadata(con,
                         tsmeta.list(tsx = list(field = "value")),
                         "2020-01-01"))
})

test_with_fresh_db(con, "db_store_ts_metadata unlocalized missing key warning contents", {
  result <- suppressWarnings(
    db_store_ts_metadata(con,
                         tsmeta.list(tsx = list(field = "value")),
                         "2020-01-01"))

  expect_equal(
    result,
    list(
      status = "warning",
      message = "Some keys not found in catalog",
      offending_keys = "tsx"
    )
  )
})

test_with_fresh_db(con, "db_store_ts_metadata unlocalized stores metadata", {
  db_store_ts_metadata(con,
                       tsmeta.list(ts1 = list(field = "value")),
                       "2020-01-01")

  result <- dbGetQuery(con, "SELECT ts_key, validity, metadata FROM timeseries.metadata")
  expect_equal(
    result,
    meta_fixture_df("ts1", "2020-01-01", '{"field": "value"}')
  )
})

test_with_fresh_db(con, "db_store_ts_metadata unlocalized can add fields", {
  db_store_ts_metadata(con,
                       tsmeta.list(ts1 = list(field = "value")),
                       "2020-01-01")
  db_store_ts_metadata(con,
                       tsmeta.list(ts1 = list(field2 = 3)),
                       "2020-01-01")

  result <- dbGetQuery(con, "SELECT ts_key, validity, metadata FROM timeseries.metadata")
  expect_equal(
    result,
    meta_fixture_df("ts1", "2020-01-01", '{"field": "value", "field2": 3}')
  )
})

test_with_fresh_db(con, "db_store_ts_metadata unlocalized can override fields", {
  db_store_ts_metadata(con,
                       tsmeta.list(ts1 = list(field = "value")),
                       "2020-01-01")
  db_store_ts_metadata(con,
                       tsmeta.list(ts1 = list(field = "new_value")),
                       "2020-01-01")

  result <- dbGetQuery(con, "SELECT ts_key, validity, metadata FROM timeseries.metadata")
  expect_equal(
    result,
    meta_fixture_df("ts1", "2020-01-01", '{"field": "new_value"}')
  )
})

## TODO: vintage tests (create multiple, try updating previous etc)
