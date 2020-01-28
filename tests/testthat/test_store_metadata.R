context("store per-ts-metadata")

con <- NULL
if(is_test_db_reachable()) {
  con <- connect_to_test_db()
}

meta_local_fixture_df <- function(ts_key,
                                  lang,
                                  data_desc) {
  out <- data.frame(
    ts_key = ts_key,
    lang = lang,
    data_desc = data_desc,
    stringsAsFactors = FALSE
  )

  class(out$data_desc) <- "pq_jsonb"

  out
}

meta_unlocal_fixture_df <- function(ts_key,
                                  data_desc) {
  out <- data.frame(
    ts_key = ts_key,
    data_desc = data_desc,
    stringsAsFactors = FALSE
  )

  class(out$data_desc) <- "pq_jsonb"

  out
}

meta_local_versioned_fixture_df <- function(id,
                                            lang,
                                            data_desc) {
  out <- data.frame(
    vintage_id = id,
    lang = lang,
    meta_data = data_desc,
    stringsAsFactors = FALSE
  )

  class(out$meta_data) <- "pq_jsonb"

  out
}

# test storing md localized, unversioned --------------------------------

# return values -----------------------------------------------------------


test_with_fresh_db(con, "db_store_ts_metadata localized returns 'ok'", {
  result <- db_store_ts_metadata(con, tsmeta.list(ts1 = list(field = "value")), locale = "de")

  expect_equal(
    result,
    list(status = "ok")
  )
})

test_with_fresh_db(con, "db_store_ts_metadata localized warns on missing keys", {
  expect_warning(
    db_store_ts_metadata(con,
                         tsmeta.list(tsx = list(field = "value")),
                         locale = "de"))
})

test_with_fresh_db(con, "db_store_ts_metadata localized missing key warning contents", {
  result <- suppressWarnings(
    db_store_ts_metadata(con,
                         tsmeta.list(tsx = list(field = "value")),
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

test_with_fresh_db(con, "db_store_ts_metadata localized returns 'ok'", {
  result <- db_store_ts_metadata(con, tsmeta.list(ts1 = list(field = "value")), locale = "de")

  expect_equal(
    result,
    list(status = "ok")
  )
})


# db state ----------------------------------------------------------------


test_with_fresh_db(con, "db_store_ts_metadata localized stores metadata", {
  db_store_ts_metadata(con, tsmeta.list(ts1 = list(field = "value")), locale = "de")

  result <- dbGetQuery(con, "SELECT * FROM timeseries.md_local_ts")
  expect_equal(
    result,
    meta_local_fixture_df("ts1", "de", '{"field": "value"}')
  )
})

test_with_fresh_db(con, "db_store_ts_metadata localized can add fields", {
  db_store_ts_metadata(con, tsmeta.list(ts1 = list(field = "value")), locale = "de")
  db_store_ts_metadata(con, tsmeta.list(ts1 = list(field2 = 3)), locale = "de")

  result <- dbGetQuery(con, "SELECT * FROM timeseries.md_local_ts")
  expect_equal(
    result,
    meta_local_fixture_df("ts1", "de", '{"field": "value", "field2": 3}')
  )
})

test_with_fresh_db(con, "db_store_ts_metadata localized can override fields", {
  db_store_ts_metadata(con, tsmeta.list(ts1 = list(field = "value")), locale = "de")
  db_store_ts_metadata(con, tsmeta.list(ts1 = list(field = "new_value")), locale = "de")

  result <- dbGetQuery(con, "SELECT * FROM timeseries.md_local_ts")
  expect_equal(
    result,
    meta_local_fixture_df("ts1", "de", '{"field": "new_value"}')
  )
})

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


test_with_fresh_db(con, "db_store_ts_metadata unlocalized stores metadata", {
  db_store_ts_metadata(con, tsmeta.list(ts1 = list(field = "value")))

  result <- dbGetQuery(con, "SELECT ts_key, data_desc FROM timeseries.catalog WHERE ts_key = 'ts1'")
  expect_equal(
    result,
    meta_unlocal_fixture_df("ts1", '{"field": "value"}')
  )
})

test_with_fresh_db(con, "db_store_ts_metadata unlocalized can add fields", {
  db_store_ts_metadata(con, tsmeta.list(ts1 = list(field = "value")))
  db_store_ts_metadata(con, tsmeta.list(ts1 = list(field2 = 3)))

  result <- dbGetQuery(con, "SELECT ts_key, data_desc FROM timeseries.catalog WHERE ts_key = 'ts1'")
  expect_equal(
    result,
    meta_unlocal_fixture_df("ts1", '{"field": "value", "field2": 3}')
  )
})

test_with_fresh_db(con, "db_store_ts_metadata unlocalized can override fields", {
  db_store_ts_metadata(con, tsmeta.list(ts1 = list(field = "value")))
  db_store_ts_metadata(con, tsmeta.list(ts1 = list(field = "new_value")))

  result <- dbGetQuery(con, "SELECT ts_key, data_desc FROM timeseries.catalog WHERE ts_key = 'ts1'")
  expect_equal(
    result,
    meta_unlocal_fixture_df("ts1", '{"field": "new_value"}')
  )
})



# test storing md localized, versioned ----------------------------------------------

test_with_fresh_db(con, "db_store_ts_metadata localized versioned stores metadata", {
  db_store_ts_metadata(con,
                       tsmeta.list(vts1 = list(field = "value")),
                       valid_from = "2020-02-01",
                       locale = "de")

  result <- dbGetQuery(con, "SELECT * FROM timeseries.md_local_vintages")
  expect_equal(
    result,
    meta_local_versioned_fixture_df("f6aa6c70-41ae-11ea-b77f-2e728ce88125",
                                    "de", '{"field": "value"}')
  )
})

test_with_fresh_db(con, "db_store_ts_metadata localized can add fields", {
  db_store_ts_metadata(con,
                       tsmeta.list(vts1 = list(field = "value")),
                       valid_from = "2020-02-01",
                       locale = "de")
  db_store_ts_metadata(con,
                       tsmeta.list(vts1 = list(field2 = 3)),
                       valid_from = "2020-02-01",
                       locale = "de")

  result <- dbGetQuery(con, "SELECT * FROM timeseries.md_local_vintages")
  expect_equal(
    result,
    meta_local_versioned_fixture_df("f6aa6c70-41ae-11ea-b77f-2e728ce88125",
                                    "de", '{"field": "value", "field2": 3}')
  )
})

test_with_fresh_db(con, "db_store_ts_metadata localized can override fields", {
  db_store_ts_metadata(con,
                       tsmeta.list(vts1 = list(field = "value")),
                       valid_from = "2020-02-01",
                       locale = "de")
  db_store_ts_metadata(con,
                       tsmeta.list(vts1 = list(field = "new_value")),
                       valid_from = "2020-02-01",
                       locale = "de")

  result <- dbGetQuery(con, "SELECT * FROM timeseries.md_local_vintages")
  expect_equal(
    result,
    meta_local_versioned_fixture_df("f6aa6c70-41ae-11ea-b77f-2e728ce88125",
                                    "de", '{"field": "new_value"}')
  )
})
