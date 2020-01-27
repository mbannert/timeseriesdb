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


test_with_fresh_db(con, "db_store_ts_metadata stores metadata", {
  db_store_ts_metadata(con, tsmeta.list(ts1 = list(field = "value")), "de")

  result <- dbGetQuery(con, "SELECT * FROM timeseries.md_local_ts")
  expect_equal(
    result,
    meta_local_fixture_df("ts1", "de", '{"field": "value"}')
  )
})

test_with_fresh_db(con, "db_store_ts_metadata can add fields", {
  db_store_ts_metadata(con, tsmeta.list(ts1 = list(field = "value")), "de")
  db_store_ts_metadata(con, tsmeta.list(ts1 = list(field2 = 3)), "de")

  result <- dbGetQuery(con, "SELECT * FROM timeseries.md_local_ts")
  expect_equal(
    result,
    meta_local_fixture_df("ts1", "de", '{"field": "value", "field2": 3}')
  )
})

test_with_fresh_db(con, "db_store_ts_metadata can override fields", {
  db_store_ts_metadata(con, tsmeta.list(ts1 = list(field = "value")), "de")
  db_store_ts_metadata(con, tsmeta.list(ts1 = list(field = "new_value")), "de")

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
                           "de")

      expect_args(fake_db_store_ts_metadata.tsmeta.list,
                  1,
                  "con",
                  "a nice tsmeta list",
                  "de",
                  "timeseries")
    }
  )
})
