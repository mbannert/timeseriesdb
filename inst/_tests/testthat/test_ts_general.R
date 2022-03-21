if(is_test_db_reachable()) {
  con_admin <- connect_to_test_db()
  con_writer <- connect_to_test_db("dev_writer")
}

context("renaming ts")

test_with_fresh_db(con_admin, "db_ts_rename returns status", {
  out <- db_ts_rename(con_admin, "rts1", "rts1_the_white", schema = "tsdb_test")

  expect_equal(
    out,
    list(
      status = "ok"
    )
  )
})

test_with_fresh_db(con_admin, "db_ts_rename is an admin thing", {
  expect_error(
    db_ts_rename(con_writer, "rts1", "rts1_the_white", schema = "tsdb_test"),
    "sufficient privileges"
  )
})

test_with_fresh_db(con_admin, "db_ts_rename with missing keys warns", {
  expect_warning(
    db_ts_rename(con_admin,
                     c("rts1", "rts3"),
                     c("rts1_2", "rts3_2"),
                     schema = "tsdb_test"),
    "in the catalog"
  )
})

test_with_fresh_db(con_admin, "db_ts_rename with missing keys return", {
  out <- suppressWarnings(db_ts_rename(con_admin,
                      c("rts1", "rts3"),
                      c("rts1_2", "rts3_2"),
                      schema = "tsdb_test"))

  expect_equal(
    out,
    list(
      status = "warning",
      message = "Some keys not found in the catalog.",
      offending_keys = c("rts3")
    )
  )
})

test_with_fresh_db(con_admin, "db_ts_rename stops with different lengths", {
  expect_error(
    db_ts_rename(con_admin,
                 c("a", "b"),
                 c("c"),
                 schema = "tsdb_test"),
    "same length"
  )
})

test_with_fresh_db(con_admin, "db_ts_rename works", {
  db_ts_rename(con_admin, "vts1", "timmothy_smith", schema = "tsdb_test")

  cat <- dbGetQuery(con_admin, "SELECT * FROM tsdb_test.catalog WHERE ts_key = 'timmothy_smith'")

  expect_equal(
    cat,
    data.frame(
      ts_key = "timmothy_smith",
      set_id = "default",
      stringsAsFactors = FALSE
    )
  )

  coll <- dbGetQuery(con_admin, "SELECT * FROM tsdb_test.collect_catalog WHERE ts_key = 'timmothy_smith' ORDER BY id")

  expect_equal(
    coll,
    data.frame(
      id = c(
        "09bb7ef8-127a-4fcd-8122-399debb9ed61",
        "bc4ad558-516a-11ea-8d77-2e728ce88125"
      ),
      ts_key = c(
        "timmothy_smith",
        "timmothy_smith"
      ),
      stringsAsFactors = FALSE
    )
  )

  md <- dbGetQuery(con_admin, "SELECT * FROM tsdb_test.metadata WHERE ts_key = 'timmothy_smith' ORDER BY validity")

  expect_equal(
    md$id,
    c(
      "1b6277fe-4378-11ea-b77f-2e728ce88125",
      "079eaf0e-4c00-11ea-b77f-2e728ce88125",
      "079eb3aa-4c00-11ea-b77f-2e728ce88125")
  )

  mdl <- dbGetQuery(con_admin, "SELECT * FROM tsdb_test.metadata_localized WHERE ts_key = 'timmothy_smith' ORDER BY locale, validity")

  expect_equal(
    mdl$id,
    c(
      "1b628578-4378-11ea-b77f-2e728ce88125",
      "1b628802-4378-11ea-b77f-2e728ce88125",
      "1b627e48-4378-11ea-b77f-2e728ce88125"
    )
  )

  tsm <- dbGetQuery(con_admin, "SELECT * FROM tsdb_test.timeseries_main WHERE ts_key = 'timmothy_smith' ORDER BY release_date")

  expect_equal(
    tsm$id,
    c(
      "f6aa69c8-41ae-11ea-b77f-2e728ce88125",
      "f6aa6c70-41ae-11ea-b77f-2e728ce88125"
    )
  )
})
