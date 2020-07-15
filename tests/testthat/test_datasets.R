context("datasets")

# Seeing as the R functions are just wrappers, these are integration only

if(is_test_db_reachable()) {
  con_admin <- connect_to_test_db()
  con_writer <- connect_to_test_db("dev_writer")
  con_reader <- connect_to_test_db("dev_reader_public")
}


# test db_create_dataset --------------------------------------------------

test_with_fresh_db(con_admin, "creating dataset returns id of set", hard_reset = TRUE, {
  out <- db_create_dataset(con_admin,
                           "testset",
                           "a set for testing",
                           meta(field = "value"),
                           schema = "tsdb_test")

  expect_equal(out, "testset")
})

test_with_fresh_db(con_admin, "writer may not create sets", hard_reset = TRUE, {
  expect_error(
    db_create_dataset(con_writer,
                      "testset",
                      "a set for testing",
                      meta(field = "value"),
                      schema = "tsdb_test"),
    "sufficient privileges")
})

test_with_fresh_db(con_admin, "writer may not create sets", hard_reset = TRUE, {
  expect_error(
    db_create_dataset(con_reader,
                      "testset",
                      "a set for testing",
                      meta(field = "value"),
                      schema = "tsdb_test"),
    "sufficient privileges")
})

test_with_fresh_db(con_admin, "creating dataset", hard_reset = TRUE, {
  db_create_dataset(con_admin,
                    "testset",
                    "a set for testing",
                    meta(field = "value"),
                    schema = "tsdb_test")
  result <- dbGetQuery(con_admin, "SELECT * FROM tsdb_test.datasets ORDER BY set_id")

  expect_is(result$set_md, "pq_jsonb")

  result$set_md <- as.character(result$set_md)

  expect_equal(result, data.frame(
    set_id = c("default", "testset"),
    set_description = c(
      "A set that is used if no other set is specified. Every time series needs to be part of a dataset",
      "a set for testing"),
    set_md = c(
      NA_character_,
      '{"field": "value"}'),
    stringsAsFactors = FALSE
  ))
})

test_with_fresh_db(con_admin, "no duplicated set ids", hard_reset = TRUE, {
  db_create_dataset(con_admin,
                    "testset",
                    "a set for testing",
                    meta(field = "value"),
                    schema = "tsdb_test")
  expect_error(
    db_create_dataset(con_admin,
                      "testset",
                      "a set for testing",
                      meta(field = "value"),
                      schema = "tsdb_test"),
    "name already exists")
})

test_with_fresh_db(con_admin, "defaults for description and md", hard_reset = TRUE, {
  db_create_dataset(con_admin,
                    "defaulttestset",
                    schema = "tsdb_test")

  result <- dbGetQuery(con_admin, "SELECT * FROM tsdb_test.datasets ORDER BY set_id")

  result$set_md <- as.character(result$set_md)

  expect_equal(result,
               data.frame(
                 set_id = c(
                   "default",
                   "defaulttestset"),
                 set_description = c(
                   "A set that is used if no other set is specified. Every time series needs to be part of a dataset",
                   NA_character_),
                 set_md = c(
                   NA_character_,
                   NA_character_),
                 stringsAsFactors = FALSE
               ))
})

# test db_get_dataset_keys ------------------------------------------------

test_with_fresh_db(con_admin, "db_get_dataset_keys", {
  expect_equal(
    db_get_dataset_keys(con_reader, "set1", schema = "tsdb_test"),
    c("ts1", "ts2")
  )
})

# test db_get_dataset_id --------------------------------------------------

test_with_fresh_db(con_admin, "db_get_dataset_id gets the correct set", {
  out <- db_get_dataset_id(con_reader, "ts1", schema = "tsdb_test")
  expected <- data.frame(
    ts_key = "ts1",
    set_id = "set1",
    stringsAsFactors = FALSE
  )

  expect_equal(out, expected)
})

test_with_fresh_db(con_admin, "db_get_dataset_id spanning multiple sets", {
  out <- db_get_dataset_id(con_reader, c("ts1", "ts3"), schema = "tsdb_test")
  expected <- data.frame(
    ts_key = c("ts1", "ts3"),
    set_id = c("set1", "set2"),
    stringsAsFactors = FALSE
  )

  expect_equal(out, expected)
})

test_with_fresh_db(con_admin, "db_get_dataset_id with missing key", {
  out <- db_get_dataset_id(con_reader, "notatskey", schema = "tsdb_test")
  expected <- data.frame(
    ts_key = "notatskey",
    set_id = NA_character_,
    stringsAsFactors = FALSE
  )

  expect_equal(out, expected)
})

# test db_assign_dataset --------------------------------------------------
test_with_fresh_db(con_admin, "reader may not assign dataset", {
  expect_error(
    db_assign_dataset(con_reader, c("ts3", "ts4"), "set1", schema = "tsdb_test"),
    "sufficient privileges")
})

test_with_fresh_db(con_admin, "db_assign_dataset returns status object", {
  out <- db_assign_dataset(con_writer, c("ts3", "ts4"), "set1", schema = "tsdb_test")

  expect_is(out, "list")
  expect_true("status" %in% names(out))
})

test_with_fresh_db(con_admin, "db_assign_dataset works", {
  db_assign_dataset(con_writer, c("ts3", "ts4"), "set1", schema = "tsdb_test")

  result <- dbGetQuery(con_admin, "SELECT set_id FROM tsdb_test.catalog WHERE ts_key ~ '[34]'")$set_id

  expect_equal(result, c("set1", "set1"))
})

test_with_fresh_db(con_admin, "db_assign_dataset warns if some keys don't exist", {
  expect_warning(
    db_assign_dataset(con_writer, c("ts1", "tsx"), "set2", schema = "tsdb_test"),
    "tsx")
})

test_with_fresh_db(con_admin, "db_assign_dataset returns list of offending keys", {
  suppressWarnings(out <- db_assign_dataset(con_writer, c("ts1", "tsx"), "set2", schema = "tsdb_test"))

  expect_equal(out$offending_keys, "tsx")
})

test_with_fresh_db(con_admin, "db_assign_dataset errors if set does not exist", {
  expect_error(
    db_assign_dataset(con_writer, "ts1", "notaset", schema = "tsdb_test"),
    "notaset does not exist"
  )
})


# test db_get_list_datasets --------------------------------------------------
test_with_fresh_db(con_admin, "db_list_datasets returns data frame with correct names", {
  out <- db_list_datasets(con_reader, schema = "tsdb_test")

  expected <- data.frame(
    set_id = c("default",
      "set1",
      "set2",
      "set_read"
    ),
    set_description = c("A set that is used if no other set is specified. Every time series needs to be part of a dataset",
      "test set 1",
      "test set 2",
      "where the series for read tests live"
    ),
    stringsAsFactors = FALSE
  )

  expect_equal(out, expected)
})


# update datasets ---------------------------------------------------------

test_with_fresh_db(con_admin, "writer may not update datasets", {
  expect_error(
    db_update_dataset(con_writer, "set1", "you've been haxx0rd", schema = "tsdb_test"),
    "sufficient privileges"
  )
})

test_with_fresh_db(con_admin, "mode must be one of overwrite or update", {
  expect_error(
    db_update_dataset(con_admin,
                      "set1",
                      "blabla",
                      metadata_update_mode = "super mode",
                      schema = "tsdb_test"),
    "one of" # us! one of us!
  )
})

test_with_fresh_db(con_admin, "updating set description returns status", {
  out <- db_update_dataset(con_admin, "set1", "right proper", schema = "tsdb_test")

  expect_equal(
    out,
    list(
      status = "ok"
    )
  )
})

test_with_fresh_db(con_admin, "updating set description", {
  db_update_dataset(con_admin, "set1", "right proper, mate", schema = "tsdb_test")

  res <- dbGetQuery(con_admin, "SELECT * FROM tsdb_test.datasets WHERE set_id = 'set1'")

  expect_equal(
    res$set_description,
    "right proper, mate"
  )

  expect_equal(
    as.character(res$set_md),
    "{\"testno\": 1}"
  )
})

test_with_fresh_db(con_admin, "updating set metadata, update", {
  db_update_dataset(con_admin,
                    "set1",
                    metadata = list(another_field = "hello"),
                    schema = "tsdb_test")

  res <- dbGetQuery(con_admin, "SELECT * FROM tsdb_test.datasets WHERE set_id = 'set1'")

  expect_equal(
    res$set_description,
    "test set 1"
  )

  md <- as.character(res$set_md)

  expect_match(
    md,
    '"testno": 1'
  )

  expect_match(
    md,
    '"another_field": "hello"'
  )
})

test_with_fresh_db(con_admin, "updating set metadata, overwrite", {
  db_update_dataset(con_admin,
                    "set1",
                    metadata = list(another_field = "hello"),
                    metadata_update_mode = "overwrite",
                    schema = "tsdb_test")

  res <- dbGetQuery(con_admin, "SELECT * FROM tsdb_test.datasets WHERE set_id = 'set1'")

  expect_equal(
    res$set_description,
    "test set 1"
  )

  md <- as.character(res$set_md)

  expect_false(
    grepl('"testno": 1', md)
  )

  expect_match(
    md,
    '"another_field": "hello"'
  )
})

# delete datasets ---------------------------------------------------------

context("datasets - delete")

test_with_fresh_db(con_admin, "writer may NOT delete datasets", {
  hacky_readline <- mock("set1")

  with_mock(
    readline = hacky_readline,
    {
      expect_error(db_dataset_delete(con_writer, "set1", schema = "tsdb_test"))
    }
  )
})

test_with_fresh_db(con_admin, "db_delete_dataset", {
  hacky_readline <- mock("set1")

  with_mock(
    readline = hacky_readline,
    {
      db_dataset_delete(con_admin, "set1", schema = "tsdb_test")
      result_cat <- dbGetQuery(con_admin, "SELECT set_id FROM tsdb_test.catalog WHERE set_id = 'set1'")$set_id
      result_sets <- dbGetQuery(con_admin, "SELECT set_id FROM tsdb_test.datasets WHERE set_id = 'set1'")$set_id

      expect_length(result_cat, 0)
      expect_length(result_sets, 0)
    }
  )
})

test_with_fresh_db(con_admin, "db_delete_dataset returns\"ok\"", {
  hacky_readline <- mock("set1")

  with_mock(
    readline = hacky_readline,
    {
      out <- db_dataset_delete(con_admin, "set1", schema = "tsdb_test")
      expect_equal(out, list(status = "ok"))
    }
  )
})

test_with_fresh_db(con_admin, "db_delete_dataset with missing set", {
  hacky_readline <- mock("set_un")

  with_mock(
    readline = hacky_readline,
    {
      out <- expect_warning(
        db_dataset_delete(con_admin, "set_un", schema = "tsdb_test"),
        "does not exist"
      )

      expect_equal(out, list(status = "warning", reason = "Dataset set_un does not exist."))
    }
  )
})

test_with_fresh_db(con_admin, "db_delete_dataset with wrong confirm", {
  hacky_readline <- mock("set_uno")

  with_mock(
    readline = hacky_readline,
    {
      expect_error(db_dataset_delete(con_admin, "set1", schema = "tsdb_test"))
    }
  )
})

test_that("db_dataset_delete_ errors when called directly", {
  expect_error(db_dataset_delete_("", "a", "a"), "directly")
})

test_with_fresh_db(con_admin, "db_delete_dataset with default", {
  with_mock(
    readline = mock("default"),
    {
      expect_error(
        db_dataset_delete(con_admin, "default", schema = "tsdb_test"),
        "may not be"
      )
    })
})

# trimming dataset history ------------------------------------------------

test_with_fresh_db(con_admin, "writer may not trim dataset", {
  expect_error(
    db_trim_dataset_history(con_writer, "default", as.Date("2020-01-30"), schema = "tsdb_test"),
    "sufficient privileges"
  )
})

test_with_fresh_db(con_admin, "trimming datasets returns ok", {
  out <- db_trim_dataset_history(con_admin, "default", as.Date("2020-01-30"), schema = "tsdb_test")

  expect_equal(
    out,
    list(
      status = "ok"
    )
  )
})

test_with_fresh_db(con_admin, "trimming datasets db state", {
  db_trim_dataset_history(con_admin, "default", as.Date("2020-01-30"), schema = "tsdb_test")

  mn <- dbGetQuery(con_admin, "SELECT ts_key, validity FROM tsdb_test.timeseries_main where ts_key ~ 'vts' ORDER BY ts_key")

  expect_equal(
    mn,
    data.frame(
      ts_key = c("vts1", "vts2"),
      validity = c(as.Date("2020-02-01"), as.Date("2020-02-01")),
      stringsAsFactors = FALSE
    )
  )
})
