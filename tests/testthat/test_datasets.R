context("datasets")

# Seeing as the R functions are just wrappers, these are integration only

if(is_test_db_reachable()) {
  con_admin <- connect_to_test_db()
  con_writer <- connect_to_test_db("dev_writer")
  con_reader <- connect_to_test_db("dev_reader_public")
}


# test db_create_dataset --------------------------------------------------

test_with_fresh_db(con_admin, "creating dataset returns id of set", hard_reset = TRUE, {
    out <- db_create_dataset(con_admin, "testset", "a set for testing", meta(field = "value"))

  expect_equal(out, "testset")
})

test_with_fresh_db(con_admin, "writer may not create sets", hard_reset = TRUE, {
    expect_error(
    db_create_dataset(con_writer, "testset", "a set for testing", meta(field = "value")),
    "Only admins")
})

test_with_fresh_db(con_admin, "writer may not create sets", hard_reset = TRUE, {
    expect_error(
    db_create_dataset(con_reader, "testset", "a set for testing", meta(field = "value")),
    "Only admins")
})

test_with_fresh_db(con_admin, "creating dataset", hard_reset = TRUE, {
    db_create_dataset(con_admin, "testset", "a set for testing", meta(field = "value"))
  result <- dbGetQuery(con_admin, "SELECT * FROM timeseries.datasets")

  expect_is(result$set_md, "pq_json")

  result$set_md <- as.character(result$set_md)

  expect_equal(result, data.frame(
    set_id = c("default", "testset"),
    set_description = c(
      "A set that is used if no other set is specified. Every time series needs to be part of a dataset",
      "a set for testing"),
    set_md = c(
      NA_character_,
      '{"field":"value"}'),
    stringsAsFactors = FALSE
  ))
})

test_with_fresh_db(con_admin, "no duplicated set ids", hard_reset = TRUE, {
    db_create_dataset(con_admin, "testset", "a set for testing", meta(field = "value"))
  expect_error(
    db_create_dataset(con_admin, "testset", "a set for testing", meta(field = "value")),
    "name already exists")
})

test_with_fresh_db(con_admin, "defaults for description and md", hard_reset = TRUE, {
    db_create_dataset(con_admin, "defaulttestset")

  result <- dbGetQuery(con_admin, "SELECT * FROM timeseries.datasets")

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
    db_get_dataset_keys(con_reader, "set1"),
    c("ts1", "ts2")
  )
})

# test db_get_dataset_id --------------------------------------------------

test_with_fresh_db(con_admin, "db_get_dataset_id gets the correct set", {
    out <- db_get_dataset_id(con_reader, "ts1")
  expected <- data.frame(
    ts_key = "ts1",
    set_id = "set1",
    stringsAsFactors = FALSE
  )

  expect_equal(out, expected)
})

test_with_fresh_db(con_admin, "db_get_dataset_id spanning multiple sets", {
    out <- db_get_dataset_id(con_reader, c("ts1", "ts3"))
  expected <- data.frame(
    ts_key = c("ts1", "ts3"),
    set_id = c("set1", "set2"),
    stringsAsFactors = FALSE
  )

  expect_equal(out, expected)
})

test_with_fresh_db(con_admin, "db_get_dataset_id with missing key", {
    out <- db_get_dataset_id(con_reader, "notatskey")
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
    db_assign_dataset(con_reader, c("ts3", "ts4"), "set1"),
    "write permissions to assign")
})

test_with_fresh_db(con_admin, "db_assign_dataset returns status object", {
    out <- db_assign_dataset(con_writer, c("ts3", "ts4"), "set1")

  expect_is(out, "list")
  expect_true("status" %in% names(out))
})

test_with_fresh_db(con_admin, "db_assign_dataset works", {
    db_assign_dataset(con_writer, c("ts3", "ts4"), "set1")

  result <- dbGetQuery(con_admin, "SELECT set_id FROM timeseries.catalog WHERE ts_key ~ '[34]'")$set_id

  expect_equal(result, c("set1", "set1"))
})

test_with_fresh_db(con_admin, "db_assign_dataset warns if some keys don't exist", {
    expect_warning(
    db_assign_dataset(con_writer, c("ts1", "tsx"), "set2"),
    "tsx")
})

test_with_fresh_db(con_admin, "db_assign_dataset returns list of offending keys", {
    suppressWarnings(out <- db_assign_dataset(con_writer, c("ts1", "tsx"), "set2"))

  expect_equal(out$offending_keys, "tsx")
})

test_with_fresh_db(con_admin, "db_assign_dataset errors if set does not exist", {
    expect_error(
    db_assign_dataset(con_writer, "ts1", "notaset"),
    "notaset does not exist"
  )
})
