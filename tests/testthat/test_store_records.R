
context("store_records")

tsl <- list(
  ts1 = ts(1:4, 2019, frequency = 12),
  ts2 = ts(1:5, 2019, frequency = 4)
)
class(tsl) <- c("tslist", "list")

tsl_update <- list(
  ts1 = ts(rep(5, 4), 2019, frequency = 12)
)
class(tsl_update) <- c("tslist", "list")

main_names <- c("id", "ts_key", "validity", "coverage", "release_date", "created_by",
                "created_at", "ts_data", "access")
names_to_test <- setdiff(main_names, c("id", "created_by", "created_at"))

# Test data generated with following code:
# con <- connect_to_test_db()
#
# dbExecute(con, "DELETE FROM timeseries.timeseries_main")
# dbExecute(con, "DELETE FROM timeseries.catalog")
#
# store_time_series(con, tsl, "timeseries_access_public", valid_from = "2019-01-01", release_date = "2019-01-02")
#
# catalog_after_insert_1 <- dbGetQuery(con, "SELECT * FROM timeseries.catalog")
# main_after_insert_1 <- dbGetQuery(con, "SELECT * FROM timeseries.timeseries_main")
#
# store_time_series(con, tsl, "timeseries_access_public", valid_from = "2019-02-01", release_date = "2019-02-02")
#
# catalog_after_insert_2 <- dbGetQuery(con, "SELECT * FROM timeseries.catalog")
# main_after_insert_2 <- dbGetQuery(con, "SELECT * FROM timeseries.timeseries_main")
#
# store_time_series(con, tsl, "timeseries_access_public", valid_from = "2019-03-01", release_date = "2019-03-02")
#
# catalog_after_insert_3 <- dbGetQuery(con, "SELECT * FROM timeseries.catalog")
# main_after_insert_3 <- dbGetQuery(con, "SELECT * FROM timeseries.timeseries_main")
#
# store_time_series(con, tsl_update, "timeseries_access_public", valid_from = "2019-03-01", release_date = "2019-03-02")
#
# main_after_update <- dbGetQuery(con, "SELECT * FROM timeseries.timeseries_main")
#
# save(
#   catalog_after_insert_1,
#   main_after_insert_1,
#   catalog_after_insert_2,
#   main_after_insert_2,
#   catalog_after_insert_3,
#   main_after_insert_3,
#   main_after_update,
#   file = "tests/testdata/store_records_data.RData"
# )

if(is_test_db_reachable()) {
  con_admin <- connect_to_test_db()
  con_writer <- connect_to_test_db("dev_writer")
  con_reader <- connect_to_test_db("dev_reader_main")
}

load("../testdata/store_records_data.RData")

test_with_fresh_db(con_admin, "reader may not store", {
  expect_error(store_time_series(con_reader,
                   tsl,
                   "timeseries_access_public",
                   valid_from = "2019-01-01",
                   release_date = "2019-01-02"),
               "may store")
})

test_with_fresh_db(con_admin, "It returns a status json", {
  out <- store_time_series(con_writer, tsl, "timeseries_access_public", valid_from = "2019-01-01", release_date = "2019-01-02")
  expect_is(out, "list")
  expect_equal(out$status, "ok")
})

test_with_fresh_db(con_admin, hard_reset = TRUE, "Inserts produce valid state", {
  store_time_series(con_writer, tsl, "timeseries_access_public", valid_from = "2019-01-01", release_date = "2019-01-02")
  expect_equal(
    dbGetQuery(con_admin, "SELECT * FROM timeseries.catalog"),
    catalog_after_insert_1
  )
  main_names <- c("id", "ts_key", "validity", "coverage", "release_date", "created_by",
                  "created_at", "ts_data", "access")
  names_to_test <- setdiff(main_names, c("id", "created_by", "created_at"))
  expect_equal(
    dbGetQuery(con_admin, "SELECT * FROM timeseries.timeseries_main")[, names_to_test],
    main_after_insert_1[, names_to_test]
  )

  store_time_series(con_writer, tsl, "timeseries_access_public", valid_from = "2019-02-01", release_date = "2019-02-02")
  expect_equal(
    dbGetQuery(con_admin, "SELECT * FROM timeseries.catalog"),
    catalog_after_insert_2
  )
  expect_equal(
    dbGetQuery(con_admin, "SELECT * FROM timeseries.timeseries_main")[, names_to_test],
    main_after_insert_2[, names_to_test]
  )

  store_time_series(con_writer, tsl, "timeseries_access_public", valid_from = "2019-03-01", release_date = "2019-03-02")
  expect_equal(
    dbGetQuery(con_admin, "SELECT * FROM timeseries.catalog"),
    catalog_after_insert_3
  )
  expect_equal(
    dbGetQuery(con_admin, "SELECT * FROM timeseries.timeseries_main")[, names_to_test],
    main_after_insert_3[, names_to_test]
  )
})

test_with_fresh_db(con_admin, hard_reset = TRUE, "storing series with invalid vintages is an error", {
  store_time_series(con_writer, tsl, "timeseries_access_public", valid_from = "2019-01-01", release_date = "2019-01-02")
  store_time_series(con_writer, tsl, "timeseries_access_public", valid_from = "2019-02-01", release_date = "2019-02-02")
  store_time_series(con_writer, tsl, "timeseries_access_public", valid_from = "2019-03-01", release_date = "2019-03-02")
  failed <- store_time_series(con_writer, tsl[1], "public", valid_from = "2019-02-01", release_date = "2019-03-02")
  expect_equal(names(failed), c("status", "message", "offending_keys"))
  expect_equal(failed$status, "warning")
  expect_equal(failed$offending_keys, "ts1")
})

test_with_fresh_db(con_admin, hard_reset = TRUE, "storing with edge vintage causes update", {
  store_time_series(con_writer, tsl, "timeseries_access_public", valid_from = "2019-01-01", release_date = "2019-01-02")

  store_time_series(con_writer, tsl, "timeseries_access_public", valid_from = "2019-02-01", release_date = "2019-02-02")

  store_time_series(con_writer, tsl, "timeseries_access_public", valid_from = "2019-03-01", release_date = "2019-03-02")

  store_time_series(con_writer, tsl_update, "timeseries_access_public", valid_from = "2019-03-01", release_date = "2019-03-02")

  expect_equal(
    dbGetQuery(con_admin, "SELECT * FROM timeseries.timeseries_main")[, names_to_test],
    main_after_update[, names_to_test]
  )
})

test_with_fresh_db(con_admin, hard_reset = TRUE, "overwriting older vintage is not possible", {
  store_time_series(con_writer, tsl, "timeseries_access_public", valid_from = "2019-01-01", release_date = "2019-01-02")

  store_time_series(con_writer, tsl, "timeseries_access_public", valid_from = "2019-02-01", release_date = "2019-02-02")

  store_time_series(con_writer, tsl, "timeseries_access_public", valid_from = "2019-03-01", release_date = "2019-03-02")

  store_time_series(con_writer, tsl_update, "timeseries_access_public", valid_from = "2019-02-01", release_date = "2019-03-02")

  expect_equal(
    dbGetQuery(con_admin, "SELECT * FROM timeseries.timeseries_main")[, names_to_test],
    main_after_insert_3[, names_to_test]
  )
})
