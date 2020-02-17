
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
# store_time_series(con, tsl, "public", valid_from = "2019-01-01", release_date = "2019-01-02")
#
# catalog_after_insert_1 <- dbGetQuery(con, "SELECT * FROM timeseries.catalog")
# main_after_insert_1 <- dbGetQuery(con, "SELECT * FROM timeseries.timeseries_main")
#
# store_time_series(con, tsl, "public", valid_from = "2019-02-01", release_date = "2019-02-02")
#
# catalog_after_insert_2 <- dbGetQuery(con, "SELECT * FROM timeseries.catalog")
# main_after_insert_2 <- dbGetQuery(con, "SELECT * FROM timeseries.timeseries_main")
#
# store_time_series(con, tsl, "public", valid_from = "2019-03-01", release_date = "2019-03-02")
#
# catalog_after_insert_3 <- dbGetQuery(con, "SELECT * FROM timeseries.catalog")
# main_after_insert_3 <- dbGetQuery(con, "SELECT * FROM timeseries.timeseries_main")
#
# store_time_series(con, tsl_update, "public", valid_from = "2019-03-01", release_date = "2019-03-02")
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

con <- NULL
if(is_test_db_reachable()) {
  con <- connect_to_test_db()
}

load("../testdata/store_records_data.RData")

test_that("It returns a status json", {
  skip_on_cran()
  skip_if_not(is_test_db_reachable())

  reset_db(con)

  out <- store_time_series(con, tsl, "public", valid_from = "2019-01-01", release_date = "2019-01-02")
  expect_is(out, "list")
  expect_equal(out$status, "ok")
})

test_that("Inserts produce valid state", {
  skip_on_cran()
  skip_if_not(is_test_db_reachable())

  reset_db(con)

  store_time_series(con, tsl, "public", valid_from = "2019-01-01", release_date = "2019-01-02")
  expect_equal(
    dbGetQuery(con, "SELECT * FROM timeseries.catalog"),
    catalog_after_insert_1
  )
  main_names <- c("id", "ts_key", "validity", "coverage", "release_date", "created_by",
                  "created_at", "ts_data", "access")
  names_to_test <- setdiff(main_names, c("id", "created_by", "created_at"))
  expect_equal(
    dbGetQuery(con, "SELECT * FROM timeseries.timeseries_main")[, names_to_test],
    main_after_insert_1[, names_to_test]
  )

  store_time_series(con, tsl, "public", valid_from = "2019-02-01", release_date = "2019-02-02")
  expect_equal(
    dbGetQuery(con, "SELECT * FROM timeseries.catalog"),
    catalog_after_insert_2
  )
  expect_equal(
    dbGetQuery(con, "SELECT * FROM timeseries.timeseries_main")[, names_to_test],
    main_after_insert_2[, names_to_test]
  )

  store_time_series(con, tsl, "public", valid_from = "2019-03-01", release_date = "2019-03-02")
  expect_equal(
    dbGetQuery(con, "SELECT * FROM timeseries.catalog"),
    catalog_after_insert_3
  )
  expect_equal(
    dbGetQuery(con, "SELECT * FROM timeseries.timeseries_main")[, names_to_test],
    main_after_insert_3[, names_to_test]
  )
})

test_that("storing series with invalid vintages is an error", {
  skip_on_cran()
  skip_if_not(is_test_db_reachable())

  reset_db(con)

  store_time_series(con, tsl, "public", valid_from = "2019-01-01", release_date = "2019-01-02")
  store_time_series(con, tsl, "public", valid_from = "2019-02-01", release_date = "2019-02-02")
  store_time_series(con, tsl, "public", valid_from = "2019-03-01", release_date = "2019-03-02")
  failed <- store_time_series(con, tsl[1], "public", valid_from = "2019-02-01", release_date = "2019-03-02")
  expect_equal(names(failed), c("status", "reason", "offending_keys"))
  expect_equal(failed$status, "failure")
  expect_equal(failed$offending_keys, "ts1")
})

test_that("storing with edge vintage causes update", {
  skip_on_cran()
  skip_if_not(is_test_db_reachable())

  reset_db(con)

  store_time_series(con, tsl, "public", valid_from = "2019-01-01", release_date = "2019-01-02")

  store_time_series(con, tsl, "public", valid_from = "2019-02-01", release_date = "2019-02-02")

  store_time_series(con, tsl, "public", valid_from = "2019-03-01", release_date = "2019-03-02")

  store_time_series(con, tsl_update, "public", valid_from = "2019-03-01", release_date = "2019-03-02")

  expect_equal(
    dbGetQuery(con, "SELECT * FROM timeseries.timeseries_main")[, names_to_test],
    main_after_update[, names_to_test]
  )
})

test_that("overwriting older vintage is not possible", {
  skip_on_cran()
  skip_if_not(is_test_db_reachable())

  reset_db(con)

  store_time_series(con, tsl, "public", valid_from = "2019-01-01", release_date = "2019-01-02")

  store_time_series(con, tsl, "public", valid_from = "2019-02-01", release_date = "2019-02-02")

  store_time_series(con, tsl, "public", valid_from = "2019-03-01", release_date = "2019-03-02")

  store_time_series(con, tsl_update, "public", valid_from = "2019-02-01", release_date = "2019-03-02")

  expect_equal(
    dbGetQuery(con, "SELECT * FROM timeseries.timeseries_main")[, names_to_test],
    main_after_insert_3[, names_to_test]
  )
})
