context("store_records with vintage and no release date (use case 1)")

tsl <- list(
  ts1 = ts(1:4, 2019, frequency = 12),
  ts2 = ts(1:5, 2019, frequency = 4)
)
class(tsl) <- c("tslist", "list")

# ## Test data generated with following code:
# 
# 
# dbExecute(con, "DELETE FROM timeseries.timeseries_main")
# dbExecute(con, "DELETE FROM timeseries.releases")
# 
# store_time_series(con, tsl, "test", "public", valid_from = "2019-01-01")
# 
# releases_after_insert_1 <- dbGetQuery(con, "SELECT * FROM timeseries.releases")
# main_after_insert_1 <- dbGetQuery(con, "SELECT * FROM timeseries.timeseries_main")
# 
# store_time_series(con, tsl, "test", "public", valid_from = "2019-02-01")
# 
# releases_after_insert_2 <- dbGetQuery(con, "SELECT * FROM timeseries.releases")
# main_after_insert_2 <- dbGetQuery(con, "SELECT * FROM timeseries.timeseries_main")
# 
# store_time_series(con, tsl, "test", "public", valid_from = "2019-03-01")
# 
# releases_after_insert_3 <- dbGetQuery(con, "SELECT * FROM timeseries.releases")
# main_after_insert_3 <- dbGetQuery(con, "SELECT * FROM timeseries.timeseries_main")
# 
# save(
#   releases_after_insert_1,
#   main_after_insert_1,
#   releases_after_insert_2,
#   main_after_insert_2,
#   releases_after_insert_3,
#   main_after_insert_3,
#   file = "tests/testdata/c1_store_records_data.RData"
# )

con <- NULL
if(is_test_db_reachable()) {
  con <- connect_to_test_db()
}

load("../testdata/c1_store_records_data.RData")

test_that("Inserts produce valid state", {
  skip_on_cran()
  skip_if_not(is_test_db_reachable())
  
  reset_db(con)
  
  store_time_series(con, tsl, "test", "public", valid_from = "2019-01-01")
  expect_equal(
    dbGetQuery(con, "SELECT * FROM timeseries.releases")[, -1],
    releases_after_insert_1[, -1]
  )
  expect_equal(
    dbGetQuery(con, "SELECT * FROM timeseries.timeseries_main")[, -3],
    main_after_insert_1[, -3]
  )
  
  store_time_series(con, tsl, "test", "public", valid_from = "2019-02-01")
  expect_equal(
    dbGetQuery(con, "SELECT * FROM timeseries.releases")[, -1],
    releases_after_insert_2[, -1]
  )
  expect_equal(
    dbGetQuery(con, "SELECT * FROM timeseries.timeseries_main")[, -3],
    main_after_insert_2[, -3]
  )
  
  store_time_series(con, tsl, "test", "public", valid_from = "2019-03-01")
  expect_equal(
    dbGetQuery(con, "SELECT * FROM timeseries.releases")[, -1],
    releases_after_insert_3[, -1]
  )
  expect_equal(
    dbGetQuery(con, "SELECT * FROM timeseries.timeseries_main")[, -3],
    main_after_insert_3[, -3]
  )
})

test_that("storing multiple times on the same day is a range-wise noop", {
  skip_on_cran()
  skip_if_not(is_test_db_reachable())
  
  reset_db(con)
  
  store_time_series(con, tsl, "test", "public", valid_from = "2019-01-01")
  
  store_time_series(con, tsl, "test", "public", valid_from = "2019-02-01")
  
  store_time_series(con, tsl, "test", "public", valid_from = "2019-03-01")
  
  store_time_series(con, tsl, "test", "public", valid_from = "2019-03-01")
  
  expect_equal(
    dbGetQuery(con, "SELECT * FROM timeseries.timeseries_main")[, -3],
    main_after_insert_3[, -3]
  )
})
