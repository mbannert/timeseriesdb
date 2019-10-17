context("store_records once and forever (use case 3)")

tsl_a <- list(
  ts1 = ts(1:4, 2019, frequency = 12),
  ts2 = ts(1:5, 2019, frequency = 4)
)
class(tsl_a) <- c("tslist", "list")

tsl_b <- list(
  ts1 = ts(10:40, 2019, frequency = 12), # yeah yeah, they have different lengths...
  ts2 = ts(10:50, 2019, frequency = 4)   # Will some day somebody look at this code and think "2019... that's when my grandpa was alive."
)
class(tsl_b) <- c("tslist", "list")

## Test data generated with following code:
# 
# dbExecute(con, "DELETE FROM timeseries.timeseries_main")
# dbExecute(con, "DELETE FROM timeseries.releases")
# 
# store_time_series(con, tsl_a, "test", "public")
# 
# releases_after_insert_1 <- dbGetQuery(con, "SELECT * FROM timeseries.releases")
# main_after_insert_1 <- dbGetQuery(con, "SELECT * FROM timeseries.timeseries_main")
# 
# store_time_series(con, tsl_b, "test", "public")
# 
# releases_after_insert_2 <- dbGetQuery(con, "SELECT * FROM timeseries.releases")
# main_after_insert_2 <- dbGetQuery(con, "SELECT * FROM timeseries.timeseries_main")
# 
# save(
#   releases_after_insert_1,
#   main_after_insert_1,
#   releases_after_insert_2,
#   main_after_insert_2,
#   file = "tests/testdata/c3_store_records_data.RData"
# )

con <- NULL
if(is_test_db_reachable()) {
  con <- connect_to_test_db()
}

load("../testdata/c3_store_records_data.RData")

test_that("Inserts produce valid state", {
  skip_on_cran()
  skip_if_not(is_test_db_reachable())
  
  reset_db(con)
  
  store_time_series(con, tsl_a, "test", "public")
  expect_equal(
    dbGetQuery(con, "SELECT * FROM timeseries.releases")[, -1],
    releases_after_insert_1[, -1]
  )
  expect_equal(
    dbGetQuery(con, "SELECT * FROM timeseries.timeseries_main")[, -3],
    main_after_insert_1[, -3]
  )
  
  store_time_series(con, tsl_b, "test", "public")
  expect_equal(
    dbGetQuery(con, "SELECT * FROM timeseries.releases")[, -1],
    releases_after_insert_2[, -1]
  )
  expect_equal(
    dbGetQuery(con, "SELECT * FROM timeseries.timeseries_main")[, -3],
    main_after_insert_2[, -3]
  )
})