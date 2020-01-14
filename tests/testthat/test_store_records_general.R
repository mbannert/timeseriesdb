context("store_records general tests")

tsl <- list(
  ts1 = ts(1:4, 2019, frequency = 12),
  ts2 = ts(1:5, 2019, frequency = 4)
)
class(tsl) <- c("tslist", "list")

con <- NULL
if(is_test_db_reachable()) {
  con <- connect_to_test_db()
}

test_that("vintage id is a uuid", {
  skip_on_cran()
  skip_if_not(is_test_db_reachable())
  
  reset_db(con)
  
  store_time_series(con, tsl, access = "public", valid_from = "2019-01-01")
  
  out <- dbGetQuery(con, "SELECT * FROM timeseries.timeseries_main")
  
  expect_match(out$id, "[0-9a-fA-F]{8}\\-[0-9a-fA-F]{4}\\-[0-9a-fA-F]{4}\\-[0-9a-fA-F]{4}\\-[0-9a-fA-F]{12}")
})

test_that("it can deal with string valid_from", {
  skip_on_cran()
  skip_if_not(is_test_db_reachable())
  
  reset_db(con)
  
  expect_error(store_time_series(con, tsl, access = "public", valid_from = "2019-01-01"),
               NA)
})

test_that("it can deal with string release_date", {
  skip_on_cran()
  skip_if_not(is_test_db_reachable())
  
  reset_db(con)
  
  expect_error(store_time_series(con, tsl, access = "public", release_date = "2019-01-01"),
               NA)
})


# TODO: We should separate unit from integration tests
test_that("it performs store operations in chunks", {
  fake_dbWithTransaction <- function(con, code) {
    xy <- code # just execute the block. It works anyway...
  }
  
  fake_db_tmp_store = mock() # the one we will test against
  
  with_mock(
    dbWithTransaction = fake_dbWithTransaction,
    dbGetQuery = mock(),
    "timeseriesdb:::db_tmp_store" = fake_db_tmp_store,
    dbExecute = mock(),
    {
      store_time_series(con, tsl, access = "public", chunk_size = 1)
      
      expect_called(fake_db_tmp_store, 2)
    }
  )
})