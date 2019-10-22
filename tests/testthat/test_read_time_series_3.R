context("read_time_series, case 3")

con <- NULL
if(is_test_db_reachable()) {
  con <- connect_to_test_db()
}

reset_db(con)

tsl_case_3 <- list(
  just_the_one = ts(rep(3, 10), 2019, frequency = 4)
)
class(tsl_case_3) <- c("tslist", "list")

store_time_series(con, tsl_case_3, "ts for testing read case 3", "public")

test_that("reading a simple ts works", {
  skip_on_cran()
  skip_if_not(is_test_db_reachable())
  
  tsl_read <- read_time_series(con, "just_the_one")
  expect_equal(tsl_read, tsl_case_3)
})