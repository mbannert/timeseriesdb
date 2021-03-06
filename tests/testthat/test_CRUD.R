context("CRUD")

con <- NULL

# same check as skip_on_cran()
on_cran <- !identical(Sys.getenv("NOT_CRAN"), "true")

if (!on_cran) {
  con <- createConObj(dbhost = "localhost",
                      dbname = "sandbox",
                      passwd = "")
  
  # Cleanest of clean slates
  dbGetQuery(con, "DROP SCHEMA timeseriesdb_unit_tests CASCADE")
  dbGetQuery(con, "CREATE SCHEMA timeseriesdb_unit_tests")
  
  # Could also write a test for that
  runCreateTables(con, "timeseriesdb_unit_tests")
}

set.seed(123)
tslist <- list()
tslist$ts1 <- ts(rnorm(20),start = c(1990,1), frequency = 4)
tslist$ts2 <- ts(rnorm(20),start = c(1990,1), frequency = 4)
tslist$ts3 <- ts(rnorm(20),start = c(1990,1), frequency = 4)
tslist$ts4 <- ts(rnorm(20),start = c(1990,1), frequency = 4)
tslist$ts5 <- ts(rnorm(20),start = c(1990,1), frequency = 4)
tslist$ts6 <- ts(rnorm(20),start = c(1990,1), frequency = 4)

meta_unlocalized <- list(
  ts1 = list(seed = 123, legacy_key = "series1"),
  ts2 = list(seed = 543, legacy_key = "series2"),
  ts3 = list(seed = 123, legacy_key = "series3"),
  ts4 = list(seed = 543, legacy_key = "series4"),
  ts5 = list(seed = 123, legacy_key = "series5"),
  ts6 = list(seed = 543, legacy_key = "series6")
)

meta.tsmeta.list <- as.tsmeta.list(meta_unlocalized)
for(i in seq_along(meta.tsmeta.list)) {
  attributes(meta.tsmeta.list[[i]]) <- c(attributes(meta.tsmeta.list[[i]]), list(locale = "de"))
}
attributes(meta.tsmeta.list) <- c(attributes(meta.tsmeta.list), list(locale = "de"))

meta.tsmeta.dt <- as.tsmeta.dt(meta_unlocalized)

# Test that ..... ##################

test_that("Time series is the same after db roundtrip",{
  skip_on_cran()
  
  # Store Series
  storeTimeSeries(con, tslist, schema = "timeseriesdb_unit_tests")
  
  # Read Series
  result <- readTimeSeries(con, "ts1", schema = "timeseriesdb_unit_tests")
  
  expect_equal(tslist$ts1, result$ts1)
})

test_that("Preventing overwrites works", {
  skip_on_cran()
  
  expect_message(stored <- storeTimeSeries(con, list(ts1 = tslist$ts2), schema = "timeseriesdb_unit_tests", overwrite = FALSE))
  expect_is(stored, "list")
  
  
  out <- readTimeSeries(con, "ts1", schema = "timeseriesdb_unit_tests")
  
  expect_equal(out$ts1, tslist$ts1)
})

test_that("We have two localized meta data objects. I.e. one does not overwrite the other", {
  skip_on_cran()
  
  # create some localized meta information
  # EN
  m_en <- list(ts1 = list(
    wording = "let's have a word.",
    check = "it's english man.!! SELECTION DELETE123"
  ))
  
  # DE
  m_de <- list(ts1 = list(
    wording = "Wir müssen uns mal unterhalten......",
    check = "Das ist deutsch. wirklich"
  ))
  
  storeMetaInformation(con,
                       m_en,
                       locale = "en",
                       tbl = "meta_data_localized",
                       schema = "timeseriesdb_unit_tests")
  
  storeMetaInformation(con,
                       m_de,
                       locale = "de",
                       tbl = "meta_data_localized",
                       schema = "timeseriesdb_unit_tests")
  
  mil_record_count <- dbGetQuery(con,"SELECT COUNT(*) FROM timeseriesdb_unit_tests.meta_data_localized WHERE ts_key = 'ts1'")$count
  
  expect_equal(mil_record_count,2)
})

test_that("After succesful store, delete is also successful,i.e., same amount of series", {
  skip_on_cran()
  
  tsl <- list()
  for(i in seq_along(1:21000)){
    tsl[[i]] <- ts(rnorm(20),start=c(1991,1),frequency = 12)
  }
  names(tsl) <- paste0("series",1:21000)
  
  count_before <- dbGetQuery(con, "SELECT COUNT(*) FROM timeseriesdb_unit_tests.timeseries_main")$count
  storeTimeSeries(con, tsl, schema = "timeseriesdb_unit_tests")
  deleteTimeSeries(con, names(tsl), schema = "timeseriesdb_unit_tests")
  count_after <- dbGetQuery(con, "SELECT COUNT(*) FROM timeseriesdb_unit_tests.timeseries_main")$count
  expect_equal(count_before,count_after)
})

# TOmaybeDO: this is not 100% kosher as it depends on an earlier test
# but since there are no before/after each test methods for setting up
# / tearing down DB state... meh.
test_that("Unlocalized meta data can be written to db in chunks.", {
  skip_on_cran()
  
  storeMetaInformation(con, meta_unlocalized, chunksize = 2,
                       schema = "timeseriesdb_unit_tests")
  
  mdul_count <- dbGetQuery(con,"SELECT COUNT(*) 
                           FROM timeseriesdb_unit_tests.meta_data_unlocalized 
                           WHERE ts_key ~ 'ts[1-6]' 
                           AND meta_data IS NOT NULL")$count
  expect_equal(mdul_count, 6)
})

test_that("storing tsmeta.list works", {
  skip_on_cran()
  
  dbGetQuery(con, "DELETE FROM timeseriesdb_unit_tests.meta_data_localized")
  storeMetaInformation(con, meta.tsmeta.list,
                       schema = "timeseriesdb_unit_tests",
                       locale = "de", tbl = "meta_data_localized")
  m <- readMetaInformation(con, "ts1", schema = "timeseriesdb_unit_tests")
  expect_is(m, "tsmeta.list")
  expect_is(m[[1]], "tsmeta")
  expect_equal(m[[1]]$legacy_key, meta.tsmeta.list[[1]]$legacy_key)
  expect_equal(as.numeric(m[[1]]$seed), meta.tsmeta.list[[1]]$seed)
  expect_true(length(setdiff(
    names(m[[1]]),
    c("seed", "legacy_key", "md_generated_by", "md_resource_last_update", "md_coverage_temp"))
    ) == 0)
})

test_that("storing tsmeta.dt works", {
  skip_on_cran()
  
  dbGetQuery(con, "DELETE FROM timeseriesdb_unit_tests.meta_data_localized")
  storeMetaInformation(con, meta.tsmeta.dt, schema = "timeseriesdb_unit_tests", locale = "de", tbl = "meta_data_localized")
  m <- readMetaInformation(con, "ts1", schema = "timeseriesdb_unit_tests", as_list = FALSE)
  expect_is(m, "tsmeta.dt")
  expect_equal(m[1, as.numeric(seed)], meta.tsmeta.dt[1, seed])
  expect_equal(m[1, legacy_key], meta.tsmeta.dt[1, legacy_key])
  expect_true(length(setdiff(
    names(m),
    c("ts_key", "seed", "legacy_key", "md_generated_by", "md_resource_last_update", "md_coverage_temp"))
  ) == 0)
})

if(!on_cran) {
  dbDisconnect(con)
}
