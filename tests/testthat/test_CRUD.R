con <- NULL

# same check as skip_on_cran()
on_cran <- !identical(Sys.getenv("NOT_CRAN"), "true")

if (!on_cran) {
  con <- createConObj(dbhost = "localhost",
                    dbname = "sandbox",
                    passwd = "")
}

set.seed(123)
tslist <- list()
tslist$ts1 <- ts(rnorm(20),start = c(1990,1), frequency = 4)
tslist$ts2 <- ts(rnorm(20),start = c(1990,1), frequency = 4)
tslist$ts3 <- ts(rnorm(20),start = c(1990,1), frequency = 4)
tslist$ts4 <- ts(rnorm(20),start = c(1990,1), frequency = 4)
tslist$ts5 <- ts(rnorm(20),start = c(1990,1), frequency = 4)
tslist$ts6 <- ts(rnorm(20),start = c(1990,1), frequency = 4)

# unlocalized meta information
m <- new.env()
meta_ts1 <- list(seed = 123,legacy_key = 'series1')
meta_ts2 <- list(seed = 543,legacy_key = 'series2')
meta_ts3 <- list(seed = 123,legacy_key = 'series1')
meta_ts4 <- list(seed = 543,legacy_key = 'series2')
meta_ts5 <- list(seed = 123,legacy_key = 'series1')
meta_ts6 <- list(seed = 543,legacy_key = 'series2')

meta_unlocalized <- addMetaInformation("ts1",meta_ts1)
addMetaInformation("ts2",meta_ts2,meta_unlocalized)
addMetaInformation("ts3",meta_ts3,meta_unlocalized)
addMetaInformation("ts4",meta_ts4,meta_unlocalized)
addMetaInformation("ts5",meta_ts5,meta_unlocalized)
addMetaInformation("ts6",meta_ts6,meta_unlocalized)


# Test that ..... ##################

test_that("Time series is the same after db roundtrip",{
  skip_on_cran()
  
  # Store Series
  storeTimeSeries(c("ts1","ts2","ts3","ts4","ts5","ts6"),con,tslist)
  
  # Read Series
  result <- readTimeSeries("ts1",con)
  
  expect_equal(tslist$ts1, result$ts1)
})

test_that("We have two localized meta data objects. I.e. one does not overwrite the other", {
  skip_on_cran()

  # create some localized meta information
  # EN
  m_e <- new.env()
  m_en <- list("wording" = "let's have a word.",
               "check" = "it's english man.!! SELECTION DELETE123")
  addMetaInformation("ts1",m_en,m_e)
  
  # DE
  m_d <- new.env()
  m_de <- list("wording" = "Wir müssen uns mal unterhalten......",
               "check" = "Das ist deutsch. wirklich")
  addMetaInformation("ts1",m_de,m_d)
  
  updateMetaInformation(m_e,
                      con,
                      locale = "en",
                      tbl = "meta_data_localized")

  updateMetaInformation(m_d,
              con,
              locale = "de",
              tbl = "meta_data_localized")

  mil_record_count <- dbGetQuery(con,"SELECT COUNT(*) FROM timeseries.meta_data_localized WHERE ts_key = 'ts1'")$count
    
  expect_equal(mil_record_count,2)
})

test_that("After succesful store, delete is also successful,i.e., same amount of series", {
  skip_on_cran()
  
  tsl <- list()
  m <- new.env()
  for(i in seq_along(1:21000)){
    tsl[[i]] <- ts(rnorm(20),start=c(1991,1),frequency = 12)
  }
  names(tsl) <- paste0("series",1:21000)
  
  count_before <- dbGetQuery(con, "SELECT COUNT(*) FROM timeseries.timeseries_main")$count
  storeTimeSeries(names(tsl),con,tsl)
  deleteTimeSeries(names(tsl),con)
  count_after <- dbGetQuery(con, "SELECT COUNT(*) FROM timeseries.timeseries_main")$count
  expect_equal(count_before,count_after)
})

test_that("Unlocalized meta data can be written to db in chunks.", {
  skip_on_cran()
  
  updateMetaInformation(meta_unlocalized,con,chunksize = 2)

  mdul_count <- dbGetQuery(con,"SELECT COUNT(*) FROM timeseries.meta_data_unlocalized WHERE ts_key ~ 'ts[1-6]'")$count
  expect_equal(mdul_count, 6)
})

if(!on_cran) {
  dbDisconnect(con)
}