con <- createConObj(dbhost = "localhost",
                    dbuser = "mbannert",
                    dbname = "sandbox",
                    passwd = "")

set.seed(123)
tslist <- list()
tslist$ts1 <- ts(rnorm(20),start = c(1990,1), frequency = 4)

# Store Series
storeTimeSeries("ts1",con,tslist)

# Read Series
result <- readTimeSeries("ts1",con)

# create some localized meta information
# EN
m_e <- new.env()
m_en <- list("wording" = "let's have a word.",
             "check" = "it's english man.!! SELECTION DELETE123")
addMetaInformation("ts1",m_en,m_e)

updateMetaInformation(m_e,
                      con,
                      locale = "en",
                      tbl = "meta_data_localized")

# DE
m_d <- new.env()
m_de <- list("wording" = "Wir müssen uns mal unterhalten......",
             "check" = "Das ist deutsch. wirklich")
addMetaInformation("ts1",m_de,m_d)

updateMetaInformation(m_d,
                      con,
                      locale = "de",
                      tbl = "meta_data_localized")

mil_record_count <- dbGetQuery(con,"SELECT COUNT(*) FROM timeseries.meta_data_localized WHERE ts_key = 'ts1'")$count


tsl <- list()
for(i in seq_along(1:30000)){
  tsl[[i]] <- ts(rnorm(20),start=c(1991,1),frequency = 12)
}
names(tsl) <- paste0("series",1:21000)

count_before <- dbGetQuery(con, "SELECT COUNT(*) FROM timeseries.timeseries_main")$count
storeTimeSeries(names(tsl),con,tsl)
deleteTimeSeries(names(tsl),con)
count_after <- dbGetQuery(con, "SELECT COUNT(*) FROM timeseries.timeseries_main")$count



dbDisconnect(con)





# Test that ..... ##################

test_that("Time series is the same after db roundtrip",{
  expect_equal(tslist$ts1, result$ts1)
})

test_that("We have to localized meta data objects. I.e. one does not overwrite the other",
          {
  expect_equal(mil_record_count,2)
})

test_that("After succesful store, delete is also successful,i.e., same amount of series",
          {
            expect_equal(count_before,count_after)
          })



