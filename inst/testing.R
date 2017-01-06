library(timeseriesdb)
sandbox <- createConObj(dbname = "sandbox",passwd = "",dbhost = "localhost")
# runCreateTables(sandbox,schema = "ts_sandbox")
keys <- dbGetQuery(sandbox,
                    "SELECT ts_key FROM ext_timeseries.timeseries_main WHERE ts_key ~ 'seco'")$ts_key


tslist <- list()
tslist$ts1 <- ts(rnorm(30),start = c(2000,1),freq=12)
tslist$ts2 <- ts(rnorm(30,1000),start = c(2000,1),freq=12)

out <- storeTimeSeries(names(tslist),
                       sandbox,
                       valid_from = NULL,
                       valid_to = NULL,
                       li = tslist,
                       overwrite = T,
                       schema="ts_sandbox")

debug(runDbQuery)
readTimeSeries(c("ts2","ts1"),sandbox,schema="ts_sandbox")


dbGetQuery(sandbox,"ROLLBACK")
# change series 
tslist$ts2 <- ts(1:30,start = c(2000,1),freq=12)


#undebug(storeTimeSeries)
#debug(runDbQuery)
out <- storeTimeSeries(names(tslist)[2],
                       sandbox,
                       valid_from = "2017-05-01",
                       valid_to = "2017-11-01",
                       vintage_date = NULL,
                       li = tslist,
                       overwrite = T,
                       schema="ts_sandbox")


tslist$ts2 <- ts(50:80,start = c(2000,1),freq=12)
out <- storeTimeSeries(names(tslist)[2],
                       sandbox,
                       valid_from = NULL,
                       valid_to = NULL,
                       li = tslist,
                       overwrite = T,
                       schema="ts_sandbox")

readTimeSeries("ts2",sandbox,"2017-01-01",schema = "ts_sandbox")


#dbGetQuery(sandbox,"ROLLBACK")


