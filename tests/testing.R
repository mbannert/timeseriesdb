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


dbGetQuery(sandbox,"ROLLBACK")






undebug(readTimeSeries)
tslist <- readTimeSeries(keys[1:10],sandbox,schema="ext_timeseries")
tslist$ch.seco.unemployment.anzahl_kurzarbeiter <- stripLeadingNAsFromTs(tslist$ch.seco.unemployment.anzahl_kurzarbeiter)
tslist$tstest <- ts(1:30,start=c(2000,1),freq=12)


undebug(storeTimeSeries)
out <- storeTimeSeries(names(tslist)[11],
                       sandbox,
                       valid_from = "",
                       valid_to = "",
                       li = tslist,
                       overwrite = T,
                       schema="ts_sandbox")

tslist$ch.seco.unemployment.anzahl_kurzarbeiter <- ts(1:10,
                                                      start = c(2010,1),freq=12)


undebug(storeTimeSeries)
out <- storeTimeSeries(keys[1:10],sandbox,li = tslist,overwrite = F,schema = "ext_timeseries")
debug(runDbQuery)


jj <- .queryGetExistingKeys(keys[1:10],"[,)","timeseries_main","ext_timeseries")
oo <- dbSendQuery(sandbox,jj)
fetch(oo)


out_obj <- runDbQuery(bogus_connection,"SELECT * FROM some_table") 
attributes(out_obj)


d <- data.frame()
attr(d,"query_status") <- "OK"
attr(d,"query_status")
?dbSendQuery

xx <- runDbQuery(sandbox,"SELECT ts_key FROM ext_timeseries.timeseries_main LIMIT 10")
attributes(xx)
tryCatch({KOFSeasonalAdjust(e)},error = function(e) NULL)


cat(geterrmessage())

stderr()

return_df <- fetch(rs)
dbClearResult(xx)

?dbSendQuery
dbClearResult(dbSendQuery(sandbox,"SELECT ts_key FROM ext_timeseries.timeseries_main LIMIT 1"))

dbDisconnect(sandbox)
