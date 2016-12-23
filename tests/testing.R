library(timeseriesdb)
sandbox <- createConObj(dbname = "sandbox",passwd = "",dbhost = "localhost")
keys <- dbGetQuery(sandbox,
                    "SELECT ts_key FROM ext_timeseries.timeseries_main WHERE ts_key ~ 'seco'")$ts_key

undebug(readTimeSeries)
tslist <- readTimeSeries(keys[1:10],sandbox,schema="ext_timeseries")

undebug(storeTimeSeries)
storeTimeSeries(keys[1:10],sandbox,li = tslist,overwrite = F)

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
