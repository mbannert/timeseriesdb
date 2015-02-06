
library(timeseriesdb)
x <- dbSendQuery(con,"SELECT ts_key,ts_data FROM timeseries_main LIMIT 10")
test <- dbFetch(x)
test$ts_data

y <- dbSendQuery(con,"SELECT ts_key,ts_frequency FROM timeseries_main LIMIT 20")
dbListResults(con)


rm(series)
s1 <- dbGetQuery(con,"SELECT ts_key FROM timeseries_main WHERE ts_key ='ch.kof.dhu_run2.rgn_sector_3d_c_size_kof.bfsr5_472_l.q_ql_exp_empl_n3m.ans_count'")$ts_key
s3 <- dbGetQuery(con,"SELECT ts_key FROM timeseries_main WHERE ts_key IN('ch.kof.dhu_run2.rgn_sector_3d_c_size_kof.bfsr5_472_l.q_ql_exp_empl_n3m.ans_count','ch.kof.dhu_run2.rgn_sector_3d_c_size_kof.bfsr5_472_l.q_ql_exp_empl_n3m.balance')")$ts_key


s <- dbGetQuery(con,"SELECT ts_key FROM timeseries_main LIMIT 5000")$ts_key
Rprof()
system.time({test2 <- readTimeSeries(s,con)})
Rprof(NULL)
summaryRprof()
undebug(strptime)