library(timeseriesdb)

con <- createConObj(dbuser = "mbannert",
             dbhost = "localhost",
             dbname = "sandbox",
             passwd = ""
             )

tslist <- readTimeSeries(c("ch.kof.ghu.run2.ng08.f4.size_eu.l.q_ql_exp_chg_bs_n6m.balance",
                           "ch.kof.ghu.run2.ng08.f4.size_eu.l.q_ql_exp_chg_bs_n6m.ans_count"),
                         con)

undebug(storeTimeSeries)
library(microbenchmark)

microbenchmark({
  storeTimeSeries(names(tslist),con,tslist)
},times = 100)


undebug(bulkStoreTimeSeries)

bulkStoreTimeSeries(names(tslist),con,tslist)  


microbenchmark({
  bulkStoreTimeSeries(names(tslist),con,tslist)  
},times = 100)




createHstore(tslist$ch.kof.ghu.run2.ng08.f4.size_eu.l.q_ql_exp_chg_bs_n6m.balance)
l <- sapply(tslist,createHstore)
xx <- data.frame(ts_key = names(tslist),
           ts_data = l,
           ts_frequency = sapply(tslist,frequency),
           stringsAsFactors = F)


xx$ts_frequency



en <- list('short_description' = 'Random Series', 'full_description' = 'Random Normal using seed 123.')
de <- list('short_description' = 'deutsch alta', 'full_description' = 'eine etwas längere deutscher Beschreibung')


meta_en <- addMetaInformation('ts1',en) 
meta_de <- addMetaInformation('ts1',de) 

meta_en$ts1

updateMetaInformation(meta_de,con,tbl = "meta_data_localized",locale="de")
updateMetaInformation(meta_en,con,tbl = "meta_data_localized",locale="en")


tcon <- kofbts::kofDbConnect()

readTimeSeries()





keys <- kofbts::getSurveyKeys("ghu", tcon)
updateMetaInformation()
