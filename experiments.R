# connect to localhost
library(devtools)
load_all()
prod <- kofbts::kofDbConnect("prod")
keys <- dbGetQuery(prod,"SELECT ts_key FROM timeseries_main
                   WHERE ts_key ~ 'ch.kof.ghu' LIMIT 10000")
tsl <- readTimeSeries(keys$ts_key,prod)

storeTimeSeries(names(tsl),lcl,tsl,schema="meta_overhaul")

mi <- new.env()
readMetaInformation(keys$ts_key[5],prod,"en",meta_env = mi)

mi$ch.kof.ghu.ng08.fx.sector_3d.463.q_ql_chg_del_time_pqpyq.balance.e2

mi_record <- mi$ch.kof.ghu.ng08.fx.sector_3d.463.q_ql_chg_del_time_pqpyq.balance.e2

class(mi_record)

mi_record$sa

library(microbenchmark)

rm(li_of_records)
microbenchmark({
  li_of_records <- list(record_1 = mi$ch.kof.ghu.ng08.fx.sector_3d.463.q_cb_restrict_none.share.d11,
                        record_2 = mi$ch.kof.ghu.ng08.fx.sector_3d.463.q_ql_chg_del_time_pqpyq.balance.e2,
                        record_3 = mi$ch.kof.ghu.ng08.fx.sector_4d.4649.q_ql_chg_profit_p3m.share_pos)
  
  li_of_records$ch.kof.ghu.ng08.fx.size_kof.l.q_ql_exp_chg_bs_n6m.share_neg.d12 <- 
    mi$ch.kof.ghu.ng08.fx.size_kof.l.q_ql_exp_chg_bs_n6m.share_neg.d12
  li_of_records$ch.kof.ghu.ng08.fx.size_kof.s.q_ql_exp_chg_bs_n6m.share_eq.e2 <-
    mi$ch.kof.ghu.ng08.fx.size_kof.s.q_ql_exp_chg_bs_n6m.share_eq.e2
},times = 1000)

rm(env_of_records)
microbenchmark({
  env_of_records <- new.env()
  assign("ch.kof.ghu.ng08.fx.sector_3d.463.q_cb_restrict_none.share.d11",
         mi$ch.kof.ghu.ng08.fx.sector_3d.463.q_cb_restrict_none.share.d11,
         envir = env_of_records )
  assign("ch.kof.ghu.ng08.fx.sector_3d.463.q_ql_chg_del_time_pqpyq.balance.e2",
         mi$ch.kof.ghu.ng08.fx.sector_3d.463.q_ql_chg_del_time_pqpyq.balance.e2,
         envir = env_of_records )
  assign("ch.kof.ghu.ng08.fx.sector_4d.4649.q_ql_chg_profit_p3m.share_pos",
         mi$ch.kof.ghu.ng08.fx.sector_4d.4649.q_ql_chg_profit_p3m.share_pos,
         envir = env_of_records )
  assign("ch.kof.ghu.ng08.fx.size_kof.l.q_ql_exp_chg_bs_n6m.share_neg.d12",
         mi$ch.kof.ghu.ng08.fx.size_kof.l.q_ql_exp_chg_bs_n6m.share_neg.d12,
         envir = env_of_records )
  assign("ch.kof.ghu.ng08.fx.size_kof.s.q_ql_exp_chg_bs_n6m.share_eq.e2",
         mi$ch.kof.ghu.ng08.fx.size_kof.s.q_ql_exp_chg_bs_n6m.share_eq.e2,
         envir = env_of_records )
  
},times = 1000)


library(jsonlite)

microbenchmark({toJSON(li_of_records)})
microbenchmark({toJSON(as.list(env_of_records))})
?list2env()



storeMetaChunkWise
class(env_of_records)
class(env_of_records) <- c("meta_env","environment")

undebug(updateMetaInformation.data.frame)
updateMetaInformation(env_of_records,con = lcl, schema = "meta_overhaul",
                      locale = "en",tbl = "meta_data_localized")


md <- dbGetQuery(lcl,"SELECT ts_key, meta_data 
           FROM meta_overhaul.meta_data_localized")
md$ts_key
jsonlite::fromJSON(md$meta_data,simplifyDataFrame = F)

jsl <- jsonlite::stream_in(textConnection(gsub("\\n", "", md$meta_data)))

as.list(jsl)
















lcl <- createConObj(dbname = "sandbox",
                    dbhost = "localhost",passwd = "")

runCreateTables(lcl,schema = "meta_overhaul")

