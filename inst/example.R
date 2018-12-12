library(timeseriesdb)
library(data.table)
con <- kofbts::kofDbConnect()

ts_keys <- runDbQuery(con, "SELECT ts_key from sandbox.meta_data_localized where locale_info = 'de' limit 100")$ts_key

meta.list <- readMetaInformation(series = ts_keys, con = con, locale = "de",
                                 schema = "sandbox")
meta.dt <- readMetaInformation(series = ts_keys, con = con, locale = "de", schema = "sandbox", as_list = FALSE)

mysteryNo <- floor(1000*runif(1))
message(sprintf("And today's lucky number is: %d", mysteryNo))

meta.dt[, randomNumber := mysteryNo]

# Note:
# Both storeMetaChunkWise and updateMetaInformation are deprecated. Do we keep them around?
storeMetaInformation(con, meta.dt, "sandbox", "meta_data_localized", "de")
meta.dt2 <- readMetaInformation(con,ts_keys,
                                "de", schema = "sandbox", as_list = FALSE)
meta.dt2[1:10, randomNumber]

writeTsmetaToExcel(de = meta.dt2, fr = meta.dt, path = "funMetaData.xlsx")

meta.dtl <- readTsmetaFromExcel("funMetaData.xlsx")

meta.dtl$de[, mean(randomNumber)]

writeTsmetaToExcel(meta.dt, path = "test.xlsx")


ll_de <- as.tsmeta.list(meta.dtl$de)

ll_de$ch.kof.inu.ng08.f4.sector_kof.cg.export.4.q_ql_ass_empl.balance


readMetaInformation(con,ts_keys[1],locale = NULL,tbl = "meta_data_unlocalized", schema = "sandbox")

readTimeSeries("ch.kof.inu.ng08.fx.q_ql_ass_bs.balance", con)
readTimeSeries(con, "ch.kof.inu.ng08.fx.q_ql_ass_bs.balance")
