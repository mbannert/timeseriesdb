library(timeseriesdb)

con <- kofbts::kofDbConnect()

ts_keys <- runDbQuery(con, "SELECT ts_key from sandbox.meta_data_localized where locale_info = 'de' limit 100")$ts_key

meta.list <- readMetaInformation(series = ts_keys, con = con, locale = "de", schema = "sandbox")
meta.dt <- readMetaInformation(series = ts_keys, con = con, locale = "de", schema = "sandbox", as_list = FALSE)

mysteryNo <- floor(1000*runif(1))
message(sprintf("And today's lucky number is: %d", mysteryNo))

meta.dt[, randomNumber := mysteryNo]

# Note:
# Both storeMetaChunkWise and updateMetaInformation are deprecated. Do we keep them around?
storeMetaInformation(meta.dt, con, "sandbox", "meta_data_localized", "de")
meta.dt2 <- readMetaInformation(ts_keys, con, "de", schema = "sandbox", as_list = FALSE)
meta.dt2[1:10, randomNumber]

writeTsmetaToExcel(de = meta.dt2, fr = meta.dt, path = "funMetaData.xlsx")

meta.dtl <- readTsmetaFromExcel("funMetaData.xlsx")

meta.dtl$de[, mean(randomNumber)]
