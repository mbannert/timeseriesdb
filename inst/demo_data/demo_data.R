library(kofdata)

# example with time series objects until 2019
kofbarometer <- get_time_series("kofbarometer")$"kofbarometer"
kofbarometer <- window(kofbarometer, end = c(2019,12))

vintages <- kofdata::get_dataset("baro_vintages_monthly")
vintages_kofbarometer <- vintages[1:69]

ch.kof.ie.retro.ch_total.ind.d11 <- get_time_series("ch.kof.ie.retro.ch_total.ind.d11")$"ch.kof.ie.retro.ch_total.ind.d11"
ch.kof.ie.retro.ch_total.ind.d11 <- window(ch.kof.ie.retro.ch_total.ind.d11, end = c(2019,4))

kof_ts <- c(list(kofbarometer), vintages_kofbarometer, list(ch.kof.ie.retro.ch_total.ind.d11))
names(kof_ts) <- c("kofbarometer", names(vintages_kofbarometer), "ch.kof.ie.retro.ch_total.ind.d11")

# example with xts object and metadata

zrh_airport <- get_time_series(ts_keys=c("ch.zrh_airport.departure.total", "ch.zrh_airport.arrival.total"))

zrh_airport_md <- read_json("https://raw.githubusercontent.com/KOF-ch/economic-monitoring/master/data/ch.zrh_airport.departures.json")

usethis::use_data(kof_ts, zrh_airport, zrh_airport_md, overwrite = TRUE)
