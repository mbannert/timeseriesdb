library(kofdata)

# example with time series object
kofbarometer <- get_time_series("kofbarometer")$"kofbarometer"
kofbarometer <- window(kofbarometer, 2000, c(2018,12))

kofbsi <- get_time_series("ch.kof.bts_total.ng08.fx.q_ql_ass_bs.balance.d11")$"ch.kof.bts_total.ng08.fx.q_ql_ass_bs.balance.d11"
kofbsi <- window(kofbsi, 2010, c(2018,12))

kofei <- get_time_series("ch.kof.ie.retro.ch_total.ind.d11")$"ch.kof.ie.retro.ch_total.ind.d11"
kofei <- window(kofei, 2000, c(2018,4))

kofmpc <- get_time_series("ch.kof.mpc")$"ch.kof.mpc"
kofmpc <- kofmpc["2000/2018-12-13"]

kof_ts <- list(kofbarometer, kofbsi, kofei, kofmpc)
names(kof_ts) <- c("kofbarometer", "kofbsi", "kofei", "kofmpc")

usethis::use_data(kof_ts, overwrite = TRUE)
