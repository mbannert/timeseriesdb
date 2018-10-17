######################################################
# Metadata
######################################################

library(microbenchmark)

meta_file <- "C:/dev/R_repos/swissdata/wd/ch.fso.besta.mjr/ch.fso.besta.mjr.yaml"

microbenchmark({
  meta_list <- meta_dt <- tstools::read_swissdata_meta(meta_file, as_list = TRUE)
  lapply(meta_list, jsonlite::toJSON, auto_unbox = TRUE)
  }, times = 50)

meta_to_json <- function(m) {
  dims <- setdiff(names(m), "ts_key")
  
  meta_fmt <- paste0("{", paste(sprintf('"%s": "%%s"', dims), collapse = ", "), "}")
  m[, .(ts_key, meta_data = do.call(sprintf, c(meta_fmt, .SD)))]
}

# DEFO the faster one!
microbenchmark({
  meta_dt <- tstools::read_swissdata_meta(meta_file)
  meta_to_json(meta_dt)
}, times  = 50)

# Depends on which version is loaded! (Actually, it doesn't, long as the schema was created with the proper type (except for reading maybe (and hstores (more parentheses!))))
# meta_list <- meta_dt <- tstools::read_swissdata_meta(meta_file, as_list = TRUE)
# microbenchmark(updateMetaInformation(meta_list, con, "timeseries_jsonb", "meta_data_localized", "de"))
# # Unit: milliseconds
# # expr
# # updateMetaInformation(meta_list, con, "timeseries_jsonb", "meta_data_localized",      "de")
# # min     lq     mean   median       uq     max neval
# # 77.7214 80.864 86.76881 84.13428 90.56706 113.972   100
# microbenchmark(updateMetaInformation(meta_list, con, "timeseries_json", "meta_data_localized", "de"))
# # Unit: milliseconds
# # expr
# # updateMetaInformation(meta_list, con, "timeseries_json", "meta_data_localized",      "de")
# # min       lq     mean   median       uq      max neval
# # 74.60877 78.05947 84.45198 81.76203 87.93455 141.8173   100
# microbenchmark(updateMetaInformation.meta_env(meta_list_2, con, "timeseries", "meta_data_localized", "de"))
# # Unit: milliseconds
# # expr
# # updateMetaInformation.meta_env(meta_list_2, con, "timeseries",      "meta_data_localized", "de")
# # min       lq     mean   median       uq      max neval
# # 88.6189 99.04183 106.4409 105.0132 111.6379 145.8754   100


##############
# Do it again more thoroughly
##############
meta_list_edc <- tstools::read_swissdata_meta("C:/dev/R_repos/swissdata/wd/ch.fso.cah.edc/ch.fso.cah.edc.yaml", as_list = TRUE)
steps <- 10^(0:4)
times <- c(1000, 100, 100, 20, 10, 5)
res_hstore <- list()
for(i in seq_along(steps)) {
  message(steps[i])
  res_hstore[[i]] <- microbenchmark(
    updateMetaInformation.meta_env(meta_list_edc[1:steps[i]], con, "timeseries", "meta_data_localized", "de"),
    times = times[i]
  )
  print(res_hstore[[i]])
}
# Extract mean times
t_hstore <- lapply(res_hstore, function(x){x$time[2]})
# 1
# Unit: milliseconds
# expr
# updateMetaInformation.meta_env(meta_list_edc[1:steps[i]], con,      "timeseries", "meta_data_localized", "de")
# min       lq     mean   median       uq      max neval
# 21.39513 22.46804 23.50356 23.06926 23.78502 68.38837  1000
# 2
# Unit: milliseconds
# expr
# updateMetaInformation.meta_env(meta_list_edc[1:steps[i]], con,      "timeseries", "meta_data_localized", "de")
# min       lq     mean   median       uq      max neval
# 29.90539 30.98015 32.08456 31.58281 32.31868 44.52679   100
# 3
# Unit: milliseconds
# expr
# updateMetaInformation.meta_env(meta_list_edc[1:steps[i]], con,      "timeseries", "meta_data_localized", "de")
# min       lq     mean   median       uq      max neval
# 109.3267 122.0952 135.7566 131.3151 143.6453 208.0959   100
# 4
# Unit: milliseconds
# expr
# updateMetaInformation.meta_env(meta_list_edc[1:steps[i]], con,      "timeseries", "meta_data_localized", "de")
# min       lq     mean   median       uq     max neval
# 996.0478 1008.894 1050.533 1038.034 1081.305 1177.66    20
# 5
# Unit: seconds
# expr
# updateMetaInformation.meta_env(meta_list_edc[1:steps[i]], con,      "timeseries", "meta_data_localized", "de")
# min       lq     mean   median       uq      max neval
# 10.00352 10.02741 10.34087 10.13728 10.72124 11.01665    10
# 6
# Unit: seconds
# expr
# updateMetaInformation.meta_env(meta_list_edc[1:steps[i]], con,      "timeseries", "meta_data_localized", "de")
# min       lq     mean   median       uq     max neval
# 102.7048 103.0394 105.5484 103.7693 107.2352 110.993     5

res_json <- list()
for(i in seq_along(steps)) {
  message(steps[i])
  res_json[[i]] <- microbenchmark(
    updateMetaInformation(meta_dt_edc[1:steps[i], ], con, "timeseries_json", "meta_data_localized", "de"),
    times = times[i]
  )
  print(res_json[[i]])
}
# Extract mean times
t_json <- lapply(res_json, function(x){x$time[2]})
# 1
# Unit: milliseconds
# expr
# updateMetaInformation.meta_env(meta_list_edc[1:steps[i]], con,      "timeseries_json", "meta_data_localized", "de")
# min       lq     mean   median       uq      max neval
# 23.28356 26.90873 28.91305 28.26389 29.84566 66.65676  1000
# 10
# Unit: milliseconds
# expr
# updateMetaInformation.meta_env(meta_list_edc[1:steps[i]], con,      "timeseries_json", "meta_data_localized", "de")
# min       lq    mean   median       uq      max neval
# 33.51969 35.37938 37.6967 36.91846 38.35366 52.05958   100
# 100
# Unit: milliseconds
# expr
# updateMetaInformation.meta_env(meta_list_edc[1:steps[i]], con,      "timeseries_json", "meta_data_localized", "de")
# min       lq     mean   median       uq      max neval
# 125.1103 129.0726 133.9857 131.8933 135.9253 182.2428   100
# 1000
# Unit: seconds
# expr
# updateMetaInformation.meta_env(meta_list_edc[1:steps[i]], con,      "timeseries_json", "meta_data_localized", "de")
# min       lq     mean   median       uq      max neval
# 1.067912 1.079897 1.110549 1.098297 1.133582 1.232129    20
# 10000
# Unit: seconds
# expr
# updateMetaInformation.meta_env(meta_list_edc[1:steps[i]], con,      "timeseries_json", "meta_data_localized", "de")
# min       lq     mean   median     uq      max neval
# 10.5545 10.78323 11.09917 10.97974 11.571 11.69654    10
# 1e+05
# Unit: seconds
# expr
# updateMetaInformation.meta_env(meta_list_edc[1:steps[i]], con,      "timeseries_json", "meta_data_localized", "de")
# min       lq   mean   median       uq      max neval
# 117.3116 120.0109 120.26 120.1423 120.3454 123.4897     5


res_jsonb <- list()
for(i in seq_along(steps)) {
  message(steps[i])
  res_jsonb[[i]] <- microbenchmark(
    updateMetaInformation(meta_dt_edc[1:steps[i], ], con, "timeseries_jsonb", "meta_data_localized", "de"),
    times = times[i]
  )
  print(res_jsonb[[i]])
}
# Extract mean times
t_jsonb <- lapply(res_jsonb, function(x){x$time[2]})
# 
# 1
# Unit: milliseconds
# expr
# updateMetaInformation.meta_env(meta_list_edc[1:steps[i]], con,      "timeseries_jsonb", "meta_data_localized", "de")
# min       lq     mean   median       uq      max neval
# 23.50771 25.77608 27.58735 26.84818 28.41661 54.53343  1000
# 10
# Unit: milliseconds
# expr
# updateMetaInformation.meta_env(meta_list_edc[1:steps[i]], con,      "timeseries_jsonb", "meta_data_localized", "de")
# min       lq     mean  median       uq      max neval
# 34.16586 35.97752 37.99326 36.7682 38.60429 57.02329   100
# 100
# Unit: milliseconds
# expr
# updateMetaInformation.meta_env(meta_list_edc[1:steps[i]], con,      "timeseries_jsonb", "meta_data_localized", "de")
# min       lq     mean   median       uq      max neval
# 129.3544 137.2938 143.2104 141.0735 145.9759 185.8395   100
# 1000
# Unit: seconds
# expr
# updateMetaInformation.meta_env(meta_list_edc[1:steps[i]], con,      "timeseries_jsonb", "meta_data_localized", "de")
# min       lq     mean   median       uq     max neval
# 1.093213 1.129791 1.142017 1.142872 1.156993 1.18674    20
# 10000
# Unit: seconds
# expr
# updateMetaInformation.meta_env(meta_list_edc[1:steps[i]], con,      "timeseries_jsonb", "meta_data_localized", "de")
# min       lq     mean   median       uq      max neval
# 10.89351 10.97753 11.85715 11.12567 13.23692 14.19857    10
# 1e+05
# Unit: seconds
# expr
# updateMetaInformation.meta_env(meta_list_edc[1:steps[i]], con,      "timeseries_jsonb", "meta_data_localized", "de")
# min       lq     mean   median       uq    max neval
# 114.7527 116.4162 122.7696 117.8135 128.8754 135.99     5



plotdata <- data.frame(
  t = c(unlist(t_hstore), unlist(t_json), unlist(t_jsonb))/1000000,
  grp = c(rep("hstore", length(steps)), rep("json", length(steps)), rep("jsonb", length(steps))),
  n = steps)
ggplot(plotdata, aes(x = n, y = t, group = grp, color = grp)) + 
  geom_line() + 
  geom_point() +
  scale_x_continuous(trans = "log10") + 
  scale_y_continuous(trans = "log10")



res_transform_jsonlite <- list()
res_transform_createjson.list <- list()
res_transform_createjson.data.frame <- list()
for(i in seq_along(steps)[1:5]) {
  message(steps[i])
  res_transform_jsonlite[[i]] <- microbenchmark({
    sapply(meta_list_edc[1:steps[i]], jsonlite::toJSON, auto_unbox = TRUE)
  }, times = times[i])
  print(res_transform_jsonlite[[i]])
  
  res_transform_createjson.list[[i]] <- microbenchmark({
    sapply(meta_list_edc[1:steps[i]], createJSON)
  }, times = times[i])
  print(res_transform_createjson.list[[i]])
  
  res_transform_createjson.data.frame[[i]] <- microbenchmark({
    createJSON(meta_dt_edc[1:steps[i], ])
  }, times = times[i])
  print(res_transform_createjson.data.frame[[i]])
}

t_transform_jsonlite <- sapply(res_transform_jsonlite, function(x){x$time[2]})
t_transform_createjson.list <- sapply(res_transform_createjson.list, function(x){x$time[2]})
t_transform_createjson.data.frame <- sapply(res_transform_createjson.data.frame, function(x){x$time[2]})

plotdata <- data.frame(
  t = c(t_transform_jsonlite, t_transform_createjson.list, t_transform_createjson.data.frame)/1000000,
  grp = c(rep("jsonlite", length(steps)-1), rep("createjson.list", length(steps)-1), rep("createjson.data.frame", length(steps)-1)),
  n = steps[1:5])
ggplot(plotdata, aes(x = n, y = t, group = grp, color = grp)) + 
  geom_line() + 
  geom_point() +
  scale_x_continuous(trans = "log10") + 
  scale_y_continuous(trans = "log10")



######################################################
# Time series (just to check)
######################################################

tsl <- tstools::generate_random_ts(lengths = 100)

# This one wins
microbenchmark({
  dates <- timeseriesdb:::indexToDate(stats::time(tsl$ts1))
  values <- tsl$ts1
  paste0('{"dates":', jsonlite::toJSON(dates), '}, "values":', jsonlite::toJSON(values))
})

microbenchmark({
  dates <- timeseriesdb:::indexToDate(stats::time(tsl$ts1))
  values <- as.numeric(tsl$ts1)
  vl <- as.list(values)
  names(vl) <- dates
  jsonlite::toJSON(vl, auto_unbox = TRUE)
})
