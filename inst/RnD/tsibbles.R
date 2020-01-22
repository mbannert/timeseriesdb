library(tsibble)

some_df <- data.frame(
  ts_key = c(rep("ts1", 4), rep("ts2", 5)),
  date = c(seq(as.Date("2020-01-01"), length.out = 4, by = "1 month"),
           seq(as.Date("2020-01-01"), length.out = 5, by = "1 month")),
  value = 1:9,
  stringsAsFactors = FALSE
)

as_tsibble(some_df, key = ts_key, index = date)

attack_df <- data.frame(
  ts_key = c(rep("ts1", 4), rep("ts2", 5), rep("ts1", 4)),
  date = c(seq(as.Date("2020-01-01"), length.out = 4, by = "2 month"),
           seq(as.Date("2020-01-01"), length.out = 5, by = "1 month"),
           seq(as.Date("2020-02-02"), length.out = 4, by = "2 month")),
  value = 1:13,
  stringsAsFactors = FALSE
)

as_tsibble(attack_df, key = ts_key, index = date)

# BäM
# Then again, how could you tell...

faulty_attack_df <- data.frame(
  ts_key = c(rep("ts1", 4), rep("ts2", 5), rep("ts1", 4)),
  date = c(seq(as.Date("2020-01-01"), length.out = 4, by = "1 month"),
           seq(as.Date("2020-01-01"), length.out = 5, by = "1 month"),
           seq(as.Date("2020-02-01"), length.out = 4, by = "1 month")),
  value = 1:13,
  stringsAsFactors = FALSE
)

as_tsibble(faulty_attack_df, key = ts_key, index = date)

# boing
