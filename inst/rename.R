require(data.table)
survey_changes <- fread("inst/function_names.csv")

files <- list.files(".", pattern = ".R", recursive = TRUE, full.names = TRUE)
files <- files[!grepl("function_names", files)]
files <- files[!grepl("RData", files)]
files <- files[!grepl("rda$", files)]
for(file in files) {
  message(file)
  l_new <- readLines(con = file)
  for(i in 1:nrow(survey_changes)) {
    l_new <- gsub(pattern = survey_changes[i,OLD], replace = survey_changes[i,NEW], x = l_new)
  }
  writeLines(l_new, con = file)
}


sql_changes <- fread("inst/sql_function_names.csv")
sql_changes <- sql_changes[OLD != NEW]
sql_files <- list.files(pattern = "\\.sql$", recursive = TRUE, full.names = TRUE)

for(f in sql_files) {
  message(f)
  lns <- readLines(con = f)

  for(i in 1:nrow(sql_changes)) {
    lns <- gsub(sql_changes[i, sprintf('timeseries\\.%s', OLD)], sql_changes[i, sprintf('timeseries\\.%s', NEW)], lns)
  }

  writeLines(lns, con = f)
}

r_files <- list.files(pattern = "\\.R$", recursive = TRUE, full.names = TRUE)

for(f in r_files) {
  message(f)
  lns <- readLines(con = f)

  for(i in 1:nrow(sql_changes)) {
    lns <- gsub(sql_changes[i, sprintf('"%s"', OLD)], sql_changes[i, sprintf('"%s"', NEW)], lns)
  }

  writeLines(lns, con = f)
}
