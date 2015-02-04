#' Read Time Series From PostgreSQL database
#' 
#' This function reads a time series from a postgres database,
#' that key value pair storage (hstore), to archive time series.
#' After reading the information from the database a standard
#' R time series object of class 'ts' is built and returned. 
#' 
#' @author Matthias Bannert, Gabriel Bucur
#' @param series character representation of the key of the time series
#' @param con a PostgreSQL connection object
#' @param tbl character string denoting the name of the main time series table
#' in the PostgreSQL database.
#' @export
readTimeSeries <- function(series,con = options()$TIMESERIESDB_CON,
                           tbl = "timeseries_main"){
  
  if(is.null(con)) stop('Default TIMESERIESDB_CON not set in options() or no proper connection given to the con argument.')
  
  
  # Because we cannot really use a global binding to 
  # the postgreSQL connection object which does not exist at the time
  # of compilation, we use the character name of the object here. 
  
  sql_statement_data <- sprintf("SELECT ts_key,((each(ts_data)).key)::date,
                                ((each(ts_data)).value)::varchar
                                FROM %s
                                WHERE ts_key 
                                IN (%s)",tbl,
                                paste(paste0("'",series,"'"),collapse=","))
  
#   # extract data from hstore 
#   sql_statement_data <- sprintf("SELECT ((each(ts_data)).key)::date,
#                                 (each(ts_data)).value FROM %s WHERE ts_key = '%s'",tbl,series)
  # get freq
  sql_statement_freq <- sprintf("SELECT ts_key,ts_frequency FROM %s WHERE ts_key 
                                IN (%s)",tbl,
                                paste(paste0("'",series,"'"),collapse=","))
  dt_freq <- data.table(DBI::dbGetQuery(con,sql_statement_freq))
   setkeyv(dt_freq,"ts_key")
 dt <- data.table(DBI::dbGetQuery(con,sql_statement_data))
   setkeyv(dt,c("ts_key","key"))
#   
 dt[,key := as.Date(key)]
# 
  #   out_list <- split(out,factor(out$ts_key))

  out_list <- apply(dt_freq,1,function(s){
  
  #out_list <- lapply(series,function(s){
    

     sdt <- dt[ts_key == s[1]]
     start_date <- min(sdt$key)
     year <- as.numeric(format(start_date, "%Y"))
     freq <- as.numeric(s[2])
    if (freq == 4){
      period <- (as.numeric(format(start_date, "%m")) -1) / 3 + 1
    } else if(freq == 12) {
      period <- as.numeric(format(start_date, "%m"))
    } else {
      stop("Not a standard frequency.")
    }
    ts(as.numeric(sdt$value),
       start =c(year,period),
       frequency = as.numeric(freq))
  })  

  names(out_list) <- series
  out_list
}

library(timeseriesdb)
x <- dbSendQuery(con,"SELECT ts_key,ts_data FROM timeseries_main LIMIT 10")
test <- dbFetch(x)
test$ts_data

y <- dbSendQuery(con,"SELECT ts_key,ts_frequency FROM timeseries_main LIMIT 20")
dbListResults(con)


rm(series)
s <- dbGetQuery(con,"SELECT ts_key FROM timeseries_main LIMIT 5000")$ts_key
Rprof()
system.time({test2 <- readTimeSeries(s,con)})
Rprof(NULL)
summaryRprof()
undebug(strptime)