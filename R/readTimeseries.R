#' Read Time Series From PostgreSQL database
#' 
#' This function reads a time series from a postgres database,
#' that key value pair storage (hstore), to archive time series.
#' After reading the information from the database a standard
#' R time series object of class 'ts' is built and returned. 
#' 
#' @author Matthias Bannert, Gabriel Bucur
#' @param series character vector of series names.
#' @param con a PostgreSQL connection object
#' @param tbl character string denoting the name of the main time series table
#' in the PostgreSQL database.
#' @export
readTimeSeries <- function(series,con = get(Sys.getenv("TIMESERIESDB_CON")),
                           tbl = "timeseries_main"){
  
  if(class(con) != "PostgreSQLConnection") stop('Default TIMESERIESDB_CON not set in Sys.getenv or no proper connection given to the con argument. con is not a PostgreSQLConnection obj.')
  
  
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
  
  # CAUTION NOT IMMUTABLE, object is modified in place ----------------------
  dt[,key := as.Date(key)]
  
  # replace "NA" with an NA_integer_
  # in order to avoid coercion warning,
  # !! this is a bit costly... 
  dt[value == 'NA',value := NA] 
  # CAUTION NOT IMMUTABLE, END IN PLACE EDITING !!!  ----------------------
  
  

  # chose the lapply solution here, to really
  # always give back a list. apply over rows might have been
  # a little faster, but behaves different for single and multiple series. 
  # i.e. apply returns matrix or list. 
  #   out_list <- apply(dt_freq,1,function(s){
    out_list <- lapply(series,function(s){
    
    
    sdt <- dt[ts_key == s]
    start_date <- min(sdt$key)
    year <- as.numeric(format(start_date, "%Y"))
    freq <- as.numeric(dt_freq[ts_key == s,ts_frequency])
    if (freq == 4){
      period <- (as.numeric(format(start_date, "%m")) -1) / 3 + 1
    } else if(freq == 12) {
      period <- as.numeric(format(start_date, "%m"))
    } else {
      stop("Not a standard frequency.")
    }
    # return a time series object
    ts(as.numeric(sdt$value),
       start =c(year,period),
       frequency = as.numeric(freq))
  })  
  
  names(out_list) <- series
  out_list
}
