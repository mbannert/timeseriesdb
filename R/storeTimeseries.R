#' Write an R Time Series to a PostgreSQL database 
#' 
#' This function writes time series object into a relational PostgreSQL database make use 
#' of PostgreSQL own 'key'=>'value' storage called hstore. The schema and database needs to 
#' created first. The parent R Package of this functions suggests a database structure
#' designed to store a larger amount of time series. This function uses INSERT INTO instead of the more convenient dbWritetable for performance reasons
#' 
#' @author Matthias Bannert
#' @param series an object of class ts or zoo
#' @param connect a connection object created by dbConnect, defaults to an con object named con
#' @param tkey optional character string to specify an explicit time series primary key for the database. Defaults to NULL and uses the name of the R time series object as a key. Note that keys need to be unique in the database. 
#' @param tbl character string denoting the name of the main time series table in the PostgreSQL database.
#' @param overwrite logical, whether time series should be overwritten in case a non-unique primary key is provided. Defaults to TRUE.
#' @examples
#' ts1 <- ts(rnorm(100),start = c(1990,1),frequency = 4)
#' storeTimeseries(ts1)
storeTimeseries <- function(series,connect = con,
                   tbl = "timeseries_main",
                   lookup_env = .GlobalEnv,
                   overwrite = T){
#   # add key
#   if(is.null(tkey)){
#     ts_key <- deparse(substitute(series))
#   } else {
#     stopifnot(is.character(tkey))
#     ts_key <- tkey
#   }
  
  # collect information for insert query
  ts_data <- createHstore(get(series,envir = lookup_env))
  ts_freq <- frequency(get(series,envir = lookup_env))
  md_generated_on <- Sys.time()
  md_generated_by <- Sys.getenv("USER")
  
  # Overwrite existing time series using an inserting statement
  if(overwrite){
    dbGetQuery(con,sprintf("DELETE FROM %s WHERE ts_key = '%s'",tbl,series))
  }
  
  sql_query <- sprintf("INSERT INTO %s (ts_key,ts_data,ts_frequency,md_generated_on,md_generated_by) VALUES ('%s','%s',%s,'%s','%s')",
                       tbl,series,ts_data,ts_freq,md_generated_on,md_generated_by)
  # Print proper success notification to console
  if(is.null(dbGetQuery(con,sql_query))){
    print("Data inserted.")
  } 
  
}
