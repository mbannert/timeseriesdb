#' Write an R Time Series to a PostgreSQL database 
#' 
#' This function writes time series object into a relational PostgreSQL database make use 
#' of PostgreSQL own 'key'=>'value' storage called hstore. The schema and database needs to 
#' created first. The parent R Package of this functions suggests a database structure
#' designed to store a larger amount of time series. This function uses INSERT INTO instead of the more convenient dbWritetable for performance reasons
#' 
#' @author Matthias Bannert, Gabriel Bucur
#' @param series character name of a time series, S3 class ts
#' @param con a PostgreSQL connection object.
#' @param ts_key optional character string to specify an explicit time series primary key for the database. Defaults to NULL and uses the name of the R time series object as a key. Note that keys need to be unique in the database. 
#' @param tbl character string denoting the name of the main time series table in the PostgreSQL database.
#' @param lookup_env environment to look in for timeseries. Defaults to .GobalEnv.
#' This option is particularly important when running storeTimeseries within loop like operations.
#' @param overwrite logical, whether time series should be overwritten in case a non-unique primary key is provided. Defaults to TRUE.
#' @export
storeTimeseries <- function(series,ts_key = NULL,
                            con,
                            tbl = "timeseries_main",
                            lookup_env = .GlobalEnv,
                            overwrite = T){
  # Because we cannot really use a global binding to 
  # the postgreSQL connection object which does not exist at the time
  # of compilation, we use the character name of the object here. 
  # connect <- get(connect,envir = parent.frame())
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
  
  # an additional key provides to opportunity to read time series key from 
  # from an attribute
  if(!is.null(ts_key)) series <- ts_key
  
  
  # Overwrite existing time series using an inserting statement
  if(overwrite){

    sql_query <- sprintf("INSERT INTO %s (ts_key,ts_data,ts_frequency,md_generated_on,md_generated_by) VALUES ('%s','%s',%s,'%s','%s')",
                       tbl,series,ts_data,ts_freq,md_generated_on,md_generated_by)
  }
  
  # Print proper success notification to console
  if(is.null(dbGetQuery(con,sql_query))){
    print("Data inserted.")
  } 
  
}