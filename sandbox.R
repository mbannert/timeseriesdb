library(RPostgres)

con <- dbConnect(RPostgres::Postgres(),
                 dbname = "postgres",
                 user = "postgres",
                 host = "localhost",
                 password = "",
                 port = 1111)

dbListTables(con)


db_create_ds <- function(){
  
}

#' Store New Version of a Time Series to PostgreSQL
#' 
#' 
#'  
#' @param con PostgreSQL connection object created with Rpostgres.
#' @param series tslist or data.table containing time series. 
#' @param release_date character date with time
#' @param schema character schema name, defaults to 'timeseries'.
#' @param chunksize 
#' importFrom @RPostgres dbWriteTable
#' @export
db_store_ts <- function(con, series,
                     access,
                     valid_from = NULL, 
                     release_date = NULL,
                     schema = "timeseries",
                     chunksize = 10000){
  if(!is.null(set_id)){
    # RUN DB FUNCTION dataset_exists
    ds_exist_check <- dbGetQuery()
    if(!ds_exist_check){
      stop("Dataset does not exist.\n
           Please check your spelling or create a new dataset 
           using db_create_set first.")
    } 
  }
  
  ts_json <- to_ts_json(series)
  # store records to a temp table in order to use dbWriteTable
  # which is fast because of STDIN usage as opposed to simple inserts
  # or sending around huge strings. 
  db_tmp_store(con, ts_json, valid_from, release_date, access)
  dbExecute(con, "CALL SOME SQL FUNCTION")

}

library(jsonlite)
tsl <- tstools::generate_random_ts(10)
class(tsl) <- c("tslist",class(tsl))
xx <- to_ts_json(tsl)
xx$ts1


store_time_series(con, tsl, "main")




