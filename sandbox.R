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


#' importFrom @RPostgres dbWriteTable
#' @export
ts_store <- function(con, series,
                     set_id = NULL,
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
  
  dbWriteTable()
  
  
}


