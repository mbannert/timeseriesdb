store_records <- function(con,
                          records,
                          valid_from = NULL,
                          release_date = NULL,
                          tbl = "timeseries_main",
                          overwrite = TRUE, # Might keep that to indicate whether old vintages should be deleted when storing single record?
                          schema = "timeseries"){
  
  # some_sql
  # also: chunking? RPostgres seems to be pretty good about it tho with param queries
  # anyway, if it is to be done then HERE is the spot. right here vvv 
  message("Here to store the following records:")
  print(records)
  message("*cheap party horn sound*")
}