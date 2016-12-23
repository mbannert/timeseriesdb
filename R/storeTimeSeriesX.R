#' Write an R Time Series to a PostgreSQL database 
#' 
#' This function writes time series object into a relational PostgreSQL database make use 
#' of PostgreSQL own 'key'=>'value' storage called hstore. The schema and database needs to 
#' created first. The parent R Package of this functions suggests a database structure
#' designed to store a larger amount of time series. This function uses INSERT INTO instead of the more convenient dbWritetable for performance reasons. DO NOT USE THIS FUNCTIONS IN LOOPS OR LAPPLY! This function can handle a set of time series on its own and is much faster than looping over a list. Non-unique primary keys are overwritten !
#' 
#' @author Matthias Bannert, Charles Clavadetscher, Gabriel Bucur
#' @param series character name of a time series, S3 class ts. When used with lists it is convenient to set series to names(li). Note that the series name needs to be unique in the database!
#' @param con a PostgreSQL connection object.
#' @param li list of time series. Defaults to NULL to no break legacy calls that use lookup environments.
#' @param tbl character string denoting the name of the main time series table in the PostgreSQL database.
#' @param md_unlocal character string denoting the name of the table that holds unlocalized meta information.
#' @param lookup_env environment to look in for timeseries. Defaults to .GobalEnv.
#' @param overwrite logical should existing records (same primary key) be overwritten? Defaults to TRUE.
#' @param schema SQL schema name. Defaults to timeseries. 
#' @importFrom DBI dbGetQuery
#' @export
storeTimeSeries <- function(series,
                            con,
                            li = NULL,
                            valid_from = "",
                            valid_to = "",
                            store_freq = T,
                            tbl = "timeseries_main",
                            md_unlocal = "meta_data_unlocalized",
                            lookup_env = .GlobalEnv,
                            overwrite = T,
                            schema = "timeseries"){
  
  # Create a PostgreSQL daterange compliant string
  validity <- sprintf("[%s,%s)",valid_from,valid_to)
  # make storeTimeSeries calls work
  # with former versions of timeseriesdb
  # that used environments. 
  if(is.null(li)){
    li <- as.list.environment(lookup_env)
  }

  # subset 
  li <- li[series]
  
  # avoid overwrite totally, 
  # i.e., existing records are not edited at all, 
  # not even vingtages are created
  if(!overwrite){
    db_keys <- runDbQuery(con,.queryGetExistingKeys(series,
                                         validity = validity,
                                         tbl = tbl,
                                         schema = schema))
    series <- series[!(series %in% db_keys$ts_key)]
    li <- li[series]
  }
  
  # stop here if none of the elements in the input list 
  # are allowed to be stored. In that case we do not need to run 
  # through the entire write process.
  if(length(li) == 0){
    cat("No time series in subset - returned empty list. Set overwrite=TRUE,\nif you want to overwrite existing series in the database.")
    return(list())
  } 
  
  # CREATE ELEMENTS AND RECORDS ##########################
  # use the form (..record1..),(..record2..),(..recordN..)
  # to be able to store everything in one big query
  
  keep <- sapply(li,function(x) inherits(x,c("ts","zoo","xts")))
  dontkeep <- !keep
  
  if(all(keep)){
    NULL #cat("No corrupted series found. \n")
  } else {
    cat("These elements are no valid time series objects: ",
        names(series[dontkeep]),"\n")  
  }
  
  li <- li[keep]
  
  hstores <- unlist(lapply(li,createHstore))
  freqs <- sapply(li,function(x) {
    ifelse(inherits(x,"zoo"),'NULL',frequency(x))
  })
  
  if(!store_freq){
    values <- paste(paste0("('",
                           paste(series,
                                 validity,
                                 hstores,
                                 sep="','"),
                           "')"),
                    collapse = ",")
  } else {
    values <- paste(paste0("('",
                           paste(series,
                                 validity,
                                 hstores,
                                 freqs,
                                 sep="','"),
                           "')"),
                    collapse = ",")
  }
  
  values <- gsub("''","'",values)
  values <- gsub("::hstore'","::hstore",values)
  values <- gsub("'NULL'","NULL",values)
  
  
  # add schema name
  tbl <- paste(schema,tbl,sep = ".")
  #md_unlocal <- paste(schema,md_unlocal,sep = ".")
  
  # CASE: Don't store vintages (version), just 
  # have one series per key
  if(valid_from == "" && valid_to == ""){
    data_query <- .queryStoreNoVintage(val = values,
                                       schema = schema,
                                       tbl = tbl)  
  }
  data_query
}

