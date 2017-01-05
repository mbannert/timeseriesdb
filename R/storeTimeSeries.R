#' Write an R time series to a PostgreSQL database 
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
#' @param valid_from character date lower bound of a date range.
#' @param valid_to character date upper bound of a date range.
#' @param vintage_date character date, usually not used, defaults to NULL for non-vintages and vintages that are entered at the current end of the series. By specifying a date one can store former vintages. 
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
                            valid_from = NULL,
                            valid_to = NULL,
                            vintage_date = NULL,
                            store_freq = T,
                            tbl = "timeseries_main",
                            tbl_vintages = "timeseries_vintages",
                            md_unlocal = "meta_data_unlocalized",
                            lookup_env = .GlobalEnv,
                            overwrite = T,
                            schema = "timeseries"){
  # backwards compatibility
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
  # not even additional vintages are created
  if(!overwrite){
    db_keys <- runDbQuery(con,.queryGetExistingKeys(series,
                                                    tbl = tbl,
                                                    schema = schema))
    series <- series[!(series %in% db_keys$ts_key)]
    li <- li[series]
  }
  
  # stop here if none of the elements in the input list 
  # are allowed to be stored. In that case we do not need to run 
  # through the entire write process.
  if(length(li) == 0){
    cat("No time series in subset - returned empty list. Set overwrite=TRUE or add valid_from and/or valid_to arguments, if you want to overwrite existing series or store different versions of a series.")
    return(list())
  } 
  
  # SANITY CHECK ##############
  keep <- sapply(li,function(x) inherits(x,c("ts","zoo","xts")))
  dontkeep <- !keep
  
  if(all(keep)){
    NULL #cat("No corrupted series found. \n")
  } else {
    cat("These elements are no valid time series objects: \n",
        paste0(names(series[dontkeep])," \n"))  
  }
  
  li <- li[keep]
  
  # VALIDITY / VINTAGES ##################
  if(is.null(valid_from) && is.null(valid_to)){
    # Standard for single versioned time series ###############
    values <- .createValues(li,NULL,store_freq = store_freq)
    data_query <- .queryStoreNoVintage(val = values,
                                       schema = schema,
                                       tbl = tbl)
    md_values <- .createValuesMeta(li)
    meta_data_query <- .queryStoreMeta(md_values,schema)
    
    out <- c(data = attributes(runDbQuery(con,data_query)),
             meta_data = attributes(runDbQuery(con,meta_data_query)))
    
  } else {
    # Handle case that either valid from OR valid to is null.
    # Create a PostgreSQL daterange compliant string
    valid_from <- ifelse(is.null(valid_from),"",valid_from)
    valid_to <- ifelse(is.null(valid_to),"",valid_to)
    validity <- sprintf("[%s,%s)",valid_from,valid_to)
    values <- .createValues(li,validity,store_freq = store_freq)
    data_query <- .queryStoreVintage(val = values,
                                     schema = schema,
                                     tbl = tbl_vintages,
                                     vintage_date = vintage_date)
    out <- attributes(runDbQuery(con,data_query))
  }
  out
}


.createValuesMeta <- function(li){
  # CREATE META INFORMATION -------------------------------------------------
  # automatically generated meta information
  md_generated_by <- Sys.info()["user"]
  md_resource_last_update <- Sys.time()
  md_coverages <- unlist(lapply(li,function(x){
    sprintf('%s to %s',
            min(zooLikeDateConvert(x)),
            max(zooLikeDateConvert(x))
    )}
  ))
  
  series <- names(li)
  
  # same trick as for data itself, one query
  md_values <- paste(paste0("('",
                            paste(series,
                                  md_generated_by,
                                  md_resource_last_update,
                                  md_coverages,
                                  sep="','"),
                            "')"),
                     collapse = ",")
  md_values
}

.createValues <- function(li, validity = NULL, store_freq){
  # CREATE ELEMENTS AND RECORDS ##########################
  # use the form (..record1..),(..record2..),(..recordN..)
  # to be able to store everything in one big query
  hstores <- unlist(lapply(li,createHstore))
  series <- names(li)
  freqs <- sapply(li,function(x) {
    ifelse(inherits(x,"zoo"),'NULL',frequency(x))
  })
  
  if(is.null(validity)){
    if(!store_freq){
      values <- paste(paste0("('",
                             paste(series,
                                   hstores,
                                   sep="','"),
                             "')"),
                      collapse = ",")
    } else {
      values <- paste(paste0("('",
                             paste(series,
                                   hstores,
                                   freqs,
                                   sep="','"),
                             "')"),
                      collapse = ",")
    }
  } else {
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
  }
    values <- gsub("''","'",values)
    values <- gsub("::hstore'","::hstore",values)
    values <- gsub("'NULL'","NULL",values)
    values
}