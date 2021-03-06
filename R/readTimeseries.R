#' Read Time Series From PostgreSQL database
#' 
#' This function reads a time series from a PostgreSQL relation
#' that uses Postgres' key value pair storage (hstore).
#' After reading the information from the database a standard
#' R time series object of class 'ts' is built and returned. Irregular time series return zoo objects.
#' 
#' @author Matthias Bannert, Gabriel Bucur
#'
#' @param series character vector of time series keys
#' @param con a PostgreSQL connection object
#' @param valid_on character date string on which the series should be valid. Defaults to NULL. Only needed when different vintages of a time series are stored.  
#' @param env environment, optional argument to dump time series directly into an environment. Most often used with globalenv(), which gives all time series directly back to the global env.
#' @param tbl character string denoting the name of the relation that contains ts_key, ts_data, ts_frequency.
#' @param tbl_vintages character table name of the relation that holds time series vintages
#' @param schema character SQL schema name. Defaults to timeseries.
#' @param pkg_for_irreg character name of package for irregular series. xts or zoo, defaults to xts.
#' @param chunksize numeric value of threshold at which input vector should be processed in chunks. defaults to 10000.
#' @param respect_release_date logical should the relaase set in the database be respected. If TRUE, the last observation will be cut off if server time is before release date. Reasonable for relesae date.
#' @param regex If set to TRUE, series will be interpreted as a regular exporession, so that all time series whose keys match the pattern will be returned.
#'
#' @importFrom DBI dbGetQuery
#' @importFrom jsonlite fromJSON
#' @export
readTimeSeries <- function(con,
                           series, 
                           valid_on = NULL,
                           tbl = "timeseries_main",
                           tbl_vintages = "timeseries_vintages",
                           schema = "timeseries",
                           env = NULL,
                           pkg_for_irreg = "xts",
                           chunksize = 10000,
                           respect_release_date = FALSE,
                           regex = FALSE){
  
  if(is.character(con)) {
    warning("You are using this function in a deprecated fashion. Use readTimeSeries(con, series, ...) in the future.")
    t <- series
    series <- con
    con <- t
  }
  
  if(regex) {
    if(length(series) > 1) {
      stop("Only supports a single expression in series!")
    }
    
    pattern <- series
    
    match_query <- sprintf("SELECT ts_key FROM %s.timeseries_main WHERE ts_key ~ '%s'",
                         schema, pattern)
    series <- dbGetQuery(con, match_query)$ts_key
    
    if(length(series) == 0) {
      stop(sprintf("No series found matching '%s'!", pattern))
    }
  }

  useries <- unique(series)
  if(length(useries) != length(series)){
    warning("Input vector contains non-unique keys, stripped duplicates.")
  } 
  series <- useries
  
  # create a function in order to be able to read series in chunkwise
  # if necessary. Found out that 100K+ series cause significantly slower 
  # read times... 
  readFromDB <- function(series,con){
    series <- paste(paste0("('",series,"')"),collapse=",")
    if(is.null(valid_on)){
      read_SQL <-
        sprintf("
                BEGIN;
                CREATE TEMPORARY TABLE ts_read (ts_key text PRIMARY KEY) ON COMMIT DROP;
                INSERT INTO ts_read(ts_key) VALUES %s;
                
                SELECT ts_key, row_to_json(t)::text AS ts_json_records, extract(EPOCH FROM NOW()) as server_time
                FROM (
                SELECT tm.ts_key, ts_data, ts_frequency, extract(epoch from ts_release_date) as ts_release_date
                FROM %s.%s tm
                JOIN ts_read tr
                ON (tm.ts_key = tr.ts_key)
                ) t;",
              series, schema, tbl)  
    } else {
      read_SQL <-
        sprintf("
                BEGIN;
                CREATE TEMPORARY TABLE ts_read (ts_key text PRIMARY KEY) ON COMMIT DROP;
                INSERT INTO ts_read(ts_key) VALUES %s;
                
                SELECT ts_key, row_to_json(t)::text AS ts_json_records, extract(EPOCH FROM NOW()) as server_time
                FROM (
                SELECT tm.ts_key, ts_data, ts_frequency, extract(epoch from ts_release_date) as ts_release_date
                FROM %s.%s tm 
                JOIN ts_read tr
                ON (tm.ts_key = tr.ts_key)
                WHERE ts_validity @> '%s'::DATE
                ) t;",
            series, schema, tbl_vintages, valid_on)
    }
    
    class(read_SQL) <- "SQL"
    out <- runDbQuery(con,read_SQL)
    suppressMessages(commitTransaction(con))
    
    if(nrow(out) == 0) return(list(error = "No series found. Did you use the right schema?"))
    
    
    # create a json array from character vector, cause
    # jsonlite expects json array
    jsn_arr <- sprintf("[%s]",paste0(out[,2],collapse = ","))
    jsn_li <- fromJSON(jsn_arr,simplifyVector = F)
    
    server_time <- out[1, "server_time"]
    
    out_li <- lapply(jsn_li,function(x){
      freq <- x$ts_frequency
      
      if(respect_release_date && x$ts_release_date > server_time) {
        x$ts_data <- x$ts_data[1:(length(x$ts_data)-1)]
      }
      
      d_chars <- names(x$ts_data)
      ts_data <- suppressWarnings(as.numeric(unlist(x$ts_data,
                                                    recursive = F)))
      # R internals :) 
      # only convert the first element to date cause this is costly for the 
      # entire vector !! the character vector (d_chars) is sorted, too,
      # which is all we need for zoo !!!
      d <- as.Date(d_chars[1])
      y <- as.numeric(format(d,"%Y"))
      p <- as.numeric(format(d,"%m"))
      
      if(is.null(freq)){
        if(pkg_for_irreg == "zoo"){
          warning("time series does not have regular frequency, using the zoo package for mapping to R.
                  This is not an error, but the functionality is currently considered experimental.")
          z <- zoo::zoo(ts_data,
                        order.by = d_chars)
          z 
        } else if(pkg_for_irreg == "xts"){
          warning("time series does not have regular frequency, using the xts package for mapping to R.
                  This is not an error, but the functionality is currently considered experimental.")
          z <- xts::xts(ts_data,
                        order.by = as.Date(d_chars))
          z 
        } else {
          stop("No valid package for irregular time series selected. Choose either xts or zoo.")
        }
      } else {
        if(freq == 4){
          period <- (p -1) / 3 + 1
        } else if(freq == 2) {
          period <- ifelse(p == 1,1,2)
        } else if(freq == 12){
          period <- p
        } else if(freq == 1){
          period <- NULL  
        }
        # create the time series object but suppress the warning of creating NAs
        # when transforming text NAs to numeric NAs
        stats::ts(ts_data,
           start=c(y,period),
           frequency = freq)
      }
    })
    
    names(out_li) <- out[,1]
    class(out_li) <- append(class(out_li),"tslist")
    out_li
    }
  
  
  if(length(series) > chunksize){
    n <- length(series)
    chunks <- suppressWarnings(split(series,
                                     rep(1:ceiling(length(series)/chunksize),
                                         each = chunksize)))
    
    names(chunks) <- NULL
    # out_li <- unlist(lapply(chunks,readFromDB,con = con),recursive = F)
    out_li <- list()
    # lapply over readchunks caused the db the not free up memory... 
    for (i in seq_along(chunks)){
      out_li[[i]] <- readFromDB(chunks[[i]],con)
    }
    out_li <- unlist(out_li,recursive = FALSE)
  } else{
    out_li <- readFromDB(series,con)
  }
  
  if(!is.null(env)) {
    if(class(env) != "environment"){
      stop("If class is not NULL, it has to be an environment.")
    } 
    list2env(out_li,envir = env)
  } else {
    out_li
  }
}
