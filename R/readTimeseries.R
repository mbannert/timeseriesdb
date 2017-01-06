#' Read Time Series From PostgreSQL database
#' 
#' This function reads a time series from a PostgreSQL relation
#' that uses Postgres' key value pair storage (hstore).
#' After reading the information from the database a standard
#' R time series object of class 'ts' is built and returned. Irregular time series return zoo objects.
#' 
#' @author Matthias Bannert, Gabriel Bucur
#' @param series character vector of time series keys
#' @param con a PostgreSQL connection object
#' @param valid_on character date string on which the series should be valid. Defaults to NULL. Only needed when different vintages of a time series are stored.  
#' @param env environment, optional argument to dump time series directly into an environment. Most often used with globalenv(), which gives all time series directly back to the global env.
#' @param tbl character string denoting the name of the relation that contains ts_key, ts_data, ts_frequency.
#' @param tbl_vintages character table name of the relation that holds time series vintages
#' @param schema character SQL schema name. Defaults to timeseries.
#' @importFrom DBI dbGetQuery
#' @importFrom jsonlite fromJSON
#' @export
readTimeSeries <- function(series, con,
                           valid_on = NULL,
                           tbl = "timeseries_main",
                           tbl_vintages = "timeseries_vintages",
                           schema = "timeseries",
                           env = NULL){
  series <- paste(paste0("('",series,"')"),collapse=",")
  if(is.null(valid_on)){
    read_SQL <-
      sprintf("
            BEGIN;
            CREATE TEMPORARY TABLE ts_read (ts_key text PRIMARY KEY) ON COMMIT DROP;
            INSERT INTO ts_read(ts_key) VALUES %s;

            SELECT ts_key, row_to_json(t)::text AS ts_json_records
            FROM (
            SELECT tm.ts_key, ts_data, ts_frequency
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
              
              SELECT ts_key, row_to_json(t)::text AS ts_json_records
              FROM (
              SELECT tm.ts_key, ts_data, ts_frequency
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
  
  out_li <- lapply(jsn_li,function(x){
    freq <- x$ts_frequency
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
      warning("time series does not have regular frequency, using the zoo package for mapping to R.
              This is not an error, but the functionality is currently considered experimental.")
      z <- zoo::zoo(ts_data,
                    order.by = d_chars)
      z
    } else {
      if(freq == 4){
        period <- (p -1) / 3 + 1
      } else if(freq == 12){
        period <- p
      } else if(freq == 1){
        period <- NULL  
      }
      # create the time series object but suppress the warning of creating NAs
      # when transforming text NAs to numeric NAs
      ts(ts_data,
         start=c(y,period),
         frequency = freq)
    }
  })
  
  
  names(out_li) <- out[,1]
  class(out_li) <- append(class(out_li),"tslist")
  
  if(!is.null(env)) {
    if(class(env) != "environment"){
      stop("If class is not NULL, it has to be an environment.")
    } 
    list2env(out_li,envir = env)
  } else {
    out_li
  }
}
