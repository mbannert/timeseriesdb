readTimeSeriesWithReleaseEpoch <- function(series, con,
                                           valid_on = NULL,
                                           tbl = "timeseries_main",
                                           tbl_vintages = "timeseries_vintages",
                                           schema = "timeseries",
                                           env = NULL,
                                           pkg_for_irreg = "xts",
                                           chunksize = 10000,
                                           respect_release_date = FALSE){
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
        ts(ts_data,
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