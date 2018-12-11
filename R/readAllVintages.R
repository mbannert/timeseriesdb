readAllVintages <- function(
  con,
  series,
  tbl_vintages = "timeseries_vintages",
  schema = "timeseries",
  pkg_for_irreg = "xts",
  respect_release_date = FALSE
) {
  query <- sprintf("
           SELECT ts_key, row_to_json(t)::text AS ts_json_records, extract(EPOCH FROM NOW()) as server_time
           FROM (
             SELECT ts_key, ts_data, ts_frequency,
                    lower(ts_validity)::TEXT as lower_bound, coalesce(upper(ts_validity)::TEXT, 'open') as upper_bound,
                    extract(epoch from ts_release_date) as ts_release_date
             FROM %s.%s
             WHERE ts_key = '%s'
             ORDER BY ts_validity
           ) t;",
        schema, tbl_vintages, series)
  class(query) <- "SQL"
  
  out <- runDbQuery(con, query)
  
  if(nrow(out) == 0) {
    return(list(error = "No series found. Did you use the right schema?"))
  }
  
  # create a json array from character vector, cause
  # jsonlite expects json array
  jsn_arr <- sprintf("[%s]", paste0(out[, 2], collapse = ","))
  jsn_li <- fromJSON(jsn_arr, simplifyVector = F)
  
  server_time <- out[1, "server_time"]
  
  out_li <- lapply(jsn_li, function(x){
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
    y <- as.numeric(format(d, "%Y"))
    p <- as.numeric(format(d, "%m"))
    
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
        period <- (p - 1) / 3 + 1
      } else if(freq == 2) {
        period <- ifelse(p == 1, 1, 2)
      } else if(freq == 12){
        period <- p
      } else if(freq == 1){
        period <- NULL
      }
      # create the time series object but suppress the warning of creating NAs
      # when transforming text NAs to numeric NAs
      stats::ts(ts_data,
                start=c(y, period),
                frequency = freq)
    }
  })
  
  lbs <- sapply(jsn_li, `[[`, "lower_bound")
  ubs <- sapply(jsn_li, `[[`, "upper_bound")
  
  nams <- sprintf("%s.%s_%s",
                  out[1, 1],
                  gsub("([0-9]{4})(-)([0-9]{2})(.*)", "\\1\\3", lbs),
                  gsub("([0-9]{4})(-)([0-9]{2})(.*)", "\\1\\3", ubs))
  
  names(out_li) <- nams
  class(out_li) <- append(class(out_li),"tslist")
  out_li
}