library(timeseriesdb)

con <- createConObj(dbname = "sandbox",dbhost = "localhost",passwd = "")

nms <- paste("ts",1:50,sep="_")

ts_list <- lapply(nms,function(x) ts(rnorm(50),start = c(1990,1),frequency = 4))
names(ts_list) <- nms
vintage_list <- lapply(nms,function(x) ts(rexp(50),start = c(1990,1),frequency = 4))
names(vintage_list) <- nms


# basic idea: vintage is rather a single store... 
# should keep you from storing too many vintages... 
# derivatives (which work with out a date )
storeTsVintage <- function(ts_key,series,con,
                           vintage_key = "regular",
                           vintage_data = format(Sys.Date(),"%b%Y"),
                           tbl = "timeseries_vintage",
                           schema = "timeseries"){
  
  # sanity checks
  #if(!timeseriesdb::dbIsValid(con)) stop("DB Connection is not valid anymore.")
  if(!inherits(ts_key,"character")) stop("Time series key needs to be a character.")
  if(!inherits(series,c("zoo","ts","xts"))) stop("Series needs to be a time series object of class ts, zoo or xts.")
  
  
  ts_hstore <- createHstore(series)
  ts_freq <- frequency(series)
  
  ts_hstore
  
  
  
  
}

storeTsVintage("ts1",ts_list$ts1,con)






# a) add a list of vintages to a ts_key

# b) add a list of single vintages to a list of ts_keys
storeTsVintage <- function(series,con,
                           li,
                           tbl = "timeseries_vintage",
                           schema = "timeseries"){
  
  li <- li[series]
  
  
  hstores <- unlist(lapply(li,createHstore))
  freqs <- sapply(li,frequency)
  values <- paste(paste0("('",
                         paste(series,
                               hstores,
                               freqs,
                               sep="','"),
                         "')"),
                  collapse = ",")
  
  # add schema name
  tbl <- paste(schema,tbl,sep = ".")
  
  sql_query_data <- sprintf("BEGIN;
                            CREATE TEMPORARY TABLE 
                            ts_updates(ts_key varchar, ts_data hstore, ts_frequency integer) ON COMMIT DROP;
                            INSERT INTO ts_updates(ts_key, ts_data, ts_frequency) VALUES %s;
                            LOCK TABLE %s.timeseries_main IN EXCLUSIVE MODE;
                            UPDATE %s.timeseries_main
                            SET ts_data = ts_updates.ts_data,
                            ts_frequency = ts_updates.ts_frequency
                            FROM ts_updates
                            WHERE ts_updates.ts_key = %s.timeseries_main.ts_key;
                            INSERT INTO %s.timeseries_main
                            SELECT ts_updates.ts_key, ts_updates.ts_data, ts_updates.ts_frequency
                            FROM ts_updates
                            LEFT OUTER JOIN %s.timeseries_main ON (%s.timeseries_main.ts_key = ts_updates.ts_key)
                            WHERE %s.timeseries_main.ts_key IS NULL;
                            COMMIT;",
                            values, schema, schema, schema, schema, schema, schema, schema)
  
  
  sql_query_data
  
}


undebug(storeTsVintage)
storeTsVintage("ts_1",con,vintage_list)

debug(storeTimeSeries)
storeTimeSeries(names(ts_list)[1],con,li = ts_list)




storeTsVintage <- function(ts_key,con,series,
               vintage_type = "regular",
               vintage_date = format(Sys.Date(),"%b%Y"),
               tbl = "timeseries_vintages",
               schema = "timeseries"){
  hstore <- createHstore(series)
  freq <- frequency(series)
  
  values <- paste(paste0("('",
                         paste(series,
                               hstore,
                               freq,
                               sep="','"),
                         "')"),
                  collapse = ",")
  
  
  # add schema name
  tbl <- paste(schema,tbl,sep = ".")
  
  values
  
  
}

undebug(storeTsVintage)
storeTsVintage("ts1",con,vintage_list$ts_1)


storeTsVintage <- function(ts_key,con,li,
                           vintage_type,
                           vintage_date = format(Sys.Date(),"%b%Y"),
                           schema = "timeseries"){
  
  
  
}

