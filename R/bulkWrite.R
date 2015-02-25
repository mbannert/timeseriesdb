

ts_list <- list()
ts_list[[1]] <- ts(1:100,start = c(1990,1),frequency = 12)
ts_list[[2]] <- ts(rnorm(45),start = c(1995,1),frequency = 4)
ts_list[[3]] <- ts(rnorm(99),start = c(1989,1),frequency = 12)
names(ts_list) <- c('ts1','ts2','ts3')

drv <- dbDriver("PostgreSQL")
dbname <- "sandbox"
dbhost <- "localhost"
dbport <- 5432
dbuser <- 'mbannert'

con <- dbConnect(drv,
                    host = dbhost,
                    port = dbport,
                    dbname = dbname,
                    user = dbuser)

bulkWrite <- function(series,li,con,tbl="timeseries_main",unlocal='meta_data_unlocalized'){
  hstores <- unlist(lapply(li,createHstore))
  freqs <- sapply(li,frequency)
  values <- paste(paste0("('",
                         paste(series,
                               hstores,
                               freqs,
                               sep="','"),
                         "')"),
                  collapse = ",")
  
  # coverages <- lapply()
  
  md_generated_by <- Sys.info()["user"]
  md_resource_last_update <- Sys.time()
  md_coverages <- unlist(lapply(li,function(x){
    sprintf('%s to %s',
            min(zooLikeDateConvert(x)),
            max(zooLikeDateConvert(x))
            )}
    ))
  
  md_values <- paste(paste0("('",
                         paste(series,
                               md_generated_by,
                               md_resource_last_update,
                               md_coverages,
                               sep="','"),
                         "')"),
                  collapse = ",")
  
  # sql_query_md <- sprintf("INSERT INTO %s (ts_key,md_generated_by,md_resource_last_update,md_coverage_temp) VALUES ('%s','%s','%s','%s')
  
  
  
  sql_query <- sprintf("INSERT INTO %s (ts_key,ts_data,ts_frequency) VALUES %s",tbl,values) 
  
  sql_query_md <- sprintf("INSERT INTO %s (ts_key,md_generated_by,md_resource_last_update,md_coverage_temp) VALUES %s",unlocal,md_values) 
#   
dbGetQuery(con,sql_query)
#dbGetQuery(con,sql_query_md)
}

ts_env <- list2env(ts_list)


# bulk write seems twice as fast

library(microbenchmark)
microbenchmark({
  bulkWrite(names(ts_list),ts_list,con)
})

microbenchmark({lapply(names(ts_list),storeTimeSeries,con = con, tbl = "timeseries_main",md_unlocal = "meta_data_unlocalized",lookup_env = ts_env)
})  


# so far bulkWrite does not update the 
# meta information... gotta check this
# but it's two times faster... 
# there might be a 1000 series problem... 



# lapply(series,storeTimeSeries,con = con, tbl = "timeseries_main",md_unlocal = "meta_data_unlocalized",lookup_env = 'ts_env')
# 
#   storeTimeSeries(,)






sql_query <- sprintf("INSERT INTO %s (ts_key,ts_data,ts_frequency) VALUES ('%s','%s',%s)",
                     tbl,series,ts_data,ts_freq)
