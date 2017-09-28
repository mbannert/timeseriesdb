library(tsbox)
library(timeseriesdb)
load("~/Desktop/tslist.RData")

ts1 <- ts(1:120,start = c(1985,1),frequency = 4)
ts2 <- ts(120:1,start = c(1985,1),frequency = 4)
ts3 <- ts(120:200,start = c(1985,1),frequency = 4)


tsl <- list()
tsl$ts1 <- ts1
tsl$ts2 <- ts2


tdf <- ts_data.frame(ts1)
names(tdf)[3] <- "ts_key" 
tdf <- tdf[,c(3,1,2)]

tdf2 <- ts_data.frame(ts2)
names(tdf2)[3] <- "ts_key" 
tdf2 <- tdf2[,c(3,1,2)]

tdf3 <- ts_data.frame(ts3)
names(tdf3)[3] <- "ts_key" 
tdf3 <- tdf3[,c(3,1,2)]



tdf <- rbind(tdf,tdf2,tdf3)

ldf <- list()
for(i in seq_along(tslist[1:1000])){
  ldf[[i]] <- ts_data.frame(tslist[[i]])
}

ldf <- lapply(seq_along(ldf),function(x){
  ldf[[x]]$var <- paste0("var",x)
  ldf[[x]]
})


tsdf <- as.data.frame(data.table::rbindlist(ldf))
tsdf <- tsdf[,c(3,1,2)]
names(tsdf) <- c("ts_key","time","value")


data_insert <- sprintf("BEGIN;
                        CREATE TEMPORARY TABLE 
                                  ts_updates (
                                  ts_key text,
                                  time text,
                                  value text,
                                  primary key (ts_key, time)) ON COMMIT DROP;")

data_copy <- "COPY ts_updates FROM STDIN;"

# localized meta information does not HAVE to exist, which 
# means we have to have an insert here!  
data_update <- sprintf("LOCK TABLE timeseries.quarterly IN EXCLUSIVE MODE;
                                  UPDATE timeseries.quarterly q
                                  SET time = ts_updates.time,
                                  value = ts_updates.value
                                  FROM ts_updates
                                  WHERE ts_updates.ts_key = q.ts_key
                                  AND ts_updates.time = q.time;
                                  
                                  ---
                                  INSERT INTO timeseries.quarterly
                                  SELECT ts_updates.ts_key,
                                  ts_updates.time,
                                  ts_updates.value
                                  FROM ts_updates
                                  LEFT OUTER JOIN timeseries.quarterly q
                                  ON q.ts_key = ts_updates.ts_key
                                  AND q.time = ts_updates.time
                                  WHERE q.ts_key IS NULL;
                                  COMMIT;")


library(timeseriesdb)
con <- createConObj(dbhost = "localhost",
                    dbuser = "mbannert",
                    dbname = "tutorial",
                    passwd = "")


data_copy <- "COPY ts_data FROM STDIN;"
ok1 <- DBI::dbGetQuery(con,data_copy)
postgresqlCopyInDataframe(con, tsdf)

library(microbenchmark)


  tsdf <- tsdf[,c(2,1,3)]
  tsdf$time <- as.character(tsdf$time)
  data_copy <- "COPY ts_data FROM STDIN;"
  ok1 <- DBI::dbGetQuery(con,data_copy)
  postgresqlCopyInDataframe(con, tsdf[1:20000,])
  
  
  ok1 <- DBI::dbGetQuery(con,data_copy)
  postgresqlCopyInDataframe(con, tsdf[10001:20000,])
  ok1 <- DBI::dbGetQuery(con,data_copy)
  postgresqlCopyInDataframe(con, tsdf[20001:30000,])
  ok1 <- DBI::dbGetQuery(con,data_copy)
  postgresqlCopyInDataframe(con, tsdf[30001:40000,])
  ok1 <- DBI::dbGetQuery(con,data_copy)
  postgresqlCopyInDataframe(con, tsdf[40001:50000,])




  


microbenchmark({
  ok <- DBI::dbGetQuery(con,data_insert)
  ok1 <- DBI::dbGetQuery(con,data_copy)
  postgresqlCopyInDataframe(con, tsdf)
  ok2 <- DBI::dbGetQuery(con,data_update)
},times = 10)

sl <- tslist[1:1000]

library(microbenchmark)
microbenchmark({
  storeTimeSeries(names(sl),con,sl)  
},times = 10)





xts2SQL <- function(x,key){
  out <- paste0("'",time(x),"'",",'",key,"',",x)
  paste(sprintf("(%s)",out),collapse=",")
}


sl <- tslist[1:1000]
sl <- lapply(sl,ts_xts)
l <- sapply(names(sl),function(x){
  xts2SQL(sl[[x]],x)
})

v <- paste(l[!grepl("NA",l)],collapse=",")
q <- sprintf("INSERT INTO ts_data VALUES %s",v)

library(microbenchmark)
# microbenchmark({
#   for (i in seq_along(l)){
#     dbGetQuery(con,sprintf("INSERT INTO ts_data VALUES %s",l[i]))
#   }
# },times=1)



library(microbenchmark)
microbenchmark(
  {dbGetQuery(con,q)},times = 1
  )

# RPostgres 
# needs 60 secs as well, has a smoother interface but it's not faster... 
# con <- dbConnect(RPostgres::Postgres(),dbname = 'tutorial', 
#                  host = 'localhost', # i.e. 'ec2-54-83-201-96.compute-1.amazonaws.com'
#                  port = 5432, # or any other port specified by your DBA
#                  user = 'mbannert',
#                  password = '')
# 
# library(microbenchmark)
# microbenchmark({
#   xx <- RPostgres::dbSendQuery(con,q)
#   dbFetch()
#   dbClearResult(xx)
# },times=1)
# 
# 
# 
# 
# 
# 
# 
# 
# 
