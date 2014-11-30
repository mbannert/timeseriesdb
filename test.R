# CONNECT TO MICRO DATA ARCHIVE AND TIME SERIES DATABASE ####
# load drivers
library(RPostgreSQL)
library(timeseriesdb)

# this can be changed in the future when schemes vary
# this connection only works within KOF
drv <- dbDriver("PostgreSQL")
dbname <- "sandbox"
dbhost <- "localhost"
dbport <- 5432
dbuser <- 'mbannert'

# create R connection object
con <- dbConnect(drv, host=dbhost, port=dbport, dbname=dbname,
                 user=dbuser)


meta_data_localized <- readMetaInformation('ts1',con)