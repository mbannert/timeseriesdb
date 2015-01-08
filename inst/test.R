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
pg_con <- dbConnect(drv,
                    host = dbhost,
                    port = dbport,
                    dbname = dbname,
                    user = dbuser)
# convenient default
options('TIMESERIESDB_CON' = pg_con)

storeTimeSeries('ts1')

meta_data_localized <- readMetaInformation('ts1')



dictionary <- list(en = list('short_description' = 'Random Series',
                             'full_description' = 'Random Time Series generated
                       from a Standard Normal using seed 123.',
                             'title' = 'test-title',
                             'remarks' = 'different languages are not required
                       to have the same items'),
                   de = list('short_description' = 'Zufallszeitreihe',
                             'full_description' = 'Eine Zufallszeitreihe
                      gezogen aus einer Standardnormalverteilung.
                      Startpunkt des Zufallsgenerators war 123.'
                   ))



meta_data_localized <- addMetaInformation('ts1',
                                          dictionary,overwrite = T)
meta_data_localized$ts1


￼￼
storeMetaInformation('ts1',lookup_env = 'meta_data_localized',con = con)

meta_data_localized$ts1


# unlocalized meta information
dict_ul <- list()
dict_ul$flexible <- list('coverage_spatial' = 'CH',
                'source' = 'test')


meta_data_unlocalized <- new.env()

addMetaInformation('ts1',dict_ul,meta_env = 'meta_data_unlocalized')

storeMetaInformation('ts1',tbl = 'meta_data_unlocalized',
                     lookup_env = 'meta_data_unlocalized',localized = F)

meta_data_unlocalized$ts1


readMetaInformation('ts1',localized = F,tbl = "meta_data_unlocalized")








