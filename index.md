#  A Time Series Database for Official Statistics

[![CRAN_Status_Badge](https://www.r-pkg.org/badges/version/timeseriesdb)](https://cran.r-project.org/package=timeseriesdb)
[![CRAN_time_from_release](https://www.r-pkg.org/badges/ago/timeseriesdb)](https://cran.r-project.org/package=timeseriesdb)
[![metacran downloads](https://cranlogs.r-pkg.org/badges/timeseriesdb)](https://cran.r-project.org/package=timeseriesdb)
[![license](https://img.shields.io/badge/license-gplv3-lightgrey.svg)](https://choosealicense.com/)

{timeseriesdb} maps R time series objects to PostgreSQL database relations for permanent storage. Instead of writing time series to spreadsheet files or .RData files on disk, {timeseriesdb} uses a set of PostgreSQL relations which allows to store data alongside extensive, multi-lingual meta information in context aware fashion. {timeseriesdb} was designed with official statistics in mind: It can keep track of various versions of the same time series to handle data revisions, e.g., in the case of GDP data. 


## Why {timeseriesdb} ?

{timeseriesdb}  ... 

- is lite weight but powerful: version
- built entirely based on license cost free open source components.
- tailored to the needs of Official and Economic Statistics
- administration friendly, extendable access rights management
- well documented, developer friendly. 
- API ready: {timeseriesdb} can easily be extended to expose data through a REST API to allow for language agnostic access to your time series.


## What Does {timeseriesdb} NOT DO ?  

{timeseriesdb} is not built to incrementally append new observations as fast as possible. {timeseriesdb} does not try to compete with the amazing speed of InfluxDB. It's not a server log or IoT minded time series storage.

## Quick Start Guide

Skip the __blabla__. You know what SQL is? You are a seasoned useR and work with time series on a regular basis? Here's how to get going as quickly as possible. 

### Using Docker

If you're familiar with the docker basics and have a remote docker host and/or local docker installation, the docker based approach is the easiest way to check out {timeseriesdb} in action. {timeseriesdb} ships a shell script to set up a test instance of {timeseriesdb} in a docker container. Basically, this script
does the following:

1. Stop container running as _timeseriesdb_dev_ if it exists.

2. Pull a __PostgreSQL_ docker image from docker hub if you don't have that already.

3. Create all tables and functions, grant access rights according to a few basic blue 
print roles (admin, reader, writer) and access levels (public, main, restricted).

Note: depending on your OS and configuration, e.g., on Ubuntu systems you'll need use _sudo_ to run docker. The easiest way to start the script is to change into your install directory and run 

```
./start_docker_dev.sh
```

If you don't know where R package installation directory is, simply find out by
running 

```
system.file(package = "timeseriesdb")
```

or download the {timeseriesdb} package source from [CRAN](https://cran.r-project.org/package=timeseriesdb) or [GitHub](). 


### Locally 

This quick start guide assumes you've PostgreSQL installed or have access to a remote PostgreSQL database including the necessary access rights to create new schemas and tables. Also, let's assume you installed R. If that's not case please refer to: 

- [PostgreSQL Download](https://www.postgresql.org/download/)
- [R Language for Statistical Computing](https://www.r-project.org/)

1. Before Install the R Package Install the R package (stable version)


2. Install the {timeseriesdb} R Package 

{timeseriesdb} is on CRAN. To install the stable version (recommended to most users),

run

```
install.packages("timeseriesdb")
```

or use the {remotes} or {devtools} packages to directly 
download the developer version {timeseriesdb} from GitHub: 

```
remotes::install_github("mbannert/timeseriesdb")
```




## Example Use (Basic Usage)

The following examples illustrate basic use in a nutshell. 
The learn more about the use of {timeseriesdb}, visit its {pkgdown} documentation page and read the vignette articles.

### Store a List of R Time Series Objects to the Database

```
# Create DB connection. 
# In this case connect to a local db running on port 1111
# /w lame passwords -- strongly discouraged for production. 
con <- dbConnect(Postgres(),
                "dev_writer", "localhost",
                 1111, "dev_writer",
                "postgres")
tsl <- list(ts1 = ts(rnorm(100), frequency = 12,
                     start = 2002),
            ts2 = ts(rnorm(100), frequency = 12,
                     start = 2001))
db_store_ts(connection, tsl)
dbDisconnect(con)
```

### Read Data into a list of R time Series object

```
con <- dbConnect(Postgres(),
                "dev_writer", "localhost",
                 1111, "dev_writer",
                "postgres")

tsl <- db_read_ts(connection, c("some_ts_id","another_ts_id"))
dbDisconnect(con)
```

### Advanced Features

{timeseriesdb} offers a plethora of features beyond just mere storage of time 
series themselves:  

- store vintages (versions) of time series
- datasets to group time series
- store extensive, multi-lingual, versioned meta information at
  dataset and time series level
- individual, user specific collections of time series similar to bookmark or
playlist functionality
- administration friendly access management with reasonable defaults
  (public, internal, restricted)
- release calendar functionality to facilitate on time publishing









