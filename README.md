# {timeseriesdb}: A Time Series Database for Official Statistics

[![CRAN_Status_Badge](https://www.r-pkg.org/badges/version/timeseriesdb)](https://cran.r-project.org/package=timeseriesdb)
[![CRAN_time_from_release](https://www.r-pkg.org/badges/ago/timeseriesdb)](https://cran.r-project.org/package=timeseriesdb)
[![metacran downloads](https://cranlogs.r-pkg.org/badges/timeseriesdb)](https://cran.r-project.org/package=timeseriesdb)
[![license](https://img.shields.io/badge/license-gplv3-lightgrey.svg)](https://choosealicense.com/)

-> [GitHub Pages Documentation Site](https://mbannert.github.io/timeseriesdb) <- 

{timeseriesdb} maps R time series objects to PostgreSQL database relations for permanent storage. Instead of writing time series to spreadsheet files or .RData files on disk, {timeseriesdb} uses a set of PostgreSQL relations which allows to store data alongside extensive, multi-lingual meta information in context aware fashion. {timeseriesdb} was designed with official statistics in mind: It can keep track of various versions of the same time series to handle data revisions, e.g., in the case of GDP data. 

## Why {timeseriesdb} ?

{timeseriesdb}  ... 

- is lite weight but powerful: multi-language meta information, versioning of time series, ...
- built entirely based on license cost free open source components.
- tailored to the needs of Official and Economic Statistics
- administration friendly, extendable access rights management
- well documented, developer friendly. 
- API ready: {timeseriesdb} can easily be extended to expose data through a REST API to allow for language agnostic access to your time series.


## What Does {timeseriesdb} NOT DO ?  

{timeseriesdb} is not built to incrementally append new observations as fast as possible. {timeseriesdb} does not try to compete with the amazing speed of InfluxDB. It's not a server log or IoT minded time series storage.

## Quick Start Guide

Make sure you followed the [installation notes](articles/installation_guide.html) to make sure all components of the
{timeseriesdb} were installed properly: PostgreSQL, necessary PostgreSQL extension, 
R as well as the {timeseriesdb} R package. 

## Example Use (Basic Usage)

The following examples illustrate basic use in a nutshell. 
The learn more about the use of {timeseriesdb},
read the vignette articles.

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
- release calendar functionality to facilitate publishing









