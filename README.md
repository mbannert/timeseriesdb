timeseriesdb
============

**R package to store and organize time series in a simple but powerful relational database using PostgreSQL**

The **timeseriesdb** package suggests a simple but yet powerful database structure to store a large amount of time series in a relational PostgreSQL database. The package provides an interface to the R user to create, update and delete time series. Also, useRs can search the database using R. I plan to extend comprehensive support for storing and querying multi-lingual meta information in the future.  

## What's new about timeseriesdb?
One of the main differences between the database structure suggested by **timeseriesdb** and the popular **TSdbi** package family is the use of key-value-pair storage. I use PostgreSQL's hstore format. Also, multi-lingual meta information is important to this project while the likes of TSPostgreSQL only offer basic support for meta descriptions.


## Install timeseriesdb
**timeseriesdb** is not CRAN ready yet, so the easiest way to get is, is to use **install_github** from the **devtools** package.
The following should do the job. 

```
library(devtools)
install_github("timeseriesdb","mbannert")
```

## Database
If you do not have a PostgreSQL that contains a table called 'timeseries_main' with the proper columns, you can use the createTable.sql script in inst/sql
or copy&paste the SQL snippet from here:

```
CREATE TABLE timeseries_main (ts_key varchar primary key, 
                              ts_data hstore, 
                              ts_frequency timestamptz,
                              md_generated_by varchar,
                              md_generated_on varchar);
```

## Hstore Extension
hstore is an extension to PostgreSQL that needs to be loaded once before you can start creating tables that contain hstore data type. 
Simply run
```
CREATE EXTENSION hstore;
```
to do so. Also make sure to add hstore to your users search path. 


