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
install_github('mbannert/timeseriesdb')
```
## Set Default Connection
It can be very convenient to set a default database connection when using the same instance of **timeseriesdb**
most of the time. This can be done by adding the following to ~/.Renviron (for the user) or globally to the same file in R_HOME. 

TIMESERIESDB_NAME = "sandbox"  # or whatever dbname you use
TIMESERIESDB_HOST = "localhost" # typically your remote host. 

If you do so and your dbuser is the same as your system user `createConObj` does not need any argument to connect. 



## Database
If you do not have a PostgreSQL that contains a table called 'timeseries_main' with the proper columns, you can use the createTable.sql script in inst/sql

## CASCADE ON DELETE
By default PostgreSQL tables don't cascade on delete. That means if you delete a row in a main table you could not do so unless the corresponding row in a table referenced by foreign key is delete before. This happens for good reasons. But in the case of unlocalized meta information you want to delete unlocalized meta information when your original series is deleted. That's why the latest version of the createTable.sql uses cascade on DELETE.You can drop this constraint by dropping the foreign key and adding a new one. However, adding cascade on delete after creating the table works like: 

```
alter table meta_data_unlocalized add constraint meta_data_unlocalized_fkey foreign key(fid) references timeseries_main (ts_key) on delete cascade

```



## Hstore Extension
hstore is an extension to PostgreSQL that needs to be loaded once before you can start creating tables that contain hstore data type. 
Simply run
```
CREATE EXTENSION hstore;
```
to do so. Also make sure to add hstore to your users search path. 


