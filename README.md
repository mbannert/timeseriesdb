timeseriesdb
============

**R package to store and organize time series in a simple but powerful relational database using PostgreSQL**

The **timeseriesdb** package suggests a simple but yet powerful database structure to store a large amount of time series in a relational PostgreSQL database. The package provides an interface to the R user to create, update and delete time series. Also, useRs can search the database using R. I plan to extend comprehensive support for storing and querying multi-lingual meta information in the future.  

## What's new about timeseriesdb?
One of the main differences between the database structure suggested by **timeseriesdb** and the popular **TSdbi** package family is the use of key-value-pair storage. I use PostgreSQL's hstore format. Also, multi-lingual meta information is important to this project while the likes of TSPostgreSQL only offer basic support for meta descriptions.


